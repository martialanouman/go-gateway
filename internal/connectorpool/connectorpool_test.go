package connectorpool_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// fakeConsumer replays each record as its own single-record batch and stops at the first failure —
// the sequential, stop-on-error semantics the M2 tests were written against.
type fakeConsumer struct{ records []kafka.Record }

func (f *fakeConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	for _, r := range f.records {
		for _, err := range handle(ctx, []kafka.Record{r}) {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// batchConsumer feeds all records as ONE batch, so the pool shards them across its binds concurrently.
type batchConsumer struct{ records []kafka.Record }

func (b *batchConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	handle(ctx, b.records)
	return nil
}

type fakeCDR struct {
	mu   sync.Mutex
	rows []clickhouse.CDRRow
}

func (f *fakeCDR) Insert(_ context.Context, row clickhouse.CDRRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, row)
	return nil
}

// fakeFlags stands in for the shared cancel-flag store.
type fakeFlags struct {
	cancelled bool
	err       error
}

func (f *fakeFlags) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.cancelled, f.err
}

// dlrPut is one recorded DLRMap.Put call.
type dlrPut struct {
	smscMsgID string
	routed    pipeline.RoutedMT
}

// fakeDLRMap records the mappings the connector writes, so a test can assert a successful submit is
// remembered. A non-nil err drives the best-effort failure path.
type fakeDLRMap struct {
	mu   sync.Mutex
	puts []dlrPut
	err  error
}

func (f *fakeDLRMap) Put(_ context.Context, smscMsgID string, r pipeline.RoutedMT) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, dlrPut{smscMsgID, r})
	return f.err
}

// runWithDLRMap drives one record through the connector with a DLR map wired, returning the CDR sink
// and Run's error.
func runWithDLRMap(t *testing.T, dlr connectorpool.DLRMap, resp func(smpp.SubmitSM) fakesmsc.Resp, r pipeline.RoutedMT) (*fakeCDR, error) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      cdr,
		DLRMap:   dlr,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	return cdr, svc.Run(context.Background())
}

// TestConnectorRecordsDLRMappingOnEnroute: a successful submit records smsc_msg_id -> message_id, with
// the connector id and trace id, so a later receipt can be correlated (step-044). The fake SMSC
// assigns "0000000000000001" as its first message id.
func TestConnectorRecordsDLRMappingOnEnroute(t *testing.T) {
	r := routed()
	dlr := &fakeDLRMap{}
	_, err := runWithDLRMap(t, dlr, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(dlr.puts) != 1 {
		t.Fatalf("expected 1 DLR mapping, got %d", len(dlr.puts))
	}
	got := dlr.puts[0]
	// The full routed envelope (the CDR projection step-044 needs) travels with the mapping.
	if got.routed.MessageID != r.MessageID || got.routed.ConnectorID != r.ConnectorID ||
		got.routed.AccountID != r.AccountID || got.routed.CustomerID != r.CustomerID {
		t.Errorf("mapping routed = %+v, want message %s / connector %s / account %s / customer %s",
			got.routed, r.MessageID, r.ConnectorID, r.AccountID, r.CustomerID)
	}
	if got.smscMsgID != "0000000000000001" {
		t.Errorf("smsc_msg_id = %q, want 0000000000000001", got.smscMsgID)
	}
}

// TestConnectorSkipsDLRMappingOnFailedSubmit: a permanent SMSC rejection has no smsc_msg_id and is not
// enroute, so no mapping is recorded.
func TestConnectorSkipsDLRMappingOnFailedSubmit(t *testing.T) {
	dlr := &fakeDLRMap{}
	cdr, err := runWithDLRMap(t, dlr, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, routed())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusFailed {
		t.Fatalf("expected one failed CDR row, got %+v", cdr.rows)
	}
	if len(dlr.puts) != 0 {
		t.Errorf("a failed submit must record no DLR mapping, got %+v", dlr.puts)
	}
}

// TestConnectorDLRMappingWriteIsBestEffort: a Redis failure writing the mapping must NOT fail the
// record — the message is already enroute, so Run commits and the enroute row still stands.
func TestConnectorDLRMappingWriteIsBestEffort(t *testing.T) {
	dlr := &fakeDLRMap{err: errors.New("redis down")}
	cdr, err := runWithDLRMap(t, dlr, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, routed())
	if err != nil {
		t.Fatalf("Run must commit despite a DLR-map write failure: %v", err)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("expected one enroute row, got %+v", cdr.rows)
	}
}

func routed() pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: "+2250700000000", Body: msg.NewBodyString("hello"),
		Encoding: "gsm7", ConnectorID: uuid.New(), SegmentCount: 1, SubmittedAt: time.Now().UTC(),
	}
}

// runService drives the connector through one mt.routed record and returns the CDR sink and Run's
// error, so a test can assert either a committed outcome (nil) or a redelivery (non-nil).
func runService(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, r pipeline.RoutedMT) (*fakeCDR, error) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)

	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      cdr,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	return cdr, svc.Run(context.Background())
}

// runOnce drives one record and fails the test if Run returns an error (i.e. the message was
// redelivered rather than committed).
func runOnce(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, r pipeline.RoutedMT) *fakeCDR {
	t.Helper()
	cdr, err := runService(t, resp, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return cdr
}

// runWithFlags drives one record through the connector with a cancel-flag store wired, returning the
// CDR sink, whether the SMSC saw a submit, and Run's error.
func runWithFlags(t *testing.T, flags connectorpool.CancelFlags, r pipeline.RoutedMT) (*fakeCDR, *bool, error) {
	t.Helper()
	submitted := false
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		submitted = true
		return fakesmsc.OK()
	}})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    &fakeConsumer{records: []kafka.Record{rec}},
		CDR:         cdr,
		CancelFlags: flags,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	return cdr, &submitted, svc.Run(context.Background())
}

// TestConnectorSkipsCancelledMessage pins that a message flagged cancelled before dispatch is NOT
// submitted to the SMSC, that the connector writes the cancelled CDR row itself (so a Canceller crash
// after flagging cannot leave the message unrecorded), and that the offset is committed (Run nil).
func TestConnectorSkipsCancelledMessage(t *testing.T) {
	cdr, submitted, err := runWithFlags(t, &fakeFlags{cancelled: true}, routed())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if *submitted {
		t.Error("a cancelled message must not be submitted to the SMSC")
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusCancelled {
		t.Errorf("connector must write a cancelled CDR row when honouring the flag, got %+v", cdr.rows)
	}
}

// TestConnectorSubmitsWhenNotCancelled pins that an un-flagged message is submitted normally and its
// enroute row written.
func TestConnectorSubmitsWhenNotCancelled(t *testing.T) {
	cdr, submitted, err := runWithFlags(t, &fakeFlags{cancelled: false}, routed())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !*submitted {
		t.Error("a non-cancelled message must be submitted")
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("expected one enroute row, got %+v", cdr.rows)
	}
}

// TestConnectorDispatchesWhenCancelFlagUnavailable pins the fail-open behaviour: a Redis failure
// reading the cancel flag must NOT halt delivery — cancellation is best-effort, so the connector logs
// and dispatches the message normally rather than stalling all outbound traffic on a Redis outage.
func TestConnectorDispatchesWhenCancelFlagUnavailable(t *testing.T) {
	cdr, submitted, err := runWithFlags(t, &fakeFlags{err: errors.New("redis down")}, routed())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !*submitted {
		t.Error("fail-open: a message must still be submitted when the cancel flag cannot be read")
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("expected one enroute row, got %+v", cdr.rows)
	}
}

func TestConnectorWritesEnrouteOnOK(t *testing.T) {
	r := routed()
	cdr := runOnce(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, r)

	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusEnroute {
		t.Errorf("status: got %q want enroute", row.Status)
	}
	if row.MessageID != r.MessageID {
		t.Error("message id not carried")
	}
	if row.ConnectorID == nil || *row.ConnectorID != r.ConnectorID {
		t.Errorf("connector id not carried: %v", row.ConnectorID)
	}
	if !row.SubmittedAt.Equal(r.SubmittedAt) {
		t.Error("submitted_at must be the immutable ingestion time")
	}
	if row.ErrorCode != nil {
		t.Errorf("enroute row must have no error code, got %v", *row.ErrorCode)
	}
}

// TestConnectorRedeliversOnTransientRejection pins that a retryable SMSC status (throttled) is
// backpressure, not a terminal failure: the handler returns an error so the record is redelivered,
// and NO failed CDR row is written (which would lose the message).
func TestConnectorRedeliversOnTransientRejection(t *testing.T) {
	cdr, err := runService(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() }, routed())

	if err == nil {
		t.Fatal("throttled submit must return an error (no commit → redelivery), got nil")
	}
	if len(cdr.rows) != 0 {
		t.Errorf("throttled submit must not write a CDR row, got %d", len(cdr.rows))
	}
}

// TestConnectorWritesFailedOnPermanentRejection pins that a non-retryable SMSC status (submit_fail)
// is terminal: a failed CDR is written with the contract error_code and the record is committed.
func TestConnectorWritesFailedOnPermanentRejection(t *testing.T) {
	cdr := runOnce(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, routed())

	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusFailed {
		t.Errorf("status: got %q want failed", row.Status)
	}
	if row.ErrorCode == nil || *row.ErrorCode != "submit_failed" {
		t.Errorf("error_code: got %v want submit_failed", row.ErrorCode)
	}
}

func TestConnectorSubmitsBodyOnTheWire(t *testing.T) {
	const text = "the actual message body"
	var seen string
	r := routed()
	r.Body = msg.NewBodyString(text)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = string(sm.ShortMessage)
		return fakesmsc.OK()
	}, r)

	if seen != text {
		t.Errorf("SMSC received short_message %q, want %q", seen, text)
	}
}

// TestConnectorShipsBodyVerbatim pins that the connector no longer encodes: Body already carries the
// segment's wire bytes (the pipeline's Split produced them in the resolved encoding), so a UCS-2
// payload — supplied here as the UTF-16BE bytes it is on the wire — reaches the SMSC unchanged, under
// the UCS-2 data_coding derived from the encoding. Transcoding now lives with segmentation, not here.
func TestConnectorShipsBodyVerbatim(t *testing.T) {
	wire := utf16BE("café ☕") // the UCS-2 wire bytes the pipeline would produce
	var seen []byte
	var dcs uint8
	r := routed()
	r.Encoding = "ucs2"
	r.Body = msg.NewBody(wire)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = append([]byte(nil), sm.ShortMessage...)
		dcs = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	if !bytes.Equal(seen, wire) {
		t.Errorf("body on the wire = % x, want it shipped verbatim % x", seen, wire)
	}
	if dcs != smpp.DataCodingUCS2 {
		t.Errorf("data_coding = %#x, want UCS-2 %#x", dcs, smpp.DataCodingUCS2)
	}
}

// TestConnectorHonorsDataCodingOverride pins that a client-supplied data_coding reaches the SMSC
// verbatim rather than being derived from the encoding.
func TestConnectorHonorsDataCodingOverride(t *testing.T) {
	override := 245 // a message-class / flash DCS the encoding would never produce
	var seen uint8
	r := routed()
	r.Encoding = "gsm7"
	r.DataCoding = &override
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	if seen != uint8(override) {
		t.Errorf("data_coding = %d, want the client override %d", seen, override)
	}
}

// TestConnectorTypesNumericSourceAsInternational pins that a "+"-prefixed numeric MSISDN source is
// sent plus-stripped with international/ISDN typing, not as an alphanumeric sender id.
func TestConnectorTypesNumericSourceAsInternational(t *testing.T) {
	var addr string
	var ton, npi uint8
	r := routed()
	r.From = "+12065550100"
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		addr, ton, npi = sm.SourceAddr, sm.SourceAddrTON, sm.SourceAddrNPI
		return fakesmsc.OK()
	}, r)

	if addr != "12065550100" {
		t.Errorf("source addr = %q, want the plus-stripped MSISDN 12065550100", addr)
	}
	if ton != smpp.TONInternational || npi != smpp.NPIISDN {
		t.Errorf("source TON/NPI = %#x/%#x, want international/ISDN %#x/%#x", ton, npi, smpp.TONInternational, smpp.NPIISDN)
	}
}

// TestConnectorTypesAlphanumericSource keeps a non-numeric sender id typed as alphanumeric.
func TestConnectorTypesAlphanumericSource(t *testing.T) {
	var ton uint8
	r := routed()
	r.From = "ACME"
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		ton = sm.SourceAddrTON
		return fakesmsc.OK()
	}, r)

	if ton != smpp.TONAlphanumeric {
		t.Errorf("alphanumeric sender TON = %#x, want %#x", ton, smpp.TONAlphanumeric)
	}
}

// TestConnectorSetsUDHIndicatorForSegment pins that a segment carrying a concatenation UDH ships with
// esm_class's UDH indicator set and the payload (UDH + content) verbatim in short_message, so the SMSC
// and the handset parse and reassemble it. A single segment with no UDH leaves esm_class clear.
func TestConnectorSetsUDHIndicatorForSegment(t *testing.T) {
	payload := append([]byte{0x05, 0x00, 0x03, 0x2a, 0x02, 0x01}, []byte("part one")...) // 6-octet concat UDH + content
	var esm uint8
	var seen []byte
	r := routed()
	r.HasUDH = true
	r.SegmentSeq, r.SegmentCount = 1, 2
	r.Body = msg.NewBody(payload)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		esm = sm.ESMClass
		seen = append([]byte(nil), sm.ShortMessage...)
		return fakesmsc.OK()
	}, r)

	if esm&smpp.ESMClassUDHIndicator == 0 {
		t.Errorf("esm_class = %#x, want the UDH indicator %#x set", esm, smpp.ESMClassUDHIndicator)
	}
	if !bytes.Equal(seen, payload) {
		t.Errorf("short_message = % x, want the UDH payload verbatim % x", seen, payload)
	}
}

// TestConnectorNoUDHIndicatorForSingleSegment pins the complement: a lone segment carries no UDH bit.
func TestConnectorNoUDHIndicatorForSingleSegment(t *testing.T) {
	var esm uint8
	r := routed() // HasUDH false
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		esm = sm.ESMClass
		return fakesmsc.OK()
	}, r)

	if esm&smpp.ESMClassUDHIndicator != 0 {
		t.Errorf("esm_class = %#x, want the UDH indicator clear for a single segment", esm)
	}
}

// TestConnectorOverlongSegmentFallsBackToMessagePayload pins the defensive guard: a segment whose
// encoded bytes exceed short_message's 254-octet limit (reachable for long accented GSM-7 until
// bit-packing lands) is carried in the message_payload TLV instead, so an over-length PDU never
// poisons the bind. The submit still completes — the SMSC accepts it — and short_message stays empty.
func TestConnectorOverlongSegmentFallsBackToMessagePayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0x41}, 300) // > 254 octets
	var short []byte
	var tlv []byte
	var haveTLV bool
	r := routed()
	r.HasUDH = true
	r.Body = msg.NewBody(payload)
	_, err := runService(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		short = append([]byte(nil), sm.ShortMessage...)
		tlv, haveTLV = sm.TLVs.Get(smpp.TagMessagePayload)
		return fakesmsc.OK()
	}, r)
	if err != nil {
		t.Fatalf("an over-length segment must still submit, not crash the bind: %v", err)
	}
	if len(short) != 0 {
		t.Errorf("short_message = % x, want empty (payload moved to the TLV)", short)
	}
	if !haveTLV || !bytes.Equal(tlv, payload) {
		t.Errorf("message_payload TLV present=%v, want the payload verbatim", haveTLV)
	}
}

// TestConnectorSetsValidityPeriod pins that the client's validity_period reaches the submit_sm.
func TestConnectorSetsValidityPeriod(t *testing.T) {
	vp := "000000010000000R" // SMPP relative validity (16 chars), passed through per the contract
	var seen string
	r := routed()
	r.ValidityPeriod = &vp
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.ValidityPeriod
		return fakesmsc.OK()
	}, r)

	if seen != vp {
		t.Errorf("validity_period on the wire = %q, want %q", seen, vp)
	}
}

// TestConnectorDropsOverlongValidityPeriod pins the poison-loop guard: a validity_period longer than
// the 16-char SMPP C-Octet String is dropped rather than marshalled into an unterminated PDU that the
// SMSC would reject by dropping the bind (blocking the partition on redelivery). The submit still
// succeeds, just without a validity.
func TestConnectorDropsOverlongValidityPeriod(t *testing.T) {
	overlong := "00000001000000000000R" // 21 chars, > 16
	var seen string
	r := routed()
	r.ValidityPeriod = &overlong
	cdr := runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.ValidityPeriod
		return fakesmsc.OK()
	}, r)

	if seen != "" {
		t.Errorf("over-length validity_period should be dropped, wire carried %q", seen)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("submit should still succeed (enroute), got %+v", cdr.rows)
	}
}

// TestConnectorToleratesZeroEnquireConfig pins the bind config clamps: a zero EnquireLinkInterval
// must not panic time.NewTicker(0) (which would crash the process), and MaxMissed 0 must not tear
// the bind down. If the clamp were missing, the enquire goroutine would panic and crash this test.
func TestConnectorToleratesZeroEnquireConfig(t *testing.T) {
	r := routed()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      cdr,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: 0, EnquireLinkMaxMissed: 0, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run with zero enquire config: %v", err)
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 CDR row, got %d", len(cdr.rows))
	}
}

// utf16BE is the test's independent UTF-16BE reference (mirrors the connector's transcoding).
func utf16BE(s string) []byte {
	units := utf16.Encode([]rune(s))
	out := make([]byte, 2*len(units))
	for i, u := range units {
		binary.BigEndian.PutUint16(out[2*i:], u)
	}
	return out
}

// --- return path (deliver_sm classification + publication) ---

// recordingProducer captures the records the connector publishes on the return path and signals each
// one on got, so a test can wait for the async publish.
type recordingProducer struct {
	mu   sync.Mutex
	recs []kafka.Record
	got  chan struct{}
}

func newRecordingProducer() *recordingProducer {
	return &recordingProducer{got: make(chan struct{}, 16)}
}

func (p *recordingProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.mu.Lock()
	p.recs = append(p.recs, rec)
	p.mu.Unlock()
	p.got <- struct{}{}
	return nil
}

func (p *recordingProducer) records() []kafka.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]kafka.Record(nil), p.recs...)
}

// blockingConsumer keeps Run alive (no mt.routed to process) until the context is cancelled, so the
// bind stays up while the SMSC pushes a deliver_sm.
type blockingConsumer struct{}

func (blockingConsumer) RunBatch(ctx context.Context, _ kafka.BatchHandler) error {
	<-ctx.Done()
	return ctx.Err()
}

// syncBuffer is a mutex-guarded buffer so the test can read the log while the service writes it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// runReturnPath binds the connector to a fake SMSC, waits for the bind, invokes send (which pushes a
// deliver_sm), and returns the captured records once one is published plus the full service log.
func runReturnPath(t *testing.T, connectorID uuid.UUID, send func(*fakesmsc.Server)) ([]kafka.Record, string) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{})
	prod := newRecordingProducer()
	logBuf := &syncBuffer{}
	rrec := otelrec.New(t)

	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    blockingConsumer{},
		CDR:         &fakeCDR{},
		Producer:    prod,
		ConnectorID: connectorID,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
		Logger: slog.New(slog.NewTextHandler(logBuf, nil)),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	if !waitFor(2*time.Second, func() bool { return svc.BindReady(context.Background()) == nil }) {
		t.Fatal("bind not ready in time")
	}
	send(smsc)

	select {
	case <-prod.got:
	case <-time.After(3 * time.Second):
		t.Fatal("no record produced from deliver_sm")
	}
	return prod.records(), logBuf.String()
}

// TestConnectorPublishesMOToMOInbound: a mobile-originated deliver_sm is classified as an MO and
// published on mo.inbound with the source, the inbound number and the (masked) body — and the body
// never appears in the log (invariant a).
func TestConnectorPublishesMOToMOInbound(t *testing.T) {
	const body = "bonjour ceci est un MO"
	connID := uuid.New()
	recs, logs := runReturnPath(t, connID, func(s *fakesmsc.Server) {
		if err := s.SendMO("22507000001", "36000", body); err != nil {
			t.Errorf("SendMO: %v", err)
		}
	})

	if len(recs) != 1 || recs[0].Topic != kafka.TopicMOInbound {
		t.Fatalf("records = %+v, want one on mo.inbound", recs)
	}
	mo, err := pipeline.DecodeMO(recs[0])
	if err != nil {
		t.Fatalf("DecodeMO: %v", err)
	}
	if mo.ConnectorID != connID || mo.From != "22507000001" || mo.To != "36000" {
		t.Errorf("mo = %+v, want connector %s / from 22507000001 / to 36000", mo, connID)
	}
	if string(mo.Body.Reveal()) != body {
		t.Errorf("body = %q, want %q", mo.Body.Reveal(), body)
	}
	if strings.Contains(logs, body) || strings.Contains(logs, "bonjour") {
		t.Errorf("the MO body leaked into the log (invariant a):\n%s", logs)
	}
}

// TestConnectorPublishesDLRToDLREvents: a delivery-receipt deliver_sm is classified as a DLR and
// published on dlr.events with the SMSC message id and state extracted.
func TestConnectorPublishesDLRToDLREvents(t *testing.T) {
	const smscID = "00000000000000ab"
	connID := uuid.New()
	recs, _ := runReturnPath(t, connID, func(s *fakesmsc.Server) {
		if err := s.SendDLR(smscID, fakesmsc.Delivered); err != nil {
			t.Errorf("SendDLR: %v", err)
		}
	})

	if len(recs) != 1 || recs[0].Topic != kafka.TopicDLREvents {
		t.Fatalf("records = %+v, want one on dlr.events", recs)
	}
	dlr, err := pipeline.DecodeDLR(recs[0])
	if err != nil {
		t.Fatalf("DecodeDLR: %v", err)
	}
	if dlr.ConnectorID != connID || dlr.SMSCMessageID != smscID {
		t.Errorf("dlr = %+v, want connector %s / smsc id %s", dlr, connID, smscID)
	}
	if dlr.State != 2 || dlr.Stat != "DELIVRD" {
		t.Errorf("dlr state/stat = %d/%q, want 2/DELIVRD", dlr.State, dlr.Stat)
	}
}

// continuingConsumer replays its records and, unlike fakeConsumer, does NOT stop on a handler error —
// it models at-least-once redelivery eventually making progress, so a throttled-then-accepted sequence
// can be driven through the connector in one Run.
type continuingConsumer struct{ records []kafka.Record }

func (c *continuingConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	for _, r := range c.records {
		_ = handle(ctx, []kafka.Record{r}) // one record per batch, ignore errors (redelivery sim)
	}
	return nil
}

// recordingThrottle captures the AIMD send-rate after each submit and counts throttle events.
type recordingThrottle struct {
	mu        sync.Mutex
	rates     []float64
	throttles int
}

func (m *recordingThrottle) SetRate(rate float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rates = append(m.rates, rate)
}

func (m *recordingThrottle) IncThrottled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.throttles++
}

// TestConnectorAIMDDropsThenRecovers is the step-086 acceptance: a burst of ESME_RTHROTTLED lowers the
// connector's send rate; when the SMSC starts accepting again, it recovers.
func TestConnectorAIMDDropsThenRecovers(t *testing.T) {
	const throttleBurst = 6
	var seen atomic.Int32
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		if seen.Add(1) <= throttleBurst {
			return fakesmsc.Throttled()
		}
		return fakesmsc.OK()
	}})

	// throttleBurst throttled submits, then several accepted ones.
	recs := make([]kafka.Record, 0, throttleBurst+4)
	for i := 0; i < throttleBurst+4; i++ {
		rec, err := pipeline.EncodeRouted(routed())
		if err != nil {
			t.Fatalf("encode routed: %v", err)
		}
		recs = append(recs, rec)
	}

	metric := &recordingThrottle{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &continuingConsumer{records: recs},
		CDR:      &fakeCDR{},
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		MaxSendRate: 1000, // high ceiling so pacing is sub-millisecond and the test stays fast
		Throttle:    metric,
		Tracer:      observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	metric.mu.Lock()
	defer metric.mu.Unlock()
	if metric.throttles != throttleBurst {
		t.Errorf("throttle events = %d, want %d", metric.throttles, throttleBurst)
	}
	if len(metric.rates) == 0 {
		t.Fatal("no rate samples recorded")
	}
	// The rate must have fallen well below the 1000 ceiling during the burst...
	minRate := metric.rates[0]
	for _, r := range metric.rates {
		if r < minRate {
			minRate = r
		}
	}
	if minRate >= 1000 {
		t.Errorf("min rate = %v, want it dropped below the ceiling 1000 during the throttle burst", minRate)
	}
	// ...and recovered above that low by the end, once the SMSC accepted again.
	if last := metric.rates[len(metric.rates)-1]; last <= minRate {
		t.Errorf("final rate = %v, want it recovered above the low %v after the SMSC accepted again", last, minRate)
	}
}

// poolBind is the BindConfig the pool tests share, parameterised by pool size and SMSC address.
func poolBind(addr string, poolSize int) connectorpool.BindConfig {
	return connectorpool.BindConfig{
		Addr: addr, SystemID: "esme", Password: "pw",
		DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
		EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		BindPoolSize: poolSize,
	}
}

// runPool drives records through a pool of poolSize binds in one batch and returns once processed.
func runPool(t *testing.T, smsc *fakesmsc.Server, poolSize int, recs []kafka.Record) {
	t.Helper()
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &batchConsumer{records: recs},
		CDR:      &fakeCDR{},
		Bind:     poolBind(smsc.Addr(), poolSize),
		Tracer:   observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run(pool=%d): %v", poolSize, err)
	}
}

// maxConcurrentSubmits runs distinct messages through a pool and reports the peak number of submit_sm
// in flight at the fake SMSC at once — the concurrency the bind pool achieves.
func maxConcurrentSubmits(t *testing.T, poolSize, messages int) int32 {
	t.Helper()
	var inFlight, maxSeen atomic.Int32
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		cur := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if cur <= m || maxSeen.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(3 * time.Millisecond) // a response latency, so concurrent binds overlap observably
		inFlight.Add(-1)
		return fakesmsc.OK()
	}})

	recs := make([]kafka.Record, 0, messages)
	for i := 0; i < messages; i++ {
		rec, err := pipeline.EncodeRouted(routed()) // distinct MessageID each → spread across shards
		if err != nil {
			t.Fatalf("encode routed: %v", err)
		}
		recs = append(recs, rec)
	}
	runPool(t, smsc, poolSize, recs)
	return maxSeen.Load()
}

// TestBindPoolRaisesConcurrency is the step-124 throughput acceptance: bind_pool_size=4 submits several
// messages at once, where a single bind is strictly one-at-a-time. Peak concurrency is the falsifiable
// proxy for aggregate throughput (a slow-link response latency makes the overlap observable).
func TestBindPoolRaisesConcurrency(t *testing.T) {
	const messages = 40
	single := maxConcurrentSubmits(t, 1, messages)
	if single != 1 {
		t.Errorf("single bind peak concurrency = %d, want 1 (one-at-a-time)", single)
	}
	pooled := maxConcurrentSubmits(t, 4, messages)
	if pooled <= single {
		t.Errorf("pool=4 peak concurrency = %d, want > single-bind %d (parallel binds raise throughput)", pooled, single)
	}
}

// TestBindPoolKeepsSegmentsOnOneBindInOrder is the step-124 ordering invariant (§7.3): every segment of
// a multipart message shares the message id, hashes to one shard, and therefore rides ONE bind, in
// segment order — even while other messages keep the other binds busy.
func TestBindPoolKeepsSegmentsOnOneBindInOrder(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{
		RecordSubmits: true,
		OnSubmit:      func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Delay(time.Millisecond) },
	})

	const segments = 5
	multipartID := uuid.New()
	var recs []kafka.Record
	// A 5-segment multipart message, all sharing multipartID. Each segment body carries a real 8-bit
	// concatenation UDH (05 00 03 ref total seq …) so the fake can read back the part number and assert
	// on-the-wire ordering, not just co-location.
	for seq := 1; seq <= segments; seq++ {
		r := routed()
		r.MessageID = multipartID
		r.To = "+2250700000042"
		r.SegmentSeq, r.SegmentCount, r.HasUDH = seq, segments, true
		r.Body = msg.NewBody([]byte{0x05, 0x00, 0x03, 0xAB, byte(segments), byte(seq), 'h', 'i'})
		rec, err := pipeline.EncodeRouted(r)
		if err != nil {
			t.Fatalf("encode segment %d: %v", seq, err)
		}
		recs = append(recs, rec)
	}
	// Plus a dozen unrelated single messages to keep the other binds busy and force interleaving.
	for i := 0; i < 12; i++ {
		rec, err := pipeline.EncodeRouted(routed())
		if err != nil {
			t.Fatalf("encode filler %d: %v", i, err)
		}
		recs = append(recs, rec)
	}

	runPool(t, smsc, 4, recs)

	// Collect the multipart message's segments as the SMSC saw them, in arrival order.
	var conns []int
	var seqs []int
	for _, s := range smsc.Submits() {
		if s.SM.DestinationAddr != "+2250700000042" {
			continue
		}
		conns = append(conns, s.ConnID)
		// The UDH concat header carries the 1-based part number in its last byte (…, ref, total, seq).
		sm := s.SM.ShortMessage
		if len(sm) >= 6 && sm[0] == 0x05 && sm[1] == 0x00 {
			seqs = append(seqs, int(sm[5]))
		}
	}
	if len(conns) != segments {
		t.Fatalf("saw %d segments for the multipart message, want %d", len(conns), segments)
	}
	for i, c := range conns {
		if c != conns[0] {
			t.Errorf("segment %d rode bind %d, want the same bind %d as the others (§7.3)", i, c, conns[0])
		}
	}
	if len(seqs) != segments {
		t.Fatalf("extracted %d part numbers from the UDH, want %d (ordering check would be vacuous)", len(seqs), segments)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("segments arrived out of order: %v (want ascending part numbers)", seqs)
			break
		}
	}
}

// fakeAgg captures the breaker states the pool's heartbeat reports.
type fakeAgg struct {
	mu     sync.Mutex
	last   map[int]breaker.State
	report int
}

func (f *fakeAgg) Report(_ context.Context, _ string, bindIndex int, s breaker.State) (breaker.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.last == nil {
		f.last = map[int]breaker.State{}
	}
	f.last[bindIndex] = s
	f.report++
	return s, nil
}

func (f *fakeAgg) state(idx int) breaker.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last[idx]
}

func (f *fakeAgg) reports() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.report
}

// feedThenBlock feeds each record once (one-per-batch) then blocks until ctx is cancelled, keeping the
// pool alive so the breaker heartbeat can publish.
type feedThenBlock struct{ records []kafka.Record }

func (c *feedThenBlock) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	for _, r := range c.records {
		_ = handle(ctx, []kafka.Record{r})
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestBreakerOpensAndIsReported: a burst of connector-health failures (ESME_RSYSERR) trips the bind's
// breaker, and the heartbeat publishes the open state through the injected aggregator (step-121/122
// wired into the pool).
func TestBreakerOpensAndIsReported(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SysErr() }})
	recs := make([]kafka.Record, 0, 5)
	for i := 0; i < 5; i++ {
		rec, err := pipeline.EncodeRouted(routed())
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		recs = append(recs, rec)
	}

	agg := &fakeAgg{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:         &feedThenBlock{records: recs},
		CDR:              &fakeCDR{},
		Bind:             poolBind(smsc.Addr(), 1),
		Breaker:          agg,
		BreakerConfig:    breaker.Config{MinRequests: 3, FailureRate: 0.5},
		BreakerHeartbeat: 10 * time.Millisecond,
		Tracer:           observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	if !waitFor(3*time.Second, func() bool { return agg.state(0) == breaker.Open }) {
		cancel()
		<-done
		t.Fatalf("breaker did not open (last reported state = %v, %d reports)", agg.state(0), agg.reports())
	}
	cancel()
	<-done
}

// TestBreakerHealthyReportsClosed: with the SMSC accepting, the heartbeat reports a closed breaker.
func TestBreakerHealthyReportsClosed(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{})
	rec, err := pipeline.EncodeRouted(routed())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	agg := &fakeAgg{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:         &feedThenBlock{records: []kafka.Record{rec}},
		CDR:              &fakeCDR{},
		Bind:             poolBind(smsc.Addr(), 1),
		Breaker:          agg,
		BreakerHeartbeat: 10 * time.Millisecond,
		Tracer:           observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	if !waitFor(2*time.Second, func() bool { return agg.reports() > 0 }) {
		cancel()
		<-done
		t.Fatal("heartbeat never reported")
	}
	if got := agg.state(0); got != breaker.Closed {
		t.Errorf("healthy breaker reported %v, want closed", got)
	}
	cancel()
	<-done
}
