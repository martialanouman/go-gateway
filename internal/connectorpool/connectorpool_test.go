package connectorpool_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"

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

type fakeConsumer struct{ records []kafka.Record }

func (f *fakeConsumer) Run(ctx context.Context, handle kafka.Handler) error {
	for _, r := range f.records {
		if err := handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

type fakeCDR struct{ rows []clickhouse.CDRRow }

func (f *fakeCDR) Insert(_ context.Context, row clickhouse.CDRRow) error {
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
	connectorID    uuid.UUID
	smscMsgID      string
	messageID      uuid.UUID
	traceID        uuid.UUID
	validityPeriod *string
}

// fakeDLRMap records the mappings the connector writes, so a test can assert a successful submit is
// remembered. A non-nil err drives the best-effort failure path.
type fakeDLRMap struct {
	puts []dlrPut
	err  error
}

func (f *fakeDLRMap) Put(_ context.Context, connectorID uuid.UUID, smscMsgID string, messageID, traceID uuid.UUID, validityPeriod *string) error {
	f.puts = append(f.puts, dlrPut{connectorID, smscMsgID, messageID, traceID, validityPeriod})
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
	if got.connectorID != r.ConnectorID || got.messageID != r.MessageID || got.traceID != r.TraceID {
		t.Errorf("mapping ids = %+v, want connector %s / message %s / trace %s",
			got, r.ConnectorID, r.MessageID, r.TraceID)
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

// TestConnectorTranscodesUCS2Body pins that a ucs2 message reaches the wire as UTF-16BE with the
// UCS-2 data_coding, not as the raw UTF-8 bytes msg.Body stores (which the handset would garble).
func TestConnectorTranscodesUCS2Body(t *testing.T) {
	const text = "café ☕" // non-ASCII: UTF-8 and UTF-16BE differ
	var seen []byte
	var dcs uint8
	r := routed()
	r.Encoding = "ucs2"
	r.Body = msg.NewBodyString(text)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = append([]byte(nil), sm.ShortMessage...)
		dcs = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	want := utf16BE(text)
	if !bytes.Equal(seen, want) {
		t.Errorf("ucs2 body on the wire = % x, want UTF-16BE % x (not raw UTF-8 % x)", seen, want, []byte(text))
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

// TestConnectorDCSAndBodyAgree pins that the wire body is transcoded to match the EFFECTIVE
// data_coding, not r.Encoding: a UCS-2 override with encoding=gsm7 must still ship UTF-16BE, so the
// DCS label and the bytes never disagree.
func TestConnectorDCSAndBodyAgree(t *testing.T) {
	const text = "héllo" // non-ASCII: UTF-8 and UTF-16BE differ
	dc := int(smpp.DataCodingUCS2)
	var body []byte
	var dcs uint8
	r := routed()
	r.Encoding = "gsm7"
	r.DataCoding = &dc
	r.Body = msg.NewBodyString(text)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		body = append([]byte(nil), sm.ShortMessage...)
		dcs = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	if dcs != smpp.DataCodingUCS2 {
		t.Fatalf("data_coding = %#x, want the UCS-2 override %#x", dcs, smpp.DataCodingUCS2)
	}
	if !bytes.Equal(body, utf16BE(text)) {
		t.Errorf("body = % x, want UTF-16BE % x to match the UCS-2 DCS", body, utf16BE(text))
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

func (blockingConsumer) Run(ctx context.Context, _ kafka.Handler) error {
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
