package connectorpool_test

import (
	"bytes"
	"context"
	"encoding/binary"
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

func routed() pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: "+2250700000000", Body: msg.NewBodyString("hello"),
		Encoding: "gsm7", ConnectorID: uuid.New(), SegmentCount: 1, SubmittedAt: time.Now().UTC(),
	}
}

func runOnce(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, r pipeline.RoutedMT) *fakeCDR {
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

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return cdr
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

func TestConnectorWritesFailedOnThrottled(t *testing.T) {
	cdr := runOnce(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() }, routed())

	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusFailed {
		t.Errorf("status: got %q want failed", row.Status)
	}
	if row.ErrorCode == nil || *row.ErrorCode != "rate_limited" {
		t.Errorf("error_code: got %v want rate_limited", row.ErrorCode)
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
