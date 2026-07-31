package smppserver

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace/noop"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// captureIngestor records the envelope handed to Accept, and returns a preset error, so the submit_sm
// handler's mapping can be asserted without a real producer.
type captureIngestor struct {
	env   pipeline.InboundMT
	err   error
	calls int
}

func (c *captureIngestor) Accept(_ context.Context, env pipeline.InboundMT) error {
	c.calls++
	c.env = env
	return c.err
}

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

// submitReq is a representative submit_sm request: a DLR-requesting GSM7 message.
func submitReq() session.SubmitRequest {
	return session.SubmitRequest{
		Source:             "ACME",
		Destination:        "+2250700000000",
		ESMClass:           smpp.ESMClassDefault,
		DataCoding:         smpp.DataCodingGSM7,
		RegisteredDelivery: smpp.RegisteredDeliveryReceipt,
		Body:               msg.NewBody([]byte("Your OTP is 123456")),
	}
}

func TestOnSubmitMapsPDUToEnvelope(t *testing.T) {
	acc, cust := uuid.New(), uuid.New()
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())

	st := &connState{accountID: acc, customerID: cust}
	res := l.onSubmit(context.Background(), st)(context.Background(), submitReq())

	if res.Status != smpp.StatusOK {
		t.Fatalf("status = %#x, want ESME_ROK", res.Status)
	}
	if ci.calls != 1 {
		t.Fatalf("ingestor called %d times, want 1", ci.calls)
	}
	env := ci.env
	if res.MessageID != env.MessageID.String() {
		t.Errorf("submit_sm_resp message_id %q must match the envelope %q", res.MessageID, env.MessageID)
	}
	if env.AccountID != acc || env.CustomerID != cust {
		t.Error("bind identity not propagated to the envelope")
	}
	if env.From != "ACME" || env.To != "+2250700000000" {
		t.Errorf("addresses not mapped: from=%q to=%q", env.From, env.To)
	}
	if string(env.Body.Reveal()) != "Your OTP is 123456" {
		t.Errorf("body not carried: %q", env.Body.Reveal())
	}
	if env.ESMClass != smpp.ESMClassDefault {
		t.Errorf("esm_class = %#x", env.ESMClass)
	}
	if env.DataCoding == nil || *env.DataCoding != int(smpp.DataCodingGSM7) {
		t.Errorf("data_coding not mapped: %v", env.DataCoding)
	}
	if !env.RegisteredDelivery {
		t.Error("registered_delivery bit not mapped to true")
	}
	if env.Encoding != "gsm7" {
		t.Errorf("encoding = %q, want gsm7 (resolved from data_coding 0)", env.Encoding)
	}
}

func TestOnSubmitResolvesEncodingFromDataCoding(t *testing.T) {
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())
	req := submitReq()
	req.DataCoding = smpp.DataCodingUCS2

	l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), req)

	if ci.env.Encoding != "ucs2" {
		t.Errorf("encoding = %q, want ucs2 (resolved from data_coding)", ci.env.Encoding)
	}
	if ci.env.DataCoding == nil || *ci.env.DataCoding != int(smpp.DataCodingUCS2) {
		t.Errorf("data_coding override not carried: %v", ci.env.DataCoding)
	}
}

func TestOnSubmitCarriesValidityAndPriority(t *testing.T) {
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())
	req := submitReq()
	req.ValidityPeriod = "000002000000000R" // relative 2 days (SMPP v3.4 §7.1.1)
	req.PriorityFlag = 2

	l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), req)

	if ci.env.ValidityPeriod == nil || *ci.env.ValidityPeriod != "000002000000000R" {
		t.Errorf("validity_period not carried: %v", ci.env.ValidityPeriod)
	}
	if ci.env.Priority != 2 {
		t.Errorf("priority = %d, want 2", ci.env.Priority)
	}
}

func TestOnSubmitEmptyValidityMapsNil(t *testing.T) {
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())

	l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), submitReq()) // submitReq leaves ValidityPeriod empty

	if ci.env.ValidityPeriod != nil {
		t.Errorf("empty validity_period must map to nil, got %v", *ci.env.ValidityPeriod)
	}
}

func TestOnSubmitUsesMessagePayloadWhenShortMessageEmpty(t *testing.T) {
	const long = "this body is carried in the message_payload TLV because it is empty in short_message"
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())

	req := submitReq()
	req.Body = msg.NewBody(nil) // short_message empty, as an SMSC sends a >254-octet message
	req.TLVs = smpp.TLVList{{Tag: smpp.TagMessagePayload, Value: []byte(long)}}

	res := l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), req)

	if res.Status != smpp.StatusOK {
		t.Fatalf("status = %#x, want ESME_ROK", res.Status)
	}
	if got := string(ci.env.Body.Reveal()); got != long {
		t.Errorf("body = %q, want the message_payload content %q", got, long)
	}
}

func TestOnSubmitRegisteredDeliveryFailureOnlyMapsTrue(t *testing.T) {
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())
	req := submitReq()
	req.RegisteredDelivery = 0x02 // SMSC delivery receipt on failure only (SMPP v3.4 §5.2.17)

	l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), req)

	if !ci.env.RegisteredDelivery {
		t.Error("registered_delivery = 0x02 (failure-only receipt) must map to true")
	}
}

func TestOnSubmitRegisteredDeliveryOffMapsFalse(t *testing.T) {
	ci := &captureIngestor{}
	l := New(nil, nil, ci, Options{}, discardLog())
	req := submitReq()
	req.RegisteredDelivery = 0

	l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), req)

	if ci.env.RegisteredDelivery {
		t.Error("registered_delivery = 0 must map to false")
	}
}

func TestOnSubmitIngestErrorMapsToCommandStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint32
	}{
		{"service unavailable -> ESME_RSYSERR", errs.ErrServiceUnavailable, errs.StatusSysErr},
		{"internal -> ESME_RSYSERR", errs.ErrInternal, errs.StatusSysErr},
		{"invalid destination -> ESME_RINVDSTADR", errs.ErrInvalidDestination, errs.StatusInvalidDstAddr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ci := &captureIngestor{err: tc.err}
			l := New(nil, nil, ci, Options{}, discardLog())
			res := l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
				context.Background(), submitReq())
			if res.Status != tc.want {
				t.Errorf("status = %#x, want %#x", res.Status, tc.want)
			}
			if res.MessageID != "" {
				t.Errorf("a rejected submit must not echo a message_id, got %q", res.MessageID)
			}
		})
	}
}

func TestOnSubmitNilIngestorRejects(t *testing.T) {
	l := New(nil, nil, nil, Options{}, discardLog())
	res := l.onSubmit(context.Background(), &connState{accountID: uuid.New(), customerID: uuid.New()})(
		context.Background(), submitReq())
	if res.Status != errs.StatusSubmitFail {
		t.Errorf("nil ingestor status = %#x, want ESME_RSUBMITFAIL", res.Status)
	}
}

// TestRESTAndSMPPProduceEquivalentEnvelope is the protocol-parity proof (step-025): the same logical
// message submitted over REST and over SMPP produces an equivalent mt.inbound envelope and the same
// accepted CDR path. Both surfaces share internal/ingest, so this guards that the two mappings feed it
// identically. Ids and the accept timestamp are minted per submission and so are excluded from the
// comparison.
func TestRESTAndSMPPProduceEquivalentEnvelope(t *testing.T) {
	acc, cust := uuid.New(), uuid.New()

	restRec, restRow := submitViaREST(t, acc, cust)
	smppRec, smppRow := submitViaSMPP(t, acc, cust)

	restEnv, err := pipeline.DecodeInbound(restRec)
	if err != nil {
		t.Fatalf("decode REST record: %v", err)
	}
	smppEnv, err := pipeline.DecodeInbound(smppRec)
	if err != nil {
		t.Fatalf("decode SMPP record: %v", err)
	}

	normalizeEnv(&restEnv)
	normalizeEnv(&smppEnv)
	if !reflect.DeepEqual(restEnv, smppEnv) {
		t.Errorf("mt.inbound envelopes differ across protocols:\n REST=%+v\n SMPP=%+v", restEnv, smppEnv)
	}

	// Same CDR path: both surfaces project an accepted row with the same canonical shape.
	if restRow.Status != clickhouse.StatusAccepted || smppRow.Status != clickhouse.StatusAccepted {
		t.Fatalf("accepted status differs: REST=%q SMPP=%q", restRow.Status, smppRow.Status)
	}
	if restRow.Direction != smppRow.Direction || restRow.DestAddr != smppRow.DestAddr ||
		restRow.SourceAddr != smppRow.SourceAddr || restRow.Encoding != smppRow.Encoding ||
		restRow.SegmentCount != smppRow.SegmentCount {
		t.Errorf("accepted CDR rows differ:\n REST=%+v\n SMPP=%+v", restRow, smppRow)
	}
}

// normalizeEnv zeroes the per-submission fields (ids, timestamp) so two envelopes can be compared for
// the mapped content alone.
func normalizeEnv(e *pipeline.InboundMT) {
	e.MessageID = uuid.UUID{}
	e.TraceID = uuid.UUID{}
	e.SubmittedAt = time.Time{}
}

// submitViaREST drives the real REST submit handler and returns the produced mt.inbound record and the
// accepted CDR row it projected.
func submitViaREST(t *testing.T, acc, cust uuid.UUID) (kafka.Record, clickhouse.CDRRow) {
	t.Helper()
	producer := &fakeProducer{}
	cdr := &fakeCDR{}
	accepted := ingest.NewAcceptedWriter(cdr, nil, 1, 16, nil)
	runAccepted(t, accepted)

	mux, _ := restapi.New(restapi.Deps{
		Principals: fakePrincipals{principal: cp.APIKeyPrincipal{
			AccountID: acc, CustomerID: cust,
			AccountStatus: cp.AccountActive, CustomerStatus: cp.CustomerActive, RESTEnabled: true,
		}, found: true},
		Ingestor:  ingest.NewIngestor(producer, accepted, nil),
		CDRReader: fakeReader{},
		Tracer:    noop.NewTracerProvider().Tracer(""),
		Version:   "test",
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// A concrete encoding matching what the SMPP side resolves from data_coding=0, so the two
	// envelopes' Encoding fields are comparable (REST expresses coding via the enum, SMPP via
	// data_coding, and both must land on the same resolved value).
	body, _ := json.Marshal(map[string]any{
		"to": "+2250700000000", "from": "ACME", "text": "Your OTP is 123456",
		"encoding": "gsm7", "data_coding": int(smpp.DataCodingGSM7), "registered_delivery": true,
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sgw_testkey")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("REST submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("REST submit status = %d, want 202", resp.StatusCode)
	}
	return producer.first(t), waitRow(t, cdr)
}

// submitViaSMPP drives the submit_sm handler directly with an equivalent request and returns the same.
func submitViaSMPP(t *testing.T, acc, cust uuid.UUID) (kafka.Record, clickhouse.CDRRow) {
	t.Helper()
	producer := &fakeProducer{}
	cdr := &fakeCDR{}
	accepted := ingest.NewAcceptedWriter(cdr, nil, 1, 16, nil)
	runAccepted(t, accepted)

	l := New(nil, nil, ingest.NewIngestor(producer, accepted, nil), Options{}, discardLog())
	res := l.onSubmit(context.Background(), &connState{accountID: acc, customerID: cust})(
		context.Background(), submitReq())
	if res.Status != smpp.StatusOK {
		t.Fatalf("SMPP submit status = %#x, want ESME_ROK", res.Status)
	}
	return producer.first(t), waitRow(t, cdr)
}

func runAccepted(t *testing.T, a *ingest.AcceptedWriter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = a.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// --- fakes shared by the parity test ---

type fakeProducer struct {
	mu      sync.Mutex
	records []kafka.Record
}

func (f *fakeProducer) Produce(_ context.Context, rec kafka.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	return nil
}

func (f *fakeProducer) first(t *testing.T) kafka.Record {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.records) != 1 {
		t.Fatalf("expected exactly 1 mt.inbound record, got %d", len(f.records))
	}
	return f.records[0]
}

type fakeCDR struct {
	mu   sync.Mutex
	rows []clickhouse.CDRRow
}

func (f *fakeCDR) InsertBatch(_ context.Context, rows []clickhouse.CDRRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
	return nil
}

// waitRow polls until the async accepted writer has flushed exactly one row, then returns it.
func waitRow(t *testing.T, f *fakeCDR) clickhouse.CDRRow {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := len(f.rows)
		if n == 1 {
			row := f.rows[0]
			f.mu.Unlock()
			return row
		}
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("accepted CDR row never landed")
	return clickhouse.CDRRow{}
}

type fakePrincipals struct {
	principal cp.APIKeyPrincipal
	found     bool
}

func (f fakePrincipals) PrincipalByAPIKeyHash(context.Context, string) (cp.APIKeyPrincipal, bool, error) {
	return f.principal, f.found, nil
}

type fakeReader struct{}

func (fakeReader) Current(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return clickhouse.CDRRow{}, false, nil
}

func (fakeReader) List(context.Context, uuid.UUID, uuid.UUID, clickhouse.CDRListFilter, int) ([]clickhouse.CDRRow, error) {
	return nil, nil
}
