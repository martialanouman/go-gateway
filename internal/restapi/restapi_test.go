package restapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type fakePrincipals struct {
	principal cp.APIKeyPrincipal
	found     bool
	err       error
}

func (f fakePrincipals) PrincipalByAPIKeyHash(context.Context, string) (cp.APIKeyPrincipal, bool, error) {
	return f.principal, f.found, f.err
}

type fakeProducer struct {
	mu      sync.Mutex
	records []kafka.Record
	err     error
}

func (f *fakeProducer) Produce(_ context.Context, rec kafka.Record) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	return nil
}

func (f *fakeProducer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

type fakeCDRReader struct {
	row   clickhouse.CDRRow
	found bool
	err   error

	// listRows/listErr drive List; gotFilter/gotLimit capture its last call for assertions.
	listRows  []clickhouse.CDRRow
	listErr   error
	gotFilter clickhouse.CDRListFilter
	gotLimit  int
}

func (f fakeCDRReader) Current(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return f.row, f.found, f.err
}

// List is a pointer receiver so the handler test can read back the filter and limit the handler
// passed (a value receiver would capture a copy).
func (f *fakeCDRReader) List(_ context.Context, _, _ uuid.UUID, filter clickhouse.CDRListFilter, limit int) ([]clickhouse.CDRRow, error) {
	f.gotFilter = filter
	f.gotLimit = limit
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listRows, nil
}

type fakeCDRWriter struct {
	mu   sync.Mutex
	rows []clickhouse.CDRRow
}

func (f *fakeCDRWriter) InsertBatch(_ context.Context, rows []clickhouse.CDRRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
	return nil
}

func (f *fakeCDRWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func activePrincipal() cp.APIKeyPrincipal {
	return cp.APIKeyPrincipal{
		AccountID: uuid.New(), CustomerID: uuid.New(),
		AccountStatus: cp.AccountActive, CustomerStatus: cp.CustomerActive, RESTEnabled: true,
	}
}

type harness struct {
	server   *httptest.Server
	producer *fakeProducer
	cdrWrite *fakeCDRWriter
}

func newHarness(t *testing.T, principals restapi.PrincipalStore, reader restapi.CDRReader) *harness {
	return buildHarness(t, principals, reader, nil)
}

// buildHarness is the shared builder; idempotency is nil for the M2 tests and set by the
// Idempotency-Key tests.
func buildHarness(t *testing.T, principals restapi.PrincipalStore, reader restapi.CDRReader, idem restapi.IdempotencyStore) *harness {
	t.Helper()
	rec := otelrec.New(t)
	producer := &fakeProducer{}
	cdrWrite := &fakeCDRWriter{}

	mux, _ := restapi.New(restapi.Deps{
		Principals:  principals,
		Ingestor:    ingest.NewIngestor(producer, nil),
		CDRReader:   reader,
		Idempotency: idem,
		Tracer:      observability.Tracer(rec.Provider(), "rest-api"),
		Version:     "test",
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &harness{server: srv, producer: producer, cdrWrite: cdrWrite}
}

// replayBatch is a one-shot BatchConsumer that feeds a fixed set of records to the handler once — it lets a
// test drive the AcceptedConsumer over the records the submit produced, mirroring how router-svc projects the
// accepted row off mt.inbound.
type replayBatch struct{ recs []kafka.Record }

func (r replayBatch) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	handle(ctx, r.recs)
	return nil
}

// projectAccepted runs the produced mt.inbound records through an AcceptedConsumer into cdrWrite, the way the
// durable accepted-row projection does in production (step-101).
func (h *harness) projectAccepted(t *testing.T) {
	t.Helper()
	h.producer.mu.Lock()
	recs := append([]kafka.Record(nil), h.producer.records...)
	h.producer.mu.Unlock()
	ac := ingest.NewAcceptedConsumer(replayBatch{recs: recs}, h.cdrWrite, nil, nil)
	if err := ac.Run(context.Background()); err != nil {
		t.Fatalf("project accepted: %v", err)
	}
}

func (h *harness) do(t *testing.T, method, path, auth string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, h.server.URL+path, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := h.server.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestHealthIsPublic(t *testing.T) {
	h := newHarness(t, fakePrincipals{}, &fakeCDRReader{})
	resp := h.do(t, http.MethodGet, "/v1/health", "", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body restapi.Health
	decode(t, resp, &body)
	if body.Status != "ok" {
		t.Errorf("health status: got %q", body.Status)
	}
}

func TestSubmitRequiresAuth(t *testing.T) {
	h := newHarness(t, fakePrincipals{found: false}, &fakeCDRReader{})
	resp := h.do(t, http.MethodPost, "/v1/messages", "", map[string]any{
		"to": "+2250700000000", "from": "ACME", "text": "hi",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d want 401", resp.StatusCode)
	}
}

func TestSubmitAcceptedAndDurable(t *testing.T) {
	principal := activePrincipal()
	h := newHarness(t, fakePrincipals{principal: principal, found: true}, &fakeCDRReader{})

	resp := h.do(t, http.MethodPost, "/v1/messages", "sgw_testkey", map[string]any{
		"to": "+2250700000000", "from": "ACME", "text": "Your OTP is 123456",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d want 202", resp.StatusCode)
	}
	var body restapi.AcceptedMessage
	decode(t, resp, &body)
	if body.Status != "accepted" {
		t.Errorf("status: got %q want accepted", body.Status)
	}
	if _, err := uuid.Parse(body.ID); err != nil {
		t.Errorf("id is not a uuid: %q", body.ID)
	}
	if _, err := uuid.Parse(body.TraceID); err != nil {
		t.Errorf("trace_id is not a uuid: %q", body.TraceID)
	}

	// The 202 is earned by the durable produce to mt.inbound.
	if h.producer.count() != 1 {
		t.Fatalf("expected 1 mt.inbound record, got %d", h.producer.count())
	}
	rec := h.producer.records[0]
	if rec.Topic != kafka.TopicMTInbound {
		t.Errorf("topic: got %q", rec.Topic)
	}
	env, err := pipeline.DecodeInbound(rec)
	if err != nil {
		t.Fatalf("decode inbound: %v", err)
	}
	if env.AccountID != principal.AccountID || env.CustomerID != principal.CustomerID {
		t.Error("principal ids not propagated to the envelope")
	}
	if env.MessageID.String() != body.ID {
		t.Error("message id in the 202 must match the envelope")
	}

	// The accepted CDR row is projected durably off mt.inbound (step-101). Drive that projection over the
	// record this submit produced.
	h.projectAccepted(t)
	if h.cdrWrite.count() != 1 {
		t.Fatalf("accepted CDR rows = %d, want 1", h.cdrWrite.count())
	}
	if h.cdrWrite.rows[0].Status != clickhouse.StatusAccepted {
		t.Errorf("accepted row status: got %q", h.cdrWrite.rows[0].Status)
	}
	// The accepted row's destination is normalized to the same canonical (no-"+") form the router
	// stores, so a message spells its destination the same across all its lifecycle rows.
	if got := h.cdrWrite.rows[0].DestAddr; got != "2250700000000" {
		t.Errorf("accepted row dest = %q, want the normalized 2250700000000", got)
	}
}

func TestSubmitRejectsWhenRestDisabled(t *testing.T) {
	principal := activePrincipal()
	principal.RESTEnabled = false
	h := newHarness(t, fakePrincipals{principal: principal, found: true}, &fakeCDRReader{})

	resp := h.do(t, http.MethodPost, "/v1/messages", "sgw_key", map[string]any{
		"to": "+2250700000000", "from": "ACME", "text": "hi",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status: got %d want 403", resp.StatusCode)
	}
}

func TestSubmitRejectsBatchBody(t *testing.T) {
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{})
	resp := h.do(t, http.MethodPost, "/v1/messages", "sgw_key", map[string]any{
		"messages": []map[string]any{{"to": "+2250700000000", "from": "ACME", "text": "hi"}},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("batch body should be 422 in M2, got %d", resp.StatusCode)
	}
}

func TestGetMessageFoundAndNotFound(t *testing.T) {
	id := uuid.New()
	principal := activePrincipal()
	row := clickhouse.CDRRow{
		MessageID: id, TraceID: uuid.New(), Direction: clickhouse.DirectionMT,
		SourceAddr: "ACME", DestAddr: "+2250700000000", Status: clickhouse.StatusEnroute,
		SegmentCount: 1, Encoding: clickhouse.EncodingGSM7, SubmittedAt: time.Now().UTC(),
	}

	found := newHarness(t, fakePrincipals{principal: principal, found: true}, &fakeCDRReader{row: row, found: true})
	resp := found.do(t, http.MethodGet, "/v1/messages/"+id.String(), "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var msg restapi.Message
	decode(t, resp, &msg)
	if msg.Status != "enroute" || msg.ID != id.String() {
		t.Errorf("message: got status=%q id=%q", msg.Status, msg.ID)
	}

	missing := newHarness(t, fakePrincipals{principal: principal, found: true}, &fakeCDRReader{found: false})
	resp2 := missing.do(t, http.MethodGet, "/v1/messages/"+uuid.New().String(), "sgw_key", nil)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("missing message: got %d want 404", resp2.StatusCode)
	}
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
