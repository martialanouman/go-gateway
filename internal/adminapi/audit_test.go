package adminapi_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// fakeAuditLog records intents and outcomes in memory.
type fakeAuditLog struct {
	mu       sync.Mutex
	intents  []cp.AuditIntent
	ids      []uuid.UUID
	finished map[uuid.UUID]int
	beginErr error
}

func newFakeAuditLog() *fakeAuditLog { return &fakeAuditLog{finished: map[uuid.UUID]int{}} }

func (f *fakeAuditLog) Begin(_ context.Context, in cp.AuditIntent) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beginErr != nil {
		return uuid.Nil, f.beginErr
	}
	id := uuid.New()
	f.intents = append(f.intents, in)
	f.ids = append(f.ids, id)
	return id, nil
}

// Finish models the repository's contract, not a convenient map write: the real one writes once (WHERE
// status IS NULL) and refuses anything that is not an HTTP status.
func (f *fakeAuditLog) Finish(_ context.Context, id uuid.UUID, status int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if status < 100 || status > 599 {
		return fmt.Errorf("record audit outcome: %d is not an HTTP status", status)
	}
	if _, done := f.finished[id]; done {
		return nil
	}
	f.finished[id] = status
	return nil
}

func (f *fakeAuditLog) snapshot() ([]cp.AuditIntent, []uuid.UUID, map[uuid.UUID]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fin := make(map[uuid.UUID]int, len(f.finished))
	for k, v := range f.finished {
		fin[k] = v
	}
	return append([]cp.AuditIntent(nil), f.intents...), append([]uuid.UUID(nil), f.ids...), fin
}

// auditOrderStore is a customer store that notes how many audit intents existed when the handler reached
// it — the only way to prove the trail is written BEFORE the action — and can panic on demand.
type auditOrderStore struct {
	*fakeCustomerStore
	audit          *fakeAuditLog
	intentsAtWrite int
	created        int
	panicOnCreate  bool
}

func (s *auditOrderStore) Create(ctx context.Context, in cp.NewCustomer) (cp.Customer, error) {
	if s.panicOnCreate {
		panic("store exploded")
	}
	intents, _, _ := s.audit.snapshot()
	s.intentsAtWrite = len(intents)
	s.created++
	return s.fakeCustomerStore.Create(ctx, in)
}

func postCustomer(t *testing.T, h http.Handler, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/customers?trace=on", strings.NewReader(`{"name":"acme"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestAuditRecordsAMutationBeforeAndAfterItsHandler: a write is recorded before its handler runs — who
// (the token's fingerprint, never the token), which operation, which path without its query string — and
// completed with the status the client received.
func TestAuditRecordsAMutationBeforeAndAfterItsHandler(t *testing.T) {
	audit := newFakeAuditLog()
	store := &auditOrderStore{fakeCustomerStore: newFakeCustomerStore(), audit: audit}
	h := newTestAPIWith(t, adminapi.Deps{Customers: store, AuditLog: audit})

	rec := postCustomer(t, h, operatorToken)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body)
	}

	intents, ids, finished := audit.snapshot()
	if len(intents) != 1 {
		t.Fatalf("recorded %d intents, want 1", len(intents))
	}
	if store.intentsAtWrite != 1 {
		t.Errorf("the handler ran with %d intents recorded, want 1: the trail is written before the action",
			store.intentsAtWrite)
	}
	got := intents[0]
	if got.Operator != auth.Fingerprint(operatorToken) {
		t.Errorf("operator = %q, want the token's fingerprint", got.Operator)
	}
	if got.OperationID != "create-customer" || got.Method != http.MethodPost || got.Target != "/v1/admin/customers" {
		t.Errorf("intent = %+v, want create-customer POST /v1/admin/customers (no query string)", got)
	}
	if got.RequestID == "" {
		t.Error("request id is empty, want chi's request id for log correlation")
	}
	if status := finished[ids[0]]; status != http.StatusCreated {
		t.Errorf("outcome = %d, want 201", status)
	}
}

// TestAuditRefusesTheRequestWhenTheIntentCannotBeRecorded: no trail, no action — the handler is never
// called, and the client learns only that a dependency is unavailable.
func TestAuditRefusesTheRequestWhenTheIntentCannotBeRecorded(t *testing.T) {
	audit := newFakeAuditLog()
	audit.beginErr = errors.New("pg: connection refused to db.internal")
	store := &auditOrderStore{fakeCustomerStore: newFakeCustomerStore(), audit: audit}
	h := newTestAPIWith(t, adminapi.Deps{Customers: store, AuditLog: audit})

	rec := postCustomer(t, h, operatorToken)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "service_unavailable") {
		t.Errorf("body = %s, want the service_unavailable code", rec.Body)
	}
	if strings.Contains(rec.Body.String(), "db.internal") {
		t.Errorf("body = %s leaks the store error", rec.Body)
	}
	if store.created != 0 {
		t.Errorf("the handler created %d customer(s) without an audit row", store.created)
	}
}

// TestAuditSkipsReadsAndUnauthenticatedRequests: a plain read leaves no row, and neither does a request
// the auth middleware refused — an unauthenticated caller must not be able to make Postgres write.
func TestAuditSkipsReadsAndUnauthenticatedRequests(t *testing.T) {
	audit := newFakeAuditLog()
	store := &auditOrderStore{fakeCustomerStore: newFakeCustomerStore(), audit: audit}

	h := newTestAPIWith(t, adminapi.Deps{Customers: store, AuditLog: audit})
	h.ServeHTTP(httptest.NewRecorder(), authed(t, http.MethodGet, "/v1/admin/customers", ""))
	if rec := postCustomer(t, h, "not-a-token"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token status = %d, want 401", rec.Code)
	}

	readOnly := newTestAPIWithScopes(t, adminapi.Deps{Customers: store, AuditLog: audit}, "admin:read")
	if rec := postCustomer(t, readOnly, operatorToken); rec.Code != http.StatusForbidden {
		t.Fatalf("read-only token status = %d, want 403", rec.Code)
	}

	if intents, _, _ := audit.snapshot(); len(intents) != 0 {
		t.Errorf("recorded %v, want nothing for a read, a 401 and a 403", intents)
	}
}

// TestAuditRecordsRevealReadsOnly: a search is a read, but with msisdn:reveal it shows subscriber numbers
// in clear — that act is recorded; the masked search is not.
func TestAuditRecordsRevealReadsOnly(t *testing.T) {
	store := &fakeSearchStore{rows: []clickhouse.CDRRow{searchRowFixture("33612345678")}}

	masked := newFakeAuditLog()
	if code, _, _ := doSearch(t, adminapi.Deps{MessageSearch: store, AuditLog: masked}, "admin:read", searchWindow()); code != http.StatusOK {
		t.Fatalf("masked search = %d, want 200", code)
	}
	if intents, _, _ := masked.snapshot(); len(intents) != 0 {
		t.Errorf("masked search recorded %v, want nothing", intents)
	}

	revealed := newFakeAuditLog()
	code, _, _ := doSearch(t, adminapi.Deps{MessageSearch: store, AuditLog: revealed}, "admin:read|msisdn:reveal", searchWindow())
	if code != http.StatusOK {
		t.Fatalf("revealing search = %d, want 200", code)
	}
	intents, _, _ := revealed.snapshot()
	if len(intents) != 1 || intents[0].OperationID != "search-messages" || intents[0].Target != "/v1/admin/messages/search" {
		t.Errorf("revealing search recorded %+v, want one search-messages row on the bare path", intents)
	}
}

// TestAuditRecordsAnExactRouteListing: the MNP override table is a list of subscriber numbers returned in
// clear under admin:read alone — no scope marks it, so the trail records it whatever the caller holds.
func TestAuditRecordsAnExactRouteListing(t *testing.T) {
	audit := newFakeAuditLog()
	h := newTestAPIWithScopes(t, adminapi.Deps{ExactRoutes: newFakeExactRouteStore(), AuditLog: audit}, "admin:read")
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/exact-routes", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}
	intents, _, _ := audit.snapshot()
	if len(intents) != 1 || intents[0].OperationID != "list-exact-routes" {
		t.Errorf("recorded %+v, want one list-exact-routes row", intents)
	}
}

// TestAuditNeedsAVerifier: without a token verifier there is no identity to record, so the trail is not
// wired at all rather than filling up with "unknown" — a trail that looks sound and names no one.
func TestAuditNeedsAVerifier(t *testing.T) {
	audit := newFakeAuditLog()
	store := &auditOrderStore{fakeCustomerStore: newFakeCustomerStore(), audit: audit}
	mux, _ := adminapi.New(adminapi.Deps{Customers: store, AuditLog: audit}) // no Verifier

	rec := postCustomer(t, mux, operatorToken)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (no verifier means no authentication at all); body=%s", rec.Code, rec.Body)
	}
	if intents, _, _ := audit.snapshot(); len(intents) != 0 {
		t.Errorf("recorded %+v, want nothing: an unidentified operator is not an audit trail", intents)
	}
}

// TestAuditRecordsAnExportRetrieval: the export status read hands over the download URL of an artefact
// that may hold unmasked numbers, and cdr:export_bulk alone opens it — so it is recorded whatever scopes
// the caller holds, unlike the reads that only unmask under msisdn:reveal.
func TestAuditRecordsAnExportRetrieval(t *testing.T) {
	jobs := newFakeExportJobs()
	job, err := jobs.Create(context.Background(), cp.NewMessageExportJob{
		Format: cp.ExportFormatCSV, Masked: false, Filters: []byte(`{}`), Operator: "tok_0123456789abcdef",
		ExpiresAt: time.Now().Add(time.Hour).UTC(),
	})
	if err != nil {
		t.Fatalf("seed the job: %v", err)
	}

	audit := newFakeAuditLog()
	h := newTestAPIWithScopes(t, adminapi.Deps{ExportJobs: jobs, AuditLog: audit}, "cdr:export_bulk")
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/export/"+job.ID.String(), http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body)
	}

	intents, _, _ := audit.snapshot()
	if len(intents) != 1 || intents[0].OperationID != "get-message-export" {
		t.Errorf("recorded %+v, want one get-message-export row", intents)
	}
}

// TestAuditRecordsAPanickingHandlerAs500: the outcome is written even when the handler panics — as the
// 500 the recoverer answers — and the panic still reaches the recoverer.
func TestAuditRecordsAPanickingHandlerAs500(t *testing.T) {
	audit := newFakeAuditLog()
	store := &auditOrderStore{fakeCustomerStore: newFakeCustomerStore(), audit: audit, panicOnCreate: true}
	h := newTestAPIWith(t, adminapi.Deps{Customers: store, AuditLog: audit})

	rec := postCustomer(t, h, operatorToken)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want the recoverer's 500", rec.Code)
	}
	_, ids, finished := audit.snapshot()
	if len(ids) != 1 || finished[ids[0]] != http.StatusInternalServerError {
		t.Errorf("outcomes = %v, want the one intent completed with 500", finished)
	}
}
