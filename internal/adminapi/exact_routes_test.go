package adminapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/platform/async"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// fakeExactRouteStore is an in-memory ExactRouteAdminStore for handler unit tests. It mirrors the
// repo's contract: Upsert is idempotent by msisdn and refreshes UpdatedAt; List is msisdn-ordered and
// keyset-paginated by the `after` cursor.
type fakeExactRouteStore struct {
	mu   sync.Mutex
	rows map[string]exact.Route
	now  time.Time
	// Optional synchronization for the async e2e test: BulkUpsert signals bulkStarted, then blocks on
	// bulkRelease. Both nil (the default) means BulkUpsert never blocks.
	bulkStarted chan struct{}
	bulkRelease chan struct{}
	// bulkErr models a partially-applied batch: postgres.BulkUpsert is a pgx.Batch, so an error can come
	// back with rows ALREADY committed. The double therefore writes what it was given and then fails,
	// rather than failing cleanly — the failure mode the handler has to cope with.
	bulkErr error
}

func newFakeExactRouteStore() *fakeExactRouteStore {
	return &fakeExactRouteStore{rows: map[string]exact.Route{}, now: time.Unix(1_700_000_000, 0).UTC()}
}

func (s *fakeExactRouteStore) Get(_ context.Context, msisdn string) (exact.Route, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[msisdn]
	return r, ok, nil
}

func (s *fakeExactRouteStore) List(_ context.Context, after string, limit int) ([]exact.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.rows))
	for k := range s.rows {
		if k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]exact.Route, len(keys))
	for i, k := range keys {
		out[i] = s.rows[k]
	}
	return out, nil
}

func (s *fakeExactRouteStore) Upsert(_ context.Context, route exact.Route) (exact.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(time.Second)
	route.UpdatedAt = s.now
	s.rows[route.MSISDN] = route
	return route, nil
}

func (s *fakeExactRouteStore) Delete(_ context.Context, msisdn string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[msisdn]; !ok {
		return false, nil
	}
	delete(s.rows, msisdn)
	return true, nil
}

func (s *fakeExactRouteStore) BulkUpsert(_ context.Context, routes []exact.Route) error {
	if s.bulkStarted != nil {
		close(s.bulkStarted)
		<-s.bulkRelease // hold the import "in flight" until the test releases it
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range routes {
		s.now = s.now.Add(time.Second)
		r.UpdatedAt = s.now
		s.rows[r.MSISDN] = r
	}
	if s.bulkErr != nil {
		return s.bulkErr
	}
	return nil
}

func (s *fakeExactRouteStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// syncRunner runs the job inline, so a test observes the import's effect the moment Go returns — the
// production path is still the real async runner; only the injected runner collapses time.
//
// It swallows the job's error on purpose, because *async.Runner does: Go reports SUBMISSION failures
// (ErrBusy, ErrClosed) and nothing else, logging whatever the job itself returns. Returning it here
// instead made a failing background import surface as a 500 on a request that had already answered 202
// — a failure mode production cannot produce, and one that hid how the handler really behaves when a
// batch fails halfway.
type syncRunner struct{}

func (syncRunner) Go(_ string, job func(context.Context) error) error {
	_ = job(context.Background())
	return nil
}

// TestExactRouteCreateLookupDeleteRoundTrip: a created override is found by lookup and gone after
// delete — the operator's core loop.
func TestExactRouteCreateLookupDeleteRoundTrip(t *testing.T) {
	store := newFakeExactRouteStore()
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store})
	connID := uuid.NewString()

	// Create with a "+"-prefixed number: it must be canonicalized to digits-only before the store.
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"msisdn":"+2250700000001","target_type":"connector","target_id":%q}`, connID)
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	if created["msisdn"] != "2250700000001" {
		t.Errorf("stored msisdn = %v, want the normalized 2250700000001", created["msisdn"])
	}
	if created["source"] != "manual" {
		t.Errorf("source = %v, want the defaulted manual", created["source"])
	}

	// Lookup resolves the same number.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/exact-routes/lookup?msisdn=2250700000001", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("lookup status = %d, want 200; body=%s", w.Code, w.Body)
	}

	// Delete removes it; a second lookup is 404.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/exact-routes/2250700000001", ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204; body=%s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/exact-routes/lookup?msisdn=2250700000001", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("lookup after delete = %d, want 404", w.Code)
	}
}

// TestCreateExactRouteInvalidMSISDNIs422: a non-E.164 msisdn is rejected before the store.
func TestCreateExactRouteInvalidMSISDNIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: newFakeExactRouteStore()})
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"msisdn":"not-a-number","target_type":"connector","target_id":%q}`, uuid.NewString())
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestLookupExactRouteUnknownIs404: an unconfigured number is a 404, the L0 "no override" state.
func TestLookupExactRouteUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: newFakeExactRouteStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/exact-routes/lookup?msisdn=2250799999999", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// TestUpdateExactRoutePreservesImportedAtAndChangesTarget: a PATCH swaps the target but keeps the
// import provenance (imported_at), and refreshes updated_at.
func TestUpdateExactRoutePreservesImportedAtAndChangesTarget(t *testing.T) {
	store := newFakeExactRouteStore()
	importedAt := time.Unix(1_600_000_000, 0).UTC()
	orig := exact.Route{
		MSISDN: "2250700000002", Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()},
		Source: exact.SourceMNPImport, ImportedAt: &importedAt,
	}
	if _, err := store.Upsert(context.Background(), orig); err != nil {
		t.Fatalf("seed: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store})

	newRoute := uuid.NewString()
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"target_type":"route","target_id":%q}`, newRoute)
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/exact-routes/2250700000002", body))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["target_type"] != "route" || got["target_id"] != newRoute {
		t.Errorf("target = {%v %v}, want {route %s}", got["target_type"], got["target_id"], newRoute)
	}
	if got["imported_at"] == nil {
		t.Errorf("imported_at was dropped by the update, want it preserved")
	}
	// The unspecified source column is preserved, not reset — a target-only PATCH keeps provenance.
	if got["source"] != "mnp_import" {
		t.Errorf("source = %v, want the preserved mnp_import", got["source"])
	}
}

// TestUpdateExactRouteUnknownIs404: patching an unconfigured number is a 404, not a silent create.
func TestUpdateExactRouteUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: newFakeExactRouteStore()})
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"target_type":"connector","target_id":%q}`, uuid.NewString())
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/exact-routes/2250700000003", body))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// TestUpdateExactRoutePartialTargetIs422: setting target_type without target_id (or vice versa) is
// rejected — the two form a unit, so a half-set target cannot mis-point the id at the wrong kind.
func TestUpdateExactRoutePartialTargetIs422(t *testing.T) {
	store := newFakeExactRouteStore()
	if _, err := store.Upsert(context.Background(), exact.Route{
		MSISDN: "2250700000004", Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()}, Source: exact.SourceManual,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/exact-routes/2250700000004", `{"target_type":"route"}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (target_type without target_id); body=%s", w.Code, w.Body)
	}
}

// TestListExactRoutesPaginates: the keyset cursor walks every row once, in msisdn order, with has_more
// set until the last page.
func TestListExactRoutesPaginates(t *testing.T) {
	store := newFakeExactRouteStore()
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := store.Upsert(context.Background(), exact.Route{
			MSISDN: fmt.Sprintf("22507000000%02d", i),
			Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()}, Source: exact.SourceManual,
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store})

	seen := map[string]bool{}
	cursor := ""
	for {
		url := "/v1/admin/exact-routes?limit=2"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodGet, url, ""))
		if w.Code != http.StatusOK {
			t.Fatalf("list status = %d; body=%s", w.Code, w.Body)
		}
		var page struct {
			Data       []map[string]any `json:"data"`
			NextCursor *string          `json:"next_cursor"`
			HasMore    bool             `json:"has_more"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, r := range page.Data {
			m, _ := r["msisdn"].(string)
			if seen[m] {
				t.Errorf("msisdn %q returned twice across pages", m)
			}
			seen[m] = true
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == nil {
			t.Fatal("has_more true but next_cursor nil")
		}
		cursor = *page.NextCursor
	}
	if len(seen) != n {
		t.Errorf("paged %d rows, want %d", len(seen), n)
	}
}

// TestImportExactRoutesAcceptsBatchAndRowsAppear: a bulk import returns 202 with a queued job, and the
// rows are then visible via list (the sync runner ran the import inline before Go returned).
func TestImportExactRoutesAcceptsBatchAndRowsAppear(t *testing.T) {
	store := newFakeExactRouteStore()
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store, Imports: syncRunner{}})

	body := fmt.Sprintf(`{"source":"mnp_import","rows":[
		{"msisdn":"+2250700000001","target_type":"connector","target_id":%q},
		{"msisdn":"2250700000002","target_type":"connector","target_id":%q}]}`,
		uuid.NewString(), uuid.NewString())
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body)
	}
	var job map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job["status"] != "queued" || job["job_id"] == nil {
		t.Errorf("job = %v, want a queued job with a job_id", job)
	}
	if store.count() != 2 {
		t.Errorf("imported %d rows, want 2", store.count())
	}
}

// TestImportExactRoutesReplayNoDuplicates: re-importing the same batch (idempotent by msisdn) leaves
// the same row count — a replayed MNP feed does not duplicate.
func TestImportExactRoutesReplayNoDuplicates(t *testing.T) {
	store := newFakeExactRouteStore()
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store, Imports: syncRunner{}})
	body := fmt.Sprintf(`{"source":"carrier_feed","rows":[{"msisdn":"2250700000001","target_type":"route","target_id":%q}]}`, uuid.NewString())

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
		if w.Code != http.StatusAccepted {
			t.Fatalf("import %d status = %d, want 202; body=%s", i, w.Code, w.Body)
		}
	}
	if store.count() != 1 {
		t.Errorf("after replaying one row: %d rows, want 1 (idempotent)", store.count())
	}
}

// TestImportExactRoutesInvalidMSISDNIs422: a bad number fails the whole request synchronously (422),
// with nothing written — never a silently failed background job.
func TestImportExactRoutesInvalidMSISDNIs422(t *testing.T) {
	store := newFakeExactRouteStore()
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store, Imports: syncRunner{}})
	body := fmt.Sprintf(`{"rows":[{"msisdn":"nope","target_type":"connector","target_id":%q}]}`, uuid.NewString())
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	if store.count() != 0 {
		t.Errorf("wrote %d rows on a rejected batch, want 0", store.count())
	}
}

// TestImportExactRoutesRunsAsynchronously: with the REAL runner, the 202 is returned while the import
// is provably still in flight (the store's BulkUpsert is blocked), and the rows land after the drain —
// the endpoint does not hold the connection for the whole import.
func TestImportExactRoutesRunsAsynchronously(t *testing.T) {
	store := newFakeExactRouteStore()
	store.bulkStarted = make(chan struct{})
	store.bulkRelease = make(chan struct{})
	runner := async.New(1, nil)
	api := newTestAPIWith(t, adminapi.Deps{ExactRoutes: store, Imports: runner})

	body := fmt.Sprintf(`{"rows":[{"msisdn":"2250700000001","target_type":"connector","target_id":%q}]}`, uuid.NewString())
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body)
	}

	<-store.bulkStarted // the import is running, and the 202 has already been returned above
	if n := store.count(); n != 0 {
		t.Errorf("rows visible (%d) before the import finished, want 0 (async)", n)
	}
	close(store.bulkRelease)
	if err := runner.Close(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if store.count() != 1 {
		t.Errorf("after drain: %d rows, want 1", store.count())
	}
}
