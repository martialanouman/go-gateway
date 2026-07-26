package adminapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// fakeSuppressionStore is an in-memory SuppressionAdminStore for handler unit tests.
type fakeSuppressionStore struct {
	mu        sync.Mutex
	rows      map[uuid.UUID]cp.Suppression
	keys      map[string]bool // scope|scopeID|msisdn — drives the 409 on a duplicate create
	createErr error
}

func newFakeSuppressionStore() *fakeSuppressionStore {
	return &fakeSuppressionStore{rows: map[uuid.UUID]cp.Suppression{}, keys: map[string]bool{}}
}

func supKey(scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) string {
	id := ""
	if scopeID != nil {
		id = scopeID.String()
	}
	return string(scope) + "|" + id + "|" + msisdn
}

func (s *fakeSuppressionStore) CreateReturning(_ context.Context, in cp.NewSuppression) (cp.Suppression, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.Suppression{}, s.createErr
	}
	k := supKey(in.Scope, in.ScopeID, in.MSISDN)
	if s.keys[k] {
		return cp.Suppression{}, errs.ErrConflict
	}
	row := cp.Suppression{ID: uuid.New(), Scope: in.Scope, ScopeID: in.ScopeID, MSISDN: in.MSISDN, Source: in.Source, Reason: in.Reason}
	s.rows[row.ID] = row
	s.keys[k] = true
	return row, nil
}

func (s *fakeSuppressionStore) ListPage(_ context.Context, _ cp.SuppressionFilter) (cp.Page[cp.Suppression], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]cp.Suppression, 0, len(s.rows))
	for _, r := range s.rows {
		items = append(items, r)
	}
	return cp.Page[cp.Suppression]{Items: items}, nil
}

func (s *fakeSuppressionStore) DeleteByID(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.rows, id)
	return nil
}

func (s *fakeSuppressionStore) Import(_ context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, source cp.SuppressionSource, msisdns []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var inserted int64
	for _, m := range msisdns {
		k := supKey(scope, scopeID, m)
		if s.keys[k] {
			continue
		}
		s.keys[k] = true
		row := cp.Suppression{ID: uuid.New(), Scope: scope, ScopeID: scopeID, MSISDN: m, Source: source}
		s.rows[row.ID] = row
		inserted++
	}
	return inserted, nil
}

func (s *fakeSuppressionStore) IsSuppressed(_ context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys[supKey(scope, scopeID, msisdn)], nil
}

func (s *fakeSuppressionStore) put(scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[supKey(scope, scopeID, msisdn)] = true
}

func TestCreateSuppressionNormalizesAndReturns201(t *testing.T) {
	store := newFakeSuppressionStore()
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: store})

	w := httptest.NewRecorder()
	// A "+"-prefixed number must be canonicalized to digits-only before the store sees it.
	body := `{"scope":"platform","msisdn":"+2250700000001","source":"admin"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions", body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["msisdn"] != "2250700000001" {
		t.Errorf("msisdn = %v, want the normalized 2250700000001", got["msisdn"])
	}
}

func TestCreateSuppressionInvalidMSISDNIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions",
		`{"scope":"platform","msisdn":"not-a-number","source":"admin"}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

func TestCreateSuppressionDuplicateIs409(t *testing.T) {
	store := newFakeSuppressionStore()
	store.put(cp.SuppressionScopePlatform, nil, "2250700000001")
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions",
		`{"scope":"platform","msisdn":"2250700000001","source":"admin"}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
}

func TestDeleteSuppressionUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/suppressions/"+uuid.New().String(), ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

func TestImportSuppressionsReturns202CompletedJob(t *testing.T) {
	store := newFakeSuppressionStore()
	store.put(cp.SuppressionScopePlatform, nil, "2250700000001") // pre-existing → skipped as duplicate
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: store})

	w := httptest.NewRecorder()
	body := `{"scope":"platform","source":"import","msisdns":["+2250700000001","2250700000002"]}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions/import", body))

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body)
	}
	var job map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job["status"] != "completed" {
		t.Errorf("status = %v, want completed", job["status"])
	}
	if job["job_id"] == nil || job["created_at"] == nil {
		t.Errorf("job must carry job_id and created_at: %s", w.Body)
	}
}

func TestImportSuppressionsInvalidMSISDNIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions/import",
		`{"scope":"platform","msisdns":["2250700000001","garbage"]}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

func TestImportSuppressionsOverLimitIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})

	var sb strings.Builder
	sb.WriteString(`{"scope":"platform","msisdns":[`)
	for i := 0; i < 10001; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`"225070000` + fmt.Sprintf("%04d", i) + `"`)
	}
	sb.WriteString(`]}`)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions/import", sb.String()))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for an over-limit batch; body=%s", w.Code, w.Body)
	}
}

func TestListSuppressionsInvalidMSISDNFilterIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/suppressions?msisdn=not-a-number", ""))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for a non-canonical msisdn filter; body=%s", w.Code, w.Body)
	}
}

// TestCheckSuppressionReportsMatchedScopes: a msisdn suppressed platform-wide is reported blocked with
// the platform scope, exact against the store (not the router's Bloom).
func TestCheckSuppressionReportsMatchedScopes(t *testing.T) {
	store := newFakeSuppressionStore()
	store.put(cp.SuppressionScopePlatform, nil, "2250700000001")
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions/check",
		`{"msisdn":"2250700000001"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var res struct {
		Blocked       bool `json:"blocked"`
		MatchedScopes []struct {
			Scope string `json:"scope"`
		} `json:"matched_scopes"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if !res.Blocked || len(res.MatchedScopes) != 1 || res.MatchedScopes[0].Scope != "platform" {
		t.Errorf("check = %+v, want blocked with the platform scope", res)
	}
}

func TestCheckSuppressionNotBlocked(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Suppressions: newFakeSuppressionStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/suppressions/check", `{"msisdn":"2250700000009"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var res struct {
		Blocked bool `json:"blocked"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Blocked {
		t.Error("an unlisted number must not be blocked")
	}
}
