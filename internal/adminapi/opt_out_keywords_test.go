package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type fakeOptOutKeywordStore struct {
	mu   sync.Mutex
	rows map[uuid.UUID]cp.OptOutKeyword
}

func newFakeOptOutKeywordStore() *fakeOptOutKeywordStore {
	return &fakeOptOutKeywordStore{rows: map[uuid.UUID]cp.OptOutKeyword{}}
}

func (s *fakeOptOutKeywordStore) List(_ context.Context) ([]cp.OptOutKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.OptOutKeyword, 0, len(s.rows))
	for _, k := range s.rows {
		out = append(out, k)
	}
	return out, nil
}

func (s *fakeOptOutKeywordStore) Create(_ context.Context, in cp.NewOptOutKeyword) (cp.OptOutKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mt := cp.OptOutMatchExact
	if in.MatchType != nil {
		mt = *in.MatchType
	}
	k := cp.OptOutKeyword{
		ID: uuid.New(), CountryCode: in.CountryCode, Keyword: in.Keyword, Action: in.Action,
		MatchType: mt, AutoReplyTemplate: in.AutoReplyTemplate, Status: cp.OptOutKeywordActive,
	}
	s.rows[k.ID] = k
	return k, nil
}

func (s *fakeOptOutKeywordStore) Update(_ context.Context, id uuid.UUID, p cp.OptOutKeywordPatch) (cp.OptOutKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.rows[id]
	if !ok {
		return cp.OptOutKeyword{}, errs.ErrNotFound
	}
	if p.Status != nil {
		k.Status = *p.Status
	}
	if p.Keyword != nil {
		k.Keyword = *p.Keyword
	}
	s.rows[id] = k
	return k, nil
}

func (s *fakeOptOutKeywordStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.rows, id)
	return nil
}

func seedOptOutKeyword(t *testing.T, api http.Handler) string {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/opt-out-keywords",
		`{"keyword":"STOP","action":"suppress","auto_reply_template":"bye"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return got["id"].(string)
}

func TestCreateOptOutKeywordDefaultsMatchType(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{OptOutKeywords: newFakeOptOutKeywordStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/opt-out-keywords",
		`{"keyword":"STOP","action":"suppress"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["match_type"] != "exact" {
		t.Errorf("match_type = %v, want the default exact", got["match_type"])
	}
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
}

func TestUpdateOptOutKeywordDisables(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{OptOutKeywords: newFakeOptOutKeywordStore()})
	id := seedOptOutKeyword(t, api)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/opt-out-keywords/"+id, `{"status":"disabled"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "disabled" {
		t.Errorf("status = %v, want disabled", got["status"])
	}
}

func TestUpdateOptOutKeywordUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{OptOutKeywords: newFakeOptOutKeywordStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/opt-out-keywords/"+uuid.New().String(), `{"status":"disabled"}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

func TestDeleteOptOutKeyword(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{OptOutKeywords: newFakeOptOutKeywordStore()})
	id := seedOptOutKeyword(t, api)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/opt-out-keywords/"+id, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}
	// Deleting again is a 404.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/opt-out-keywords/"+id, ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want 404", w.Code)
	}
}

func TestListOptOutKeywords(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{OptOutKeywords: newFakeOptOutKeywordStore()})
	seedOptOutKeyword(t, api)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/opt-out-keywords", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got) != 1 || got[0]["keyword"] != "STOP" {
		t.Errorf("list = %v, want one STOP keyword", got)
	}
}
