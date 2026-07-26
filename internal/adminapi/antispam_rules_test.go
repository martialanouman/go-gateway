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

type fakeAntispamStore struct {
	mu   sync.Mutex
	rows map[uuid.UUID]cp.AntispamRule
}

func newFakeAntispamStore() *fakeAntispamStore {
	return &fakeAntispamStore{rows: map[uuid.UUID]cp.AntispamRule{}}
}

func (s *fakeAntispamStore) List(context.Context) ([]cp.AntispamRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.AntispamRule, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	return out, nil
}

func (s *fakeAntispamStore) Get(_ context.Context, id uuid.UUID) (cp.AntispamRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return cp.AntispamRule{}, errs.ErrNotFound
	}
	return r, nil
}

func (s *fakeAntispamStore) Create(_ context.Context, in cp.NewAntispamRule) (cp.AntispamRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := cp.AntispamRule{
		ID: uuid.New(), RuleType: in.RuleType, Scope: in.Scope, ScopeID: in.ScopeID,
		ConfigJSON: in.ConfigJSON, Action: in.Action, Status: cp.AntispamRuleActive,
	}
	s.rows[r.ID] = r
	return r, nil
}

func (s *fakeAntispamStore) Update(_ context.Context, id uuid.UUID, p cp.AntispamRulePatch) (cp.AntispamRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return cp.AntispamRule{}, errs.ErrNotFound
	}
	if p.Status != nil {
		r.Status = *p.Status
	}
	if p.Action != nil {
		r.Action = *p.Action
	}
	if p.ConfigJSON != nil {
		r.ConfigJSON = p.ConfigJSON
	}
	s.rows[id] = r
	return r, nil
}

func (s *fakeAntispamStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.rows, id)
	return nil
}

func TestCreateAntispamRuleValid(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"content_blacklist","scope":"global","action":"block","config_json":{"patterns":["(?i)spam"]}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "active" || got["rule_type"] != "content_blacklist" {
		t.Errorf("rule = %v, want active content_blacklist", got)
	}
}

func TestCreateAntispamRuleInvalidRegexIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"content_blacklist","scope":"global","action":"block","config_json":{"patterns":["[unclosed"]}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for an uncompilable regex; body=%s", w.Code, w.Body)
	}
}

func TestCreateAntispamRuleGlobalWithScopeIDIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"duplicate","scope":"global","scope_id":"` + uuid.New().String() + `","action":"flag","config_json":{"window_seconds":60}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (global scope must not carry a scope_id); body=%s", w.Code, w.Body)
	}
}

func TestCreateAntispamRuleAccountWithoutScopeIDIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"velocity","scope":"smpp_account","action":"throttle","config_json":{"max":100,"window_seconds":60}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (account scope requires a scope_id); body=%s", w.Code, w.Body)
	}
}

func TestCreateVelocityRuleZeroMaxIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"velocity","scope":"global","action":"throttle","config_json":{"max":0,"window_seconds":60}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (max must be positive); body=%s", w.Code, w.Body)
	}
}

func TestCreateReputationRuleWithoutMinScoreIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	// min_score is required — an empty config would make the rule a silent no-op.
	body := `{"rule_type":"reputation","scope":"global","action":"flag","config_json":{}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (reputation requires min_score); body=%s", w.Code, w.Body)
	}
}

// TestCreateReputationRuleNegativeScoreOK: reputation scores may be negative, so a negative min_score
// is accepted (only its absence is rejected).
func TestCreateReputationRuleNegativeScoreOK(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	body := `{"rule_type":"reputation","scope":"global","action":"block","config_json":{"min_score":-5}}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/antispam-rules", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (a negative min_score is valid); body=%s", w.Code, w.Body)
	}
}

func TestUpdateAntispamRuleUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/antispam-rules/"+uuid.New().String(), `{"status":"disabled"}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

func TestUpdateAntispamRuleInvalidConfigIs422(t *testing.T) {
	store := newFakeAntispamStore()
	// Seed a content rule, then try to patch it with an uncompilable regex.
	seed, _ := store.Create(context.Background(), cp.NewAntispamRule{RuleType: cp.AntispamContentBlacklist, Scope: cp.AntispamScopeGlobal, Action: cp.AntispamActionBlock, ConfigJSON: []byte(`{"patterns":["ok"]}`)})
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/antispam-rules/"+seed.ID.String(), `{"config_json":{"patterns":["[bad"]}}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (config validated against the rule's type on update); body=%s", w.Code, w.Body)
	}
}

func TestDeleteAntispamRuleUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{AntispamRules: newFakeAntispamStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/antispam-rules/"+uuid.New().String(), ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}
