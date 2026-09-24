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

type fakeRewriteStore struct {
	mu         sync.Mutex
	rows       map[uuid.UUID]cp.SenderRewriteRule
	lastFilter cp.SenderRewriteFilter
}

func newFakeRewriteStore() *fakeRewriteStore {
	return &fakeRewriteStore{rows: map[uuid.UUID]cp.SenderRewriteRule{}}
}

func (s *fakeRewriteStore) List(_ context.Context, f cp.SenderRewriteFilter) ([]cp.SenderRewriteRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFilter = f
	out := make([]cp.SenderRewriteRule, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	return out, nil
}

func (s *fakeRewriteStore) Get(_ context.Context, id uuid.UUID) (cp.SenderRewriteRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return cp.SenderRewriteRule{}, errs.ErrNotFound
	}
	return r, nil
}

func (s *fakeRewriteStore) Create(_ context.Context, in cp.NewSenderRewriteRule) (cp.SenderRewriteRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := cp.SenderRewriteRule{
		ID: uuid.New(), Scope: in.Scope, ScopeID: in.ScopeID, Direction: "mt",
		MatchSenderPattern: in.MatchSenderPattern, MatchDestPattern: in.MatchDestPattern,
		RewriteType: in.RewriteType, RewriteTo: in.RewriteTo, FallbackPool: in.FallbackPool,
		MaxLength: in.MaxLength, SanitizeCharset: in.SanitizeCharset, Priority: in.Priority,
		Reason: in.Reason, Status: "active",
	}
	s.rows[r.ID] = r
	return r, nil
}

func (s *fakeRewriteStore) Update(_ context.Context, id uuid.UUID, p cp.SenderRewriteRulePatch) (cp.SenderRewriteRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[id]
	if !ok {
		return cp.SenderRewriteRule{}, errs.ErrNotFound
	}
	if p.RewriteType != nil {
		r.RewriteType = *p.RewriteType
	}
	if p.MaxLength != nil {
		r.MaxLength = p.MaxLength
	}
	if p.Status != nil {
		r.Status = *p.Status
	}
	s.rows[id] = r
	return r, nil
}

func (s *fakeRewriteStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.rows, id)
	return nil
}

const rewritePath = "/v1/admin/sender-rewrite-rules"

func serveRewrite(t *testing.T, store *fakeRewriteStore, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	api := newTestAPIWith(t, adminapi.Deps{SenderRewriteRules: store})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, method, path, body))
	return w
}

func TestCreateSenderRewriteRuleAcceptsEachTypeWithWhatItNeeds(t *testing.T) {
	for name, body := range map[string]string{
		"static":        `{"scope":"platform","rewrite_type":"static","rewrite_to":"INFO"}`,
		"fallback_pool": `{"scope":"platform","rewrite_type":"fallback_pool","fallback_pool_json":["A","B"]}`,
		"truncate":      `{"scope":"platform","rewrite_type":"truncate","max_length":11}`,
		"sanitize":      `{"scope":"platform","rewrite_type":"sanitize"}`,
		"sanitize set":  `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":"ABC"}}`,
		"patterns":      `{"scope":"platform","rewrite_type":"static","rewrite_to":"INFO","match_sender_pattern":"[A-Z]{3,11}","match_dest_pattern":"225.*"}`,
		"scoped":        `{"scope":"connector","scope_id":"` + uuid.NewString() + `","rewrite_type":"truncate","max_length":11,"direction":"mt"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if w := serveRewrite(t, newFakeRewriteStore(), http.MethodPost, rewritePath, body); w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
			}
		})
	}
}

// Each case is a rule the PR2 engine could not evaluate: accepting it would store a rule that silently
// never applies.
func TestCreateSenderRewriteRuleRefusesWhatNoEngineEvaluates(t *testing.T) {
	for name, body := range map[string]string{
		"mo direction":           `{"scope":"platform","direction":"mo","rewrite_type":"static","rewrite_to":"INFO"}`,
		"static without target":  `{"scope":"platform","rewrite_type":"static"}`,
		"static blank target":    `{"scope":"platform","rewrite_type":"static","rewrite_to":"  "}`,
		"pool missing":           `{"scope":"platform","rewrite_type":"fallback_pool"}`,
		"pool empty":             `{"scope":"platform","rewrite_type":"fallback_pool","fallback_pool_json":[]}`,
		"pool blank entry":       `{"scope":"platform","rewrite_type":"fallback_pool","fallback_pool_json":["A",""]}`,
		"truncate without max":   `{"scope":"platform","rewrite_type":"truncate"}`,
		"charset unknown key":    `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allow":"ABC"}}`,
		"charset extra key":      `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":"AB","replace":"_"}}`,
		"charset empty":          `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":""}}`,
		"charset not a string":   `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":3}}`,
		"bad sender pattern":     `{"scope":"platform","rewrite_type":"truncate","max_length":11,"match_sender_pattern":"[A-Z"}`,
		"bad dest pattern":       `{"scope":"platform","rewrite_type":"truncate","max_length":11,"match_dest_pattern":"(225"}`,
		"platform with scope_id": `{"scope":"platform","scope_id":"` + uuid.NewString() + `","rewrite_type":"truncate","max_length":11}`,
		"customer without id":    `{"scope":"customer","rewrite_type":"truncate","max_length":11}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeRewriteStore()
			if w := serveRewrite(t, store, http.MethodPost, rewritePath, body); w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
			}
			if len(store.rows) != 0 {
				t.Fatal("a refused rule was stored")
			}
		})
	}
}

func TestCreateSenderRewriteRulePriorityDefaultsTo100AndKeepsAnExplicitZero(t *testing.T) {
	for body, want := range map[string]float64{
		`{"scope":"platform","rewrite_type":"sanitize"}`:              100,
		`{"scope":"platform","rewrite_type":"sanitize","priority":0}`: 0,
	} {
		w := serveRewrite(t, newFakeRewriteStore(), http.MethodPost, rewritePath, body)
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		if w.Code != http.StatusCreated || got["priority"] != want {
			t.Errorf("%s: status %d, priority %v; want 201 and %v", body, w.Code, got["priority"], want)
		}
	}
}

func seedRewrite(store *fakeRewriteStore, in cp.NewSenderRewriteRule) cp.SenderRewriteRule {
	r, _ := store.Create(context.Background(), in)
	return r
}

// The update is validated on the rule it produces, not on the fields it carries: switching a static
// rule to truncate without a max_length leaves a rule the engine cannot apply.
func TestUpdateSenderRewriteRuleValidatesTheMergedRule(t *testing.T) {
	store := newFakeRewriteStore()
	r := seedRewrite(store, cp.NewSenderRewriteRule{
		Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteStatic, RewriteTo: ptr("INFO"), Priority: 100,
	})
	path := rewritePath + "/" + r.ID.String()

	if w := serveRewrite(t, store, http.MethodPatch, path, `{"rewrite_type":"truncate"}`); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("truncate without max_length: status = %d, want 422; body=%s", w.Code, w.Body)
	}
	if store.rows[r.ID].RewriteType != cp.RewriteStatic {
		t.Fatal("a refused update was applied")
	}
	if w := serveRewrite(t, store, http.MethodPatch, path, `{"rewrite_type":"truncate","max_length":11}`); w.Code != http.StatusOK {
		t.Fatalf("truncate with max_length: status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if w := serveRewrite(t, store, http.MethodPatch, path, `{"match_sender_pattern":"[bad"}`); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad pattern: status = %d, want 422; body=%s", w.Code, w.Body)
	}
	if w := serveRewrite(t, store, http.MethodPatch, path, `{"status":"disabled"}`); w.Code != http.StatusOK {
		t.Fatalf("status only: status = %d, want 200; body=%s", w.Code, w.Body)
	}
}

func TestSenderRewriteRuleUnknownIDIs404(t *testing.T) {
	path := rewritePath + "/" + uuid.NewString()
	if w := serveRewrite(t, newFakeRewriteStore(), http.MethodPatch, path, `{"status":"disabled"}`); w.Code != http.StatusNotFound {
		t.Errorf("update: status = %d, want 404; body=%s", w.Code, w.Body)
	}
	if w := serveRewrite(t, newFakeRewriteStore(), http.MethodDelete, path, ""); w.Code != http.StatusNotFound {
		t.Errorf("delete: status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

func TestDeleteSenderRewriteRule(t *testing.T) {
	store := newFakeRewriteStore()
	r := seedRewrite(store, cp.NewSenderRewriteRule{Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteSanitize})
	if w := serveRewrite(t, store, http.MethodDelete, rewritePath+"/"+r.ID.String(), ""); w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if len(store.rows) != 0 {
		t.Fatal("rule still stored")
	}
}

func TestListSenderRewriteRulesPassesTheFilter(t *testing.T) {
	store := newFakeRewriteStore()
	id := uuid.New()
	w := serveRewrite(t, store, http.MethodGet, rewritePath+"?scope=customer&scopeId="+id.String(), "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	f := store.lastFilter
	if f.Scope == nil || *f.Scope != cp.RewriteScopeCustomer || f.ScopeID == nil || *f.ScopeID != id {
		t.Fatalf("filter = %+v; want scope=customer scopeId=%s", f, id)
	}
}
