package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type fakeRewriteStore struct {
	mu         sync.Mutex
	rows       map[uuid.UUID]cp.SenderRewriteRule
	lastFilter cp.SenderRewriteFilter
	writes     int
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
	s.writes++
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
	s.writes++
	r, ok := s.rows[id]
	if !ok {
		return cp.SenderRewriteRule{}, errs.ErrNotFound
	}
	// The repository's COALESCE: every non-nil field overwrites.
	set := func(dst **string, v *string) {
		if v != nil {
			*dst = v
		}
	}
	set(&r.MatchSenderPattern, p.MatchSenderPattern)
	set(&r.MatchDestPattern, p.MatchDestPattern)
	set(&r.RewriteTo, p.RewriteTo)
	set(&r.Reason, p.Reason)
	if p.RewriteType != nil {
		r.RewriteType = *p.RewriteType
	}
	if p.FallbackPool != nil {
		r.FallbackPool = p.FallbackPool
	}
	if p.MaxLength != nil {
		r.MaxLength = p.MaxLength
	}
	if p.SanitizeCharset != nil {
		r.SanitizeCharset = p.SanitizeCharset
	}
	if p.Priority != nil {
		r.Priority = *p.Priority
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
	s.writes++
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
		"sanitize set":  `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":"ABC"},"reason":"carrier"}`,
		"patterns":      `{"scope":"platform","rewrite_type":"static","rewrite_to":"INFO","match_sender_pattern":"[A-Z]{3,11}","match_dest_pattern":"225.*"}`,
		"scoped":        `{"scope":"connector","scope_id":"` + uuid.NewString() + `","rewrite_type":"truncate","max_length":11,"direction":"mt"}`,
	} {
		t.Run(name, func(t *testing.T) {
			w := serveRewrite(t, newFakeRewriteStore(), http.MethodPost, rewritePath, body)
			if w.Code != http.StatusCreated {
				t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
			}
			assertJSONFields(t, w.Body.Bytes(), body)
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
		"pool blank entry":       `{"scope":"platform","rewrite_type":"fallback_pool","fallback_pool_json":["A","  "]}`,
		"truncate without max":   `{"scope":"platform","rewrite_type":"truncate"}`,
		"charset unknown key":    `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allow":"ABC"}}`,
		"charset extra key":      `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":"AB","replace":"_"}}`,
		"charset empty":          `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":""}}`,
		"charset not a string":   `{"scope":"platform","rewrite_type":"sanitize","sanitize_charset_json":{"allowed":3}}`,
		"bad sender pattern":     `{"scope":"platform","rewrite_type":"truncate","max_length":11,"match_sender_pattern":"[A-Z"}`,
		"bad dest pattern":       `{"scope":"platform","rewrite_type":"truncate","max_length":11,"match_dest_pattern":"(225"}`,
		"static over 20 octets":  `{"scope":"platform","rewrite_type":"static","rewrite_to":"ÉÉÉÉÉÉÉÉÉÉÉ"}`,
		"pool entry over 20":     `{"scope":"platform","rewrite_type":"fallback_pool","fallback_pool_json":["A","XXXXXXXXXXXXXXXXXXXXX"]}`,
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
	for name, body := range map[string]string{
		"blank static target": `{"rewrite_type":"static","rewrite_to":"  "}`,
		"bad charset":         `{"sanitize_charset_json":{}}`,
		"direction":           `{"direction":"mo"}`,
	} {
		if w := serveRewrite(t, store, http.MethodPatch, path, body); w.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d, want 422; body=%s", name, w.Code, w.Body)
		}
	}
}

// Every field of the update body reaches the store and comes back in the response.
func TestUpdateSenderRewriteRuleWritesEveryField(t *testing.T) {
	store := newFakeRewriteStore()
	r := seedRewrite(store, cp.NewSenderRewriteRule{Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteSanitize})
	body := `{"match_sender_pattern":"[A-Z]+","match_dest_pattern":"225.*","rewrite_type":"fallback_pool",` +
		`"rewrite_to":"INFO","fallback_pool_json":["A"],"max_length":9,"sanitize_charset_json":{"allowed":"A"},` +
		`"priority":3,"reason":"carrier","status":"disabled"}`
	w := serveRewrite(t, store, http.MethodPatch, rewritePath+"/"+r.ID.String(), body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	assertJSONFields(t, w.Body.Bytes(), body)
}

// assertJSONFields checks that every field of sent comes back unchanged in got.
func assertJSONFields(t *testing.T, got []byte, sent string) {
	t.Helper()
	var g, s map[string]any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("response: %v", err)
	}
	if err := json.Unmarshal([]byte(sent), &s); err != nil {
		t.Fatalf("sent body: %v", err)
	}
	for k, v := range s {
		if !reflect.DeepEqual(g[k], v) {
			t.Errorf("%s = %v, want %v", k, g[k], v)
		}
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

type testResult struct {
	Matched         bool    `json:"matched"`
	RewrittenSource *string `json:"rewritten_source"`
}

func testRewrite(t *testing.T, store *fakeRewriteStore, id uuid.UUID, body string) (int, testResult) {
	t.Helper()
	w := serveRewrite(t, store, http.MethodPost, rewritePath+"/"+id.String()+"/test", body)
	var got testResult
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

// The test runs the rule alone, disabled or not — it is how an operator checks one before enabling it —
// and it writes nothing.
func TestTestSenderRewriteRuleRunsTheRuleAloneAndWritesNothing(t *testing.T) {
	store := newFakeRewriteStore()
	r := seedRewrite(store, cp.NewSenderRewriteRule{
		Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteStatic, RewriteTo: ptr("INFO"), MatchDestPattern: ptr("225.*"),
	})
	r.Status = "disabled"
	store.rows[r.ID] = r
	store.writes = 0

	code, got := testRewrite(t, store, r.ID, `{"source_addr":"ACME","dest_addr":"2250700000001"}`)
	if code != http.StatusOK || !got.Matched || got.RewrittenSource == nil || *got.RewrittenSource != "INFO" {
		t.Errorf("matching sample: %d %+v, want 200 matched INFO", code, got)
	}
	code, got = testRewrite(t, store, r.ID, `{"source_addr":"ACME","dest_addr":"3312345"}`)
	if code != http.StatusOK || got.Matched || got.RewrittenSource != nil {
		t.Errorf("other sample: %d %+v, want 200 unmatched without a source", code, got)
	}
	if store.writes != 0 {
		t.Errorf("the test wrote %d times", store.writes)
	}
}

// A fallback_pool rule answers what the pool would send for that message id — the nil UUID when none
// is given.
func TestTestSenderRewriteRuleFollowsTheMessageID(t *testing.T) {
	store := newFakeRewriteStore()
	pool := []string{"A", "B", "C", "D", "E", "F", "G"}
	r := seedRewrite(store, cp.NewSenderRewriteRule{Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteFallbackPool, FallbackPool: pool})
	for _, id := range []uuid.UUID{uuid.Nil, uuid.New(), uuid.New(), uuid.New()} {
		body := `{"source_addr":"ACME","dest_addr":"225","message_id":"` + id.String() + `"}`
		if id == uuid.Nil {
			body = `{"source_addr":"ACME","dest_addr":"225"}`
		}
		want, _, _ := senderrewrite.EvalRule(store.rows[r.ID], "ACME", "225", id)
		if code, got := testRewrite(t, store, r.ID, body); code != http.StatusOK || got.RewrittenSource == nil || *got.RewrittenSource != want {
			t.Errorf("message %s: %d %+v, want %s", id, code, got, want)
		}
	}
}

func TestTestSenderRewriteRuleUnknownIDIs404(t *testing.T) {
	if code, _ := testRewrite(t, newFakeRewriteStore(), uuid.New(), `{"source_addr":"ACME","dest_addr":"225"}`); code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
}

// A stored pattern that no longer compiles (a hand-written row) is reported, not answered as a miss.
func TestTestSenderRewriteRuleReportsAnUncompilableStoredRule(t *testing.T) {
	store := newFakeRewriteStore()
	r := seedRewrite(store, cp.NewSenderRewriteRule{Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteSanitize, MatchSenderPattern: ptr("[A-Z")})
	if code, _ := testRewrite(t, store, r.ID, `{"source_addr":"ACME","dest_addr":"225"}`); code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", code)
	}
}
