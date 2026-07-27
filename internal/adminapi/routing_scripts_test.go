package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// fakeRoutingScriptStore is an in-memory RoutingScriptAdminStore for handler unit tests.
type fakeRoutingScriptStore struct {
	mu   sync.Mutex
	rows map[uuid.UUID]script.Script
	seq  int
}

func newFakeRoutingScriptStore() *fakeRoutingScriptStore {
	return &fakeRoutingScriptStore{rows: map[uuid.UUID]script.Script{}}
}

func (s *fakeRoutingScriptStore) Create(_ context.Context, sc script.Script) (script.Script, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	sc.ID = uuid.New()
	sc.Status = script.StatusDraft
	sc.CreatedAt = time.Unix(int64(1_700_000_000+s.seq), 0).UTC()
	s.rows[sc.ID] = sc
	return sc, nil
}

func (s *fakeRoutingScriptStore) Get(_ context.Context, id uuid.UUID) (script.Script, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.rows[id]
	return sc, ok, nil
}

func (s *fakeRoutingScriptStore) Update(_ context.Context, id uuid.UUID, sc script.Script) (script.Script, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return script.Script{}, false, nil
	}
	sc.ID = id
	s.rows[id] = sc
	return sc, true, nil
}

func (s *fakeRoutingScriptStore) Delete(_ context.Context, id uuid.UUID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rows[id]; !ok {
		return false, nil
	}
	delete(s.rows, id)
	return true, nil
}

func (s *fakeRoutingScriptStore) ListVersions(_ context.Context, scope script.Scope, scopeID *uuid.UUID) ([]script.Script, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []script.Script
	for _, sc := range s.rows {
		if sc.Scope != scope {
			continue
		}
		if (sc.ScopeID == nil) != (scopeID == nil) {
			continue
		}
		if scopeID != nil && *sc.ScopeID != *scopeID {
			continue
		}
		out = append(out, sc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *fakeRoutingScriptStore) List(_ context.Context, after uuid.UUID, limit int) ([]script.Script, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]script.Script, 0, len(s.rows))
	for _, sc := range s.rows {
		all = append(all, sc)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID.String() < all[j].ID.String() })
	out := make([]script.Script, 0, limit)
	for _, sc := range all {
		if sc.ID.String() > after.String() || after == uuid.Nil {
			out = append(out, sc)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (s *fakeRoutingScriptStore) Assign(_ context.Context, id uuid.UUID, scope script.Scope, scopeID *uuid.UUID) (script.Script, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.rows[id]
	if !ok {
		return script.Script{}, false, nil
	}
	sc.Scope, sc.ScopeID = scope, scopeID
	s.rows[id] = sc
	return sc, true, nil
}

func (s *fakeRoutingScriptStore) Publish(_ context.Context, id uuid.UUID) (script.Script, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, ok := s.rows[id]
	if !ok {
		return script.Script{}, false, nil
	}
	for other, o := range s.rows {
		if o.Scope == sc.Scope && ((o.ScopeID == nil) == (sc.ScopeID == nil)) && o.Status == script.StatusActive {
			o.Status = script.StatusDisabled
			s.rows[other] = o
		}
	}
	now := time.Now().UTC()
	sc.Status, sc.PublishedAt = script.StatusActive, &now
	s.rows[id] = sc
	return sc, true, nil
}

// TestRoutingScriptCRUDRoundTrip: create a draft → get → update (recomputes checksum) → list versions →
// delete.
func TestRoutingScriptCRUDRoundTrip(t *testing.T) {
	store := newFakeRoutingScriptStore()
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	w := httptest.NewRecorder()
	body := `{"scope":"platform","name":"night","language":"js","source_code":"function resolveRoute(m){return null}"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	id, _ := created["id"].(string)
	if created["status"] != "draft" || created["checksum"] == "" {
		t.Errorf("created = %v, want draft with a checksum", created)
	}
	oldChecksum := created["checksum"]

	// Get.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/routing-scripts/"+id, ""))
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", w.Code)
	}

	// Update the source: checksum must change.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routing-scripts/"+id, `{"source_code":"function resolveRoute(m){return \"x\"}"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var updated map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &updated)
	if updated["checksum"] == oldChecksum {
		t.Error("checksum did not change after a source update")
	}

	// Versions.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/routing-scripts/"+id+"/versions", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("versions status = %d, want 200", w.Code)
	}
	var versions []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &versions)
	if len(versions) != 1 {
		t.Errorf("versions = %d, want 1", len(versions))
	}

	// Delete, then get is 404.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/routing-scripts/"+id, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", w.Code)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/routing-scripts/"+id, ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("get after delete = %d, want 404", w.Code)
	}
}

// TestCreateRoutingScriptScopeValidation: platform with a scope_id, and a non-platform without one,
// are both 422.
func TestCreateRoutingScriptScopeValidation(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: newFakeRoutingScriptStore()})

	cases := []string{
		`{"scope":"platform","scope_id":"` + uuid.NewString() + `","name":"n","language":"js","source_code":"x"}`,
		`{"scope":"customer","name":"n","language":"js","source_code":"x"}`,
	}
	for _, body := range cases {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts", body))
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("scope-mismatch create status = %d, want 422; body=%s", w.Code, w.Body)
		}
	}
}

// TestListRoutingScriptsFiltersByScope: the list filters by the scope query.
func TestListRoutingScriptsFiltersByScope(t *testing.T) {
	store := newFakeRoutingScriptStore()
	cust := uuid.New()
	_, _ = store.Create(context.Background(), script.Script{Scope: script.ScopePlatform, Name: "p", Language: script.LanguageJS, Source: "x"})
	_, _ = store.Create(context.Background(), script.Script{Scope: script.ScopeCustomer, ScopeID: &cust, Name: "c", Language: script.LanguageJS, Source: "x"})
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/routing-scripts?scope=customer", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", w.Code)
	}
	var got []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got) != 1 || got[0]["scope"] != "customer" {
		t.Errorf("scope=customer list = %v, want just the customer script", got)
	}
}

// TestUpdateActiveRoutingScriptIs409: an active (published) script is immutable — editing it in place
// is rejected so a change always goes through create-draft → publish.
func TestUpdateActiveRoutingScriptIs409(t *testing.T) {
	store := newFakeRoutingScriptStore()
	created, _ := store.Create(context.Background(), script.Script{Scope: script.ScopePlatform, Name: "p", Language: script.LanguageJS, Source: "x"})
	if _, _, err := store.Publish(context.Background(), created.ID); err != nil {
		t.Fatalf("publish: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routing-scripts/"+created.ID.String(), `{"name":"renamed"}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("update of an active script = %d, want 409; body=%s", w.Code, w.Body)
	}
}

// TestUpdateRoutingScriptPreservesImmutableFields: a PATCH keeps scope/language/status/created_at.
func TestUpdateRoutingScriptPreservesImmutableFields(t *testing.T) {
	store := newFakeRoutingScriptStore()
	cust := uuid.New()
	created, _ := store.Create(context.Background(), script.Script{
		Scope: script.ScopeCustomer, ScopeID: &cust, Name: "c", Language: script.LanguageLua, Source: "old",
	})
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routing-scripts/"+created.ID.String(), `{"name":"renamed"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["scope"] != "customer" || got["language"] != "lua" || got["status"] != "draft" {
		t.Errorf("update mutated an immutable field: %v", got)
	}
	if got["name"] != "renamed" {
		t.Errorf("name = %v, want renamed", got["name"])
	}
}

// TestGetRoutingScriptUnknownIs404: an unknown id is a 404.
func TestGetRoutingScriptUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: newFakeRoutingScriptStore()})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/routing-scripts/"+uuid.NewString(), ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// seedDraft creates a draft script directly in the store and returns its id.
func seedDraft(t *testing.T, store *fakeRoutingScriptStore, s script.Script) uuid.UUID {
	t.Helper()
	s.Checksum = script.Checksum(s.Source)
	if s.TimeoutMs == 0 {
		s.TimeoutMs = 5
	}
	saved, err := store.Create(context.Background(), s)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return saved.ID
}

// TestAssignRoutingScript: a draft is reassigned to a new scope; an active script is not (409).
func TestAssignRoutingScript(t *testing.T) {
	store := newFakeRoutingScriptStore()
	id := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "p", Language: script.LanguageJS, Source: "function resolveRoute(m){return null}"})
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	cust := uuid.NewString()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routing-scripts/"+id.String()+"/assign", `{"scope":"customer","scope_id":"`+cust+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("assign status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["scope"] != "customer" || got["scope_id"] != cust {
		t.Errorf("assigned = %v, want customer/%s", got, cust)
	}

	// Assigning an active script is rejected.
	if _, _, err := store.Publish(context.Background(), id); err != nil {
		t.Fatalf("publish: %v", err)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routing-scripts/"+id.String()+"/assign", `{"scope":"platform"}`))
	if w.Code != http.StatusConflict {
		t.Errorf("assign of active = %d, want 409", w.Code)
	}
}

// TestValidateRoutingScript: a compilable script is valid with a checksum; a broken one is invalid
// with errors (not an HTTP error).
func TestValidateRoutingScript(t *testing.T) {
	store := newFakeRoutingScriptStore()
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})

	good := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "g", Language: script.LanguageJS, Source: "function resolveRoute(m){return null}"})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+good.String()+"/validate", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("validate status = %d, want 200", w.Code)
	}
	var okRes map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &okRes)
	if okRes["valid"] != true || okRes["checksum"] == nil {
		t.Errorf("valid script result = %v, want valid with a checksum", okRes)
	}

	bad := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "b", Language: script.LanguageJS, Source: "function resolveRoute( { syntax"})
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+bad.String()+"/validate", ""))
	var badRes map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &badRes)
	if badRes["valid"] != false {
		t.Errorf("broken script result = %v, want valid=false", badRes)
	}
}

// TestTestRoutingScript: a dry-run returns the resolved route id, null for a fallthrough, and flags a
// timeout for a runaway script.
func TestTestRoutingScript(t *testing.T) {
	store := newFakeRoutingScriptStore()
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})
	route := uuid.NewString()

	id := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "r", Language: script.LanguageJS,
		Source: `function resolveRoute(m){ return m.to === "2250700000001" ? "` + route + `" : null }`})
	req := `{"message":{"source_addr":"BANK","dest_addr":"2250700000001","content":"hi"}}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+id.String()+"/test", req))
	if w.Code != http.StatusOK {
		t.Fatalf("test status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var res map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res["route_id"] != route {
		t.Errorf("test route_id = %v, want %s", res["route_id"], route)
	}
	if res["timed_out"] != false {
		t.Errorf("timed_out = %v, want false", res["timed_out"])
	}

	loop := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "l", Language: script.LanguageJS, Source: `function resolveRoute(m){ for(;;){} }`})
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+loop.String()+"/test", req))
	var loopRes map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &loopRes)
	if loopRes["timed_out"] != true {
		t.Errorf("runaway test timed_out = %v, want true", loopRes["timed_out"])
	}
}

// TestPublishRoutingScript: publish activates a draft.
func TestPublishRoutingScript(t *testing.T) {
	store := newFakeRoutingScriptStore()
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})
	id := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "p", Language: script.LanguageJS, Source: "function resolveRoute(m){return null}"})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+id.String()+"/publish", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "active" || got["published_at"] == nil {
		t.Errorf("published = %v, want active with a timestamp", got)
	}
}

// TestPublishUncompilableScriptIs422: publishing a draft that does not compile is rejected — an active
// script must always be runnable (the router would otherwise skip it and silently fall back).
func TestPublishUncompilableScriptIs422(t *testing.T) {
	store := newFakeRoutingScriptStore()
	api := newTestAPIWith(t, adminapi.Deps{RoutingScripts: store})
	id := seedDraft(t, store, script.Script{Scope: script.ScopePlatform, Name: "bad", Language: script.LanguageJS, Source: "function resolveRoute( { syntax"})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routing-scripts/"+id.String()+"/publish", ""))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("publish of an uncompilable script = %d, want 422; body=%s", w.Code, w.Body)
	}
}
