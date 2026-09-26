package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// fakePlatformPolicy holds the platform default. The table's CHECK is proven against Postgres
// (platform_content_policy_integration_test.go); the handler refuses the other values before calling it.
type fakePlatformPolicy struct {
	mu sync.Mutex
	cs cp.ContentStorage
}

func (f *fakePlatformPolicy) PlatformContentStorage(context.Context) (cp.ContentStorage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cs == "" {
		return cp.ContentOff, nil
	}
	return f.cs, nil
}

func (f *fakePlatformPolicy) SetPlatformContentStorage(_ context.Context, cs cp.ContentStorage) (cp.ContentStorage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cs = cs
	return cs, nil
}

type contentPolicyBody struct {
	ContentStorage       string `json:"content_storage"`
	ContentRetentionDays *int   `json:"content_retention_days"`
}

func contentPolicyCall(t *testing.T, api http.Handler, method, path, body string) (int, contentPolicyBody) {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, method, path, body))
	var got contentPolicyBody
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

func TestPlatformContentPolicyStartsOffWithTheCDRRetention(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{PlatformContentPolicy: &fakePlatformPolicy{}})
	code, got := contentPolicyCall(t, api, http.MethodGet, "/v1/admin/platform/content-policy", "")
	if code != http.StatusOK || got.ContentStorage != "off" || got.ContentRetentionDays == nil || *got.ContentRetentionDays != 30 {
		t.Fatalf("status=%d body=%+v, want 200 off/30", code, got)
	}
}

func TestPlatformContentPolicyCanBecomeEncrypted(t *testing.T) {
	store := &fakePlatformPolicy{}
	api := newTestAPIWith(t, adminapi.Deps{PlatformContentPolicy: store})
	code, got := contentPolicyCall(t, api, http.MethodPatch, "/v1/admin/platform/content-policy", `{"content_storage":"stored_encrypted"}`)
	if code != http.StatusOK || got.ContentStorage != "stored_encrypted" {
		t.Fatalf("status=%d body=%+v, want 200 stored_encrypted", code, got)
	}
	if cs, _ := store.PlatformContentStorage(context.Background()); cs != cp.ContentStoredEncrypted {
		t.Fatalf("stored platform default = %q, want stored_encrypted", cs)
	}
}

func TestPlatformContentPolicyRefusesClearAndInherit(t *testing.T) {
	store := &fakePlatformPolicy{}
	api := newTestAPIWith(t, adminapi.Deps{PlatformContentPolicy: store})
	for _, cs := range []string{"stored_plaintext", "inherit"} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/platform/content-policy", `{"content_storage":"`+cs+`"}`))
		if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"field":"content_storage"`) {
			t.Errorf("PATCH %s: status=%d body=%s, want 422 naming content_storage", cs, w.Code, w.Body)
		}
	}
	if cs, _ := store.PlatformContentStorage(context.Background()); cs != cp.ContentOff {
		t.Fatalf("platform default = %q after refused PATCHes, want off", cs)
	}
}

func TestPlatformContentRetentionIsReadOnly(t *testing.T) {
	store := &fakePlatformPolicy{}
	api := newTestAPIWith(t, adminapi.Deps{PlatformContentPolicy: store})
	code, _ := contentPolicyCall(t, api, http.MethodPatch, "/v1/admin/platform/content-policy",
		`{"content_storage":"stored_encrypted","content_retention_days":7}`)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("PATCH retention 7: status=%d, want 422 — it is a ClickHouse TTL, not a setting", code)
	}
	if cs, _ := store.PlatformContentStorage(context.Background()); cs != cp.ContentOff {
		t.Fatalf("a refused PATCH changed the storage to %q", cs)
	}
	// What GET returned must PATCH back unchanged.
	code, _ = contentPolicyCall(t, api, http.MethodPatch, "/v1/admin/platform/content-policy",
		`{"content_storage":"off","content_retention_days":30}`)
	if code != http.StatusOK {
		t.Fatalf("PATCH of the current values: status=%d, want 200", code)
	}
}

// TestPlatformContentRetentionMatchesTheCDRColumnTTL: the value served is the one ClickHouse applies, to the
// body and to the key reference that expires with it.
func TestPlatformContentRetentionMatchesTheCDRColumnTTL(t *testing.T) {
	ddl, err := os.ReadFile("../../migrations/clickhouse/0003_cdr_content_ttl.up.sql")
	if err != nil {
		t.Fatalf("read DDL: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{PlatformContentPolicy: &fakePlatformPolicy{}})
	_, got := contentPolicyCall(t, api, http.MethodGet, "/v1/admin/platform/content-policy", "")
	if got.ContentRetentionDays == nil {
		t.Fatal("no content_retention_days served")
	}
	for _, column := range []string{"content_ciphertext", "content_key_id"} {
		ttl := regexp.MustCompile(`\b` + column + `\s+Nullable\(\w+\)\s+TTL\s+toDate\(submitted_at\)\s*\+\s*INTERVAL\s+(\d+)\s+DAY`)
		m := ttl.FindSubmatch(ddl)
		if m == nil || string(m[1]) != strconv.Itoa(*got.ContentRetentionDays) {
			t.Errorf("%s TTL in the DDL = %q, served retention = %d days; want them equal", column, m, *got.ContentRetentionDays)
		}
	}
}

func TestCustomerContentPolicyReadsAndWritesTheCustomersOwnSetting(t *testing.T) {
	customers := newFakeCustomerStore()
	c, err := customers.Create(context.Background(), cp.NewCustomer{Name: "acme"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{Customers: customers})
	path := "/v1/admin/customers/" + c.ID.String() + "/content-policy"

	code, got := contentPolicyCall(t, api, http.MethodGet, path, "")
	if code != http.StatusOK || got.ContentStorage != "inherit" {
		t.Fatalf("GET: status=%d body=%+v, want 200 inherit (the raw setting, not the resolved one)", code, got)
	}

	code, got = contentPolicyCall(t, api, http.MethodPatch, path, `{"content_storage":"stored_encrypted","content_retention_days":7}`)
	if code != http.StatusOK || got.ContentStorage != "stored_encrypted" || got.ContentRetentionDays == nil || *got.ContentRetentionDays != 7 {
		t.Fatalf("PATCH: status=%d body=%+v, want 200 stored_encrypted/7", code, got)
	}
	stored, _ := customers.Get(context.Background(), c.ID)
	if stored.ContentStorage != cp.ContentStoredEncrypted || stored.ContentRetentionDays == nil || *stored.ContentRetentionDays != 7 {
		t.Fatalf("stored customer = %q/%v, want stored_encrypted/7", stored.ContentStorage, stored.ContentRetentionDays)
	}
}

func TestCustomerContentPolicyOfAnUnknownCustomerIsNotFound(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Customers: newFakeCustomerStore()})
	path := "/v1/admin/customers/" + uuid.NewString() + "/content-policy"
	for _, m := range []string{http.MethodGet, http.MethodPatch} {
		if code, _ := contentPolicyCall(t, api, m, path, `{"content_storage":"off"}`); code != http.StatusNotFound {
			t.Errorf("%s unknown customer: status=%d, want 404", m, code)
		}
	}
}
