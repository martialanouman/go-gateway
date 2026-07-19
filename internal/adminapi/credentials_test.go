package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/credential"
)

func credAPI(t *testing.T, creds adminapi.CredentialStore, accounts adminapi.AccountStore) http.Handler {
	t.Helper()
	return newTestAPIWith(t, adminapi.Deps{Credentials: creds, Accounts: accounts})
}

// TestCreateAPIKeyReturnsTheSecretOnceWithSgwPrefix: creating an api_key returns a
// CredentialWithSecret whose secret is a real sgw_ key, and the masked fields are present.
func TestCreateAPIKeyReturnsTheSecretOnceWithSgwPrefix(t *testing.T) {
	accountID := uuid.New()
	api := credAPI(t, newFakeCredentialStore(), newFakeAccountStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+accountID.String()+"/credentials", `{"type":"api_key"}`))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	secret, _ := got["secret"].(string)
	if !strings.HasPrefix(secret, credential.APIKeyPrefix) {
		t.Errorf("secret %q does not carry the sgw_ prefix", secret)
	}
	if got["type"] != "api_key" {
		t.Errorf("type = %v, want api_key", got["type"])
	}
}

// TestCreateSmppBindWithoutSystemIDIs422: system_id is required for a bind, and its absence is the
// amended 422 (not a 500 or a raw DB error).
func TestCreateSmppBindWithoutSystemIDIs422(t *testing.T) {
	accountID := uuid.New()
	api := credAPI(t, newFakeCredentialStore(), newFakeAccountStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+accountID.String()+"/credentials", `{"type":"smpp_bind"}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestCreateSmppBindWithTooLongSystemIDIs422: system_id is bounded by the SMPP bind field (15 chars,
// v3.4 §4.1.1); a longer value is a 422 at creation rather than a stored-but-unbindable credential.
func TestCreateSmppBindWithTooLongSystemIDIs422(t *testing.T) {
	accountID := uuid.New()
	api := credAPI(t, newFakeCredentialStore(), newFakeAccountStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+accountID.String()+"/credentials",
		`{"type":"smpp_bind","system_id":"this-system-id-is-way-too-long"}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestCreateSmppBindReturnsAnSMPPLegalPassword: a successful bind credential returns a one-time secret
// that fits the SMPP password field (<= 8 chars), so the issued password is actually bindable.
func TestCreateSmppBindReturnsAnSMPPLegalPassword(t *testing.T) {
	accountID := uuid.New()
	api := credAPI(t, newFakeCredentialStore(), newFakeAccountStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+accountID.String()+"/credentials",
		`{"type":"smpp_bind","system_id":"esme01"}`))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	secret, _ := got["secret"].(string)
	if len(secret) == 0 || len(secret) > 8 {
		t.Errorf("secret is %d chars, want 1..8 (SMPP password field limit)", len(secret))
	}
	if got["type"] != "smpp_bind" {
		t.Errorf("type = %v, want smpp_bind", got["type"])
	}
}

// TestSecondCredentialOfATypeIs409: the cardinality rule surfaces as a conflict.
func TestSecondCredentialOfATypeIs409(t *testing.T) {
	accountID := uuid.New()
	store := newFakeCredentialStore()
	api := credAPI(t, store, newFakeAccountStore())

	path := "/v1/admin/smpp-accounts/" + accountID.String() + "/credentials"
	first := httptest.NewRecorder()
	api.ServeHTTP(first, authed(t, http.MethodPost, path, `{"type":"api_key"}`))
	if first.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201; body=%s", first.Code, first.Body)
	}

	second := httptest.NewRecorder()
	api.ServeHTTP(second, authed(t, http.MethodPost, path, `{"type":"api_key"}`))
	if second.Code != http.StatusConflict {
		t.Fatalf("second create status = %d, want 409; body=%s", second.Code, second.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(second.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestListCredentialsNeverReturnsASecret is the invariant guard: a read must never expose a secret,
// even the "secret" key, whatever the stored data.
func TestListCredentialsNeverReturnsASecret(t *testing.T) {
	store := newFakeCredentialStore()
	accounts := newFakeAccountStore()
	// Seed the account so the existence check in list passes.
	seeded, _ := accounts.Create(t.Context(), newAccountInput())
	api := credAPI(t, store, accounts)

	// Create a key so there is something to list.
	create := httptest.NewRecorder()
	api.ServeHTTP(create, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+seeded.ID.String()+"/credentials", `{"type":"api_key"}`))

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/smpp-accounts/"+seeded.ID.String()+"/credentials", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "\"secret\"") {
		t.Errorf("list response carries a secret field: %s", w.Body)
	}
}
