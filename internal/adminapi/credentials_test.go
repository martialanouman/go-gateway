package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// createCredForRotation issues an api_key on a fresh account and returns the store, the mounted API and
// the credential id, so a rotation test starts from a credential that actually exists.
func createCredForRotation(t *testing.T) (*fakeCredentialStore, http.Handler, string, string) {
	t.Helper()
	store := newFakeCredentialStore()
	accountID := uuid.New()
	api := credAPI(t, store, newFakeAccountStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/smpp-accounts/"+accountID.String()+"/credentials", `{"type":"api_key"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed create: status = %d, body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	credID, _ := got["id"].(string)
	return store, api, accountID.String(), credID
}

func rotatePath(accountID, credID string) string {
	return "/v1/admin/smpp-accounts/" + accountID + "/credentials/" + credID + "/rotate"
}

// TestRotateTranslatesGracePeriodSecondsToADuration pins the one conversion nothing else observes:
// grace_period_sec is seconds on the wire and a time.Duration in the domain. Dropping the
// `* time.Second` would turn a 600-second window into 600 nanoseconds — every rotation becomes an
// immediate cutover that severs live ESMEs, which is exactly what the grace window exists to prevent.
func TestRotateTranslatesGracePeriodSecondsToADuration(t *testing.T) {
	tests := []struct {
		name string
		body string
		want *time.Duration
	}{
		{"seconds become the matching duration", `{"grace_period_sec":600}`, ptr(10 * time.Minute)},
		{"a zero window is a zero duration, not absent", `{"grace_period_sec":0}`, ptr(time.Duration(0))},
		{"an omitted field is an immediate cutover", `{}`, nil},
		{"an absent body is an immediate cutover", ``, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, api, accountID, credID := createCredForRotation(t)

			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, http.MethodPost, rotatePath(accountID, credID), tc.body))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
			}

			got := store.rotation(t).Grace
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("Grace = %v, want nil (immediate cutover)", *got)
			case tc.want != nil && got == nil:
				t.Errorf("Grace = nil, want %v", *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("Grace = %v, want %v", *got, *tc.want)
			}
		})
	}
}

// TestRotateRejectsAnUnboundedGraceWindow: the window is capped, so a typo when rotating a leaked
// secret (86400000 for 86400) cannot leave the compromised secret binding for years.
func TestRotateRejectsAnUnboundedGraceWindow(t *testing.T) {
	store, api, accountID, credID := createCredForRotation(t)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, rotatePath(accountID, credID), `{"grace_period_sec":86400000}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	if store.lastRotation != nil {
		t.Error("a rejected rotation still reached the store")
	}
}

func ptr[T any](v T) *T { return &v }
