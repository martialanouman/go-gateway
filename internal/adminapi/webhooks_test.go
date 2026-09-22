package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

const testSecret = "a-signing-secret-long-enough"

func sealedFor(plaintext string) cp.SealedSecret {
	return cp.SealedSecret{Sealed: []byte("sealed:" + plaintext), KMSKeyRef: "local/test-kek"}
}

// newWebhookAPI wires the Admin API with both stores the webhook surface needs: the account is read
// first to honour the 404 its path promises, then the webhooks themselves.
func newWebhookAPI(t *testing.T, hooks adminapi.WebhookStore, accounts adminapi.AccountStore) http.Handler {
	t.Helper()
	return newTestAPIWith(t, adminapi.Deps{Webhooks: hooks, Accounts: accounts})
}

// seedAccount puts one account in the store and returns its id, so a test can address the path.
func seedAccount(t *testing.T, accounts *fakeAccountStore) uuid.UUID {
	t.Helper()
	a, err := accounts.Create(t.Context(), cp.NewAccount{CustomerID: uuid.New(), Name: "hook-app"})
	if err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return a.ID
}

func webhookPath(accountID uuid.UUID, rest ...string) string {
	p := "/v1/admin/smpp-accounts/" + accountID.String() + "/webhooks"
	if len(rest) > 0 {
		p += "/" + strings.Join(rest, "/")
	}
	return p
}

// TestCreateWebhookNeverReturnsTheSecret is the step's central property, and it is asserted on the
// SERIALISED body rather than on the DTO: a field added to the struct without a json tag, or a store
// that echoes the whole row, leaks through the wire and not through the type.
func TestCreateWebhookNeverReturnsTheSecret(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	api := newWebhookAPI(t, newFakeWebhookStore(), accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"`+testSecret+`"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), testSecret) {
		t.Fatalf("the signing secret came back on the wire: %s", w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if _, present := got["secret"]; present {
		t.Errorf("response carries a secret key: %v", got)
	}
	if got["event_type"] != "mo" || got["url"] != "https://acme.test/mo" {
		t.Errorf("webhook = %v, want the created mo hook", got)
	}
	// account_id comes from the PATH, not from the body: a handler that let the caller pick it would
	// create a webhook on someone else's account without ever crossing the 404 guard.
	if got["account_id"] != id.String() {
		t.Errorf("account_id = %v, want the path's account %s", got["account_id"], id)
	}
	// created_at is omitempty so it stays out of the generated schema's required list, which is how the
	// DTO matches the contract without touching it. What omitempty must NOT do is drop it from the
	// response — and it does not, a struct never being "empty" to encoding/json. That is the whole of
	// what this asserts: the key is on the wire.
	if got["created_at"] == nil {
		t.Errorf("created_at is missing from the response: %v", got)
	}
}

// TestWebhookRetryPolicyRoundTrips: retry_policy_json is the one body field that is neither a string
// nor an enum, and it crosses two conversions — decoded object to stored jsonb and back. Without this
// the handlers could drop it entirely and every other test would stay green.
func TestWebhookRetryPolicyRoundTrips(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"`+testSecret+`",`+
			`"retry_policy_json":{"max_attempts":7}}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	policy, _ := created["retry_policy_json"].(map[string]any)
	if policy["max_attempts"] != float64(7) {
		t.Fatalf("retry_policy_json = %v, want max_attempts 7", created["retry_policy_json"])
	}

	hookID := uuid.MustParse(created["id"].(string))
	if stored := hooks.mustGet(t, hookID); string(stored.RetryPolicyJSON) != `{"max_attempts":7}` {
		t.Errorf("stored policy = %s, want the submitted object", stored.RetryPolicyJSON)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, webhookPath(id, hookID.String()),
		`{"retry_policy_json":{"max_attempts":2}}`))
	if w.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if stored := hooks.mustGet(t, hookID); string(stored.RetryPolicyJSON) != `{"max_attempts":2}` {
		t.Errorf("stored policy after patch = %s, want the patched object", stored.RetryPolicyJSON)
	}
}

// TestWebhookSecretAndURLAreRefusedWhenUseless: an empty secret makes every HMAC signature computable
// by anyone who knows the scheme, and "acme.test/mo" — no scheme — is an URL the sender cannot dial, so
// every MO and DLR of that account would burn its attempt budget and dead-letter. The secret carries a
// minimum length and the URL a pattern that wants a scheme AND a host, which is what took the contract
// to a major version. The pattern is case-sensitive on the scheme: `(?i)` is not ECMA-262, and the
// dashboard generates its validators from this contract.
func TestWebhookSecretAndURLAreRefusedWhenUseless(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: sealedFor(testSecret), Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	for _, tc := range []struct{ name, method, path, body string }{
		{"create empty secret", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"https://a.test/h","secret":""}`},
		{"create empty url", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"","secret":"` + testSecret + `"}`},
		{"create schemeless url", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"acme.test/mo","secret":"` + testSecret + `"}`},
		{"create hostless url", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"https://","secret":"` + testSecret + `"}`},
		{"update empty secret", http.MethodPatch, webhookPath(id, hookID.String()), `{"secret":""}`},
		{"update empty url", http.MethodPatch, webhookPath(id, hookID.String()), `{"url":""}`},
		{"update schemeless url", http.MethodPatch, webhookPath(id, hookID.String()), `{"url":"acme.test/mo"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, tc.method, tc.path, tc.body))
			if w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
			}
		})
	}
}

// TestCreateWebhookDuplicateBecomes409: webhooks_uq makes one webhook per (account, event_type), and
// 409 is the code the contract declares for a second one.
func TestCreateWebhookDuplicateBecomes409(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hooks.createErr = errs.ErrConflict
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"`+testSecret+`"}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
}

// TestWebhookOfAnotherAccountIs404 is the only thing standing between a webhookId and a cross-account
// write: the id is a UUID on a path whose account segment the caller also chooses, so every read and
// write has to be keyed by BOTH. A handler that trusted the id alone would let an operator scoped to
// one account rewrite another's delivery URL.
func TestWebhookOfAnotherAccountIs404(t *testing.T) {
	accounts := newFakeAccountStore()
	mine, theirs := seedAccount(t, accounts), seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: theirs, EventType: cp.WebhookEventMO,
		URL: "https://theirs.test/mo", Secret: sealedFor(testSecret), Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	for _, tc := range []struct{ name, method, body string }{
		{"update", http.MethodPatch, `{"url":"https://attacker.test/mo"}`},
		{"delete", http.MethodDelete, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, tc.method, webhookPath(mine, hookID.String()), tc.body))
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
			}
		})
	}
}

// TestWebhooksOfAnUnknownAccountIs404: the account is a path segment, so an unknown one is a missing
// resource — not an empty list, and not the foreign-key violation a blind insert would raise.
func TestWebhooksOfAnUnknownAccountIs404(t *testing.T) {
	api := newWebhookAPI(t, newFakeWebhookStore(), newFakeAccountStore())
	unknown := uuid.New()

	for _, tc := range []struct{ name, method, body string }{
		{"list", http.MethodGet, ""},
		{"create", http.MethodPost, `{"event_type":"mo","url":"https://a.test/h","secret":"` + testSecret + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, tc.method, webhookPath(unknown), tc.body))
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
			}
		})
	}
}

// That a rotation really re-seals the stored value is TestRotatingAWebhookSecretResealsIt, against a real
// KMS. This one covers the wire: neither the new nor the old secret comes back out.
func TestUpdateWebhookRotatesTheSecretWithoutReturningIt(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventDLR,
		URL: "https://acme.test/dlr", Secret: sealedFor("old-signing-secret-here"), Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, webhookPath(id, hookID.String()),
		`{"secret":"`+testSecret+`","status":"disabled"}`))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), testSecret) || strings.Contains(w.Body.String(), "old-signing") {
		t.Fatalf("a signing secret came back on the wire: %s", w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "disabled" {
		t.Errorf("status = %v, want disabled", got["status"])
	}
}

// TestDeleteWebhookReturns204: the contract's delete answers 204 with no body. That the row is really
// gone is the repository's property, proved against SQL in TestWebhookRepoCRUD.
func TestDeleteWebhookReturns204(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: sealedFor(testSecret), Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, webhookPath(id, hookID.String()), ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if w.Body.Len() != 0 {
		t.Errorf("204 carried a body: %s", w.Body)
	}
}

// TestListWebhooksShowsDisabledOnes: the delivery paths stop resolving a disabled webhook, but the
// surface that administers it must keep showing it — otherwise switching one off would look like
// having deleted it, and there would be no way to switch it back on.
func TestListWebhooksShowsDisabledOnes(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hooks.seed(cp.Webhook{ID: uuid.New(), AccountID: id, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: sealedFor(testSecret), Status: cp.WebhookDisabled})
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, webhookPath(id), ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var list []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &list)
	if len(list) != 1 || list[0]["status"] != "disabled" {
		t.Fatalf("list = %v, want the one disabled webhook", list)
	}
	// The list builds its DTOs on a loop of its own, so it is its own chance to leak the secret.
	if strings.Contains(w.Body.String(), testSecret) {
		t.Errorf("the signing secret came back on the wire: %s", w.Body)
	}
}
