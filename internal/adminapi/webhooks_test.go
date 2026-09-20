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
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
}

// TestWebhookSecretAndURLCannotBeEmpty: an empty secret makes every HMAC signature computable by
// anyone who knows the scheme, and an empty URL points nowhere. Both bodies declare a minimum length,
// which is why the contract went to a major version.
func TestWebhookSecretAndURLCannotBeEmpty(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: testSecret, Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	for _, tc := range []struct{ name, method, path, body string }{
		{"create empty secret", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"https://a.test/h","secret":""}`},
		{"create empty url", http.MethodPost, webhookPath(id), `{"event_type":"mo","url":"","secret":"` + testSecret + `"}`},
		{"update empty secret", http.MethodPatch, webhookPath(id, hookID.String()), `{"secret":""}`},
		{"update empty url", http.MethodPatch, webhookPath(id, hookID.String()), `{"url":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, tc.method, tc.path, tc.body))
			if w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
			}
		})
	}

	// The rejected updates must not have reached the store: a 422 raised after the write would leave
	// the webhook signing with an empty secret while answering as though it had refused.
	if got := hooks.mustGet(t, id, hookID); got.Secret != testSecret || got.URL != "https://acme.test/mo" {
		t.Errorf("webhook = %+v, want the seeded url and secret untouched", got)
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
		URL: "https://theirs.test/mo", Secret: testSecret, Status: cp.WebhookActive})
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

	if got := hooks.mustGet(t, theirs, hookID); got.URL != "https://theirs.test/mo" {
		t.Errorf("the other account's webhook was rewritten: %+v", got)
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

// TestUpdateWebhookRotatesTheSecretWithoutReturningIt covers the write-only field's other half: the
// value goes in and changes the stored one, and still never comes back out.
func TestUpdateWebhookRotatesTheSecretWithoutReturningIt(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventDLR,
		URL: "https://acme.test/dlr", Secret: "old-signing-secret-here", Status: cp.WebhookActive})
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
	if stored := hooks.mustGet(t, id, hookID); stored.Secret != testSecret {
		t.Errorf("stored secret = %q, want the rotated one", stored.Secret)
	}
}

// TestDeleteWebhookReturns204AndTheListForgetsIt: the 204 alone says nothing about what the list still
// shows — a store that drops the id from one index and keeps it in another answers 204 and then serves
// a webhook the operator believes deleted.
func TestDeleteWebhookReturns204AndTheListForgetsIt(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	hooks := newFakeWebhookStore()
	hookID := uuid.New()
	hooks.seed(cp.Webhook{ID: hookID, AccountID: id, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: testSecret, Status: cp.WebhookActive})
	api := newWebhookAPI(t, hooks, accounts)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, webhookPath(id, hookID.String()), ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, webhookPath(id), ""))
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var list []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &list)
	if len(list) != 0 {
		t.Errorf("list returned %v after the delete, want empty", list)
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
		URL: "https://acme.test/mo", Secret: testSecret, Status: cp.WebhookDisabled})
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
	if strings.Contains(w.Body.String(), testSecret) {
		t.Errorf("the signing secret came back on the wire: %s", w.Body)
	}
}
