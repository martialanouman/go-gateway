package adminapi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// kmsSealer is the sealer the Admin API is given, backed by a REAL KMS rather than a stand-in that
// returns a marker: the point of these tests is that what lands in the column opens again, and a fake
// that hands back the plaintext under another name would prove exactly nothing.
type kmsSealer struct {
	kms  content.KMS
	fail error
}

func newKMSSealer() *kmsSealer { return &kmsSealer{kms: content.NewDevKMS()} }

func (s *kmsSealer) Seal(ctx context.Context, plaintext []byte) (cp.SealedSecret, error) {
	if s.fail != nil {
		return cp.SealedSecret{}, s.fail
	}
	sealed, err := s.kms.WrapDataKey(ctx, plaintext)
	if err != nil {
		return cp.SealedSecret{}, err
	}
	return cp.SealedSecret{Sealed: sealed, KMSKeyRef: s.kms.KeyRef()}, nil
}

// open is what a future connector-pool-svc or billing HTTP provider will do with the stored bytes.
func (s *kmsSealer) open(t *testing.T, secret cp.SealedSecret) []byte {
	t.Helper()
	plaintext, err := s.kms.UnwrapDataKey(context.Background(), secret.Sealed)
	if err != nil {
		t.Fatalf("the stored secret does not open: %v", err)
	}
	return plaintext
}

// This is step-295 itself: the outbound bind password must come back USABLE. It used to be an argon2id
// hash, which no bind_transceiver PDU can carry (SMPP v3.4 §4.1.1) — the column existed for a purpose it
// structurally could not serve.
func TestCreateConnectorStoresAPasswordThatOpensAgain(t *testing.T) {
	store := newFakeConnectorStore()
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, SecretSealer: sealer})

	const password = "s3cr3t-canary"
	body := `{"name":"smsc-1","host":"smsc.example","port":2775,"bind_type":"trx","system_id":"sys","password":"` + password + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}

	stored := store.created.Password
	if len(stored.Sealed) == 0 {
		t.Fatal("nothing was stored in password_sealed")
	}
	if bytes.Contains(stored.Sealed, []byte(password)) {
		t.Error("the stored bytes contain the password in clear")
	}
	if stored.KMSKeyRef == "" {
		t.Error("password_kms_key_ref is empty: nothing says which master key sealed the row")
	}
	if got := sealer.open(t, stored); string(got) != password {
		t.Errorf("the stored password opens to %q, want %q", got, password)
	}
}

// A sealing failure must abort the write. Storing the connector anyway would leave a row whose password
// column holds nothing usable — the very defect this step exists to remove, re-created at runtime.
func TestCreateConnectorRefusesWhenSealingFails(t *testing.T) {
	store := newFakeConnectorStore()
	sealer := newKMSSealer()
	sealer.fail = context.DeadlineExceeded
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-1","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"p"}`))

	if w.Code < 500 {
		t.Errorf("status = %d, want a 5xx when the secret cannot be sealed; body=%s", w.Code, w.Body)
	}
	if store.createCount != 0 {
		t.Error("the connector was stored although its password could not be sealed")
	}
	if strings.Contains(w.Body.String(), "\"p\"") {
		t.Errorf("the error response echoes the password: %s", w.Body)
	}
}

// Same decision, same proof, for the other secret: the provider credentials were stored as CLEAR jsonb,
// so they sat in every backup and every replica. Masking them on read protected the HTTP response only.
func TestCreateProviderStoresCredentialsSealedAndTheyOpenAgain(t *testing.T) {
	store := &fakeProviderStore{}
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: store, SecretSealer: sealer})

	const apiKey = "SUPER-SECRET"
	body := `{"name":"acme","base_url":"https://acme.example","mode":"balance_check","auth_config_json":{"api_key":"` + apiKey + `"}}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/billing-providers", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}

	stored := store.created.AuthConfig
	if bytes.Contains(stored.Sealed, []byte(apiKey)) {
		t.Error("the stored bytes contain the credentials in clear")
	}
	if stored.KMSKeyRef == "" {
		t.Error("auth_config_kms_key_ref is empty")
	}
	var opened map[string]any
	if err := json.Unmarshal(sealer.open(t, stored), &opened); err != nil {
		t.Fatalf("what was stored does not open as the JSON document that went in: %v", err)
	}
	if opened["api_key"] != apiKey {
		t.Errorf("api_key = %v, want %q", opened["api_key"], apiKey)
	}
}

// A provider created without credentials still needs sealed bytes in a NOT NULL column, and '{}' has no
// constant sealed form (the nonce is per-call), so the handler seals it rather than the DDL defaulting it.
func TestCreateProviderWithoutCredentialsSealsAnEmptyDocument(t *testing.T) {
	store := &fakeProviderStore{}
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: store, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/billing-providers",
		`{"name":"acme","base_url":"https://acme.example","mode":"balance_check"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}

	stored := store.created.AuthConfig
	if len(stored.Sealed) == 0 {
		t.Fatal("auth_config_sealed is empty, and the column is NOT NULL")
	}
	if got := sealer.open(t, stored); string(got) != "{}" {
		t.Errorf("the stored document opens to %q, want %q", got, "{}")
	}
}

// Rotating a connector password through PATCH. Untested, this branch could be deleted outright and a
// rotation would answer 200 without changing anything — the exact "a surface that answers 200 to a setting
// with no effect" this step was opened to remove, one layer earlier than the debt describes it.
func TestPatchConnectorSealsTheRotatedPassword(t *testing.T) {
	store := newFakeConnectorStore()
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-1","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"first"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("create: status = %d; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)

	const rotated = "second-s3cr3t"
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+created["id"].(string),
		`{"password":"`+rotated+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("patch: status = %d; body=%s", w.Code, w.Body)
	}

	got := store.patched.Password
	if got == nil {
		t.Fatal("the rotation reached the store with no password: it answered 200 and changed nothing")
	}
	if bytes.Contains(got.Sealed, []byte(rotated)) {
		t.Error("the rotated password went to the store in clear")
	}
	// Both halves or neither: a ciphertext from one master key beside a reference naming another is a row
	// that opens today and that a key rotation would skip.
	if got.KMSKeyRef == "" {
		t.Error("password_kms_key_ref was not part of the rotation")
	}
	if opened := sealer.open(t, *got); string(opened) != rotated {
		t.Errorf("the rotated password opens to %q, want %q", opened, rotated)
	}
}

// The canaries a read response must never carry, in any encoding.
const (
	sealedCanary = "SEALED-CANARY"
	keyRefCanary = "KEYREF-CANARY"
)

// step-295 ADDED a leak surface that did not exist before: cp.Connector now carries the sealed password,
// where the type previously had no password field at all ("write-only: it is hashed on the way in and never
// read back, so it has no field here"). Nothing but toConnectorDTO's shape keeps it off the wire, and a
// field added there later would publish it without a single test rougissant.
//
// The fake must therefore HOLD the secret on the read path, or this asserts the double's omission.
func TestReadingAConnectorNeverReturnsTheSealedPassword(t *testing.T) {
	store := newFakeConnectorStore()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-read","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"s3cr3t"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("create: status = %d; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	id := created["id"].(string)

	// A recognisable stored value, as Postgres would hand it back.
	store.setPassword(uuid.MustParse(id), cp.SealedSecret{Sealed: []byte(sealedCanary), KMSKeyRef: keyRefCanary})

	for _, path := range []string{"/v1/admin/connectors", "/v1/admin/connectors/" + id} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodGet, path, ""))
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s: status = %d; body=%s", path, w.Code, w.Body)
		}
		// The base64 form is the one that would actually appear: encoding/json renders a []byte that way,
		// so a DTO field carrying the sealed bytes publishes "U0VBTEVELUNBTkFSWQ==" and never the literal
		// canary. Derived from the canary rather than written out, so the two cannot drift apart.
		for _, canary := range []string{
			sealedCanary, base64.StdEncoding.EncodeToString([]byte(sealedCanary)),
			keyRefCanary, base64.StdEncoding.EncodeToString([]byte(keyRefCanary)),
			"password",
		} {
			if strings.Contains(w.Body.String(), canary) {
				t.Errorf("GET %s leaks %q: %s", path, canary, w.Body)
			}
		}
	}
}

func TestCreateWebhookStoresASecretThatOpensAgain(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	store := newFakeWebhookStore()
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{Webhooks: store, Accounts: accounts, SecretSealer: sealer})

	const secret = "whsec-canary-long-enough"
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"`+secret+`"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)

	stored := store.mustGet(t, uuid.MustParse(created["id"].(string)))
	if bytes.Contains(stored.Secret.Sealed, []byte(secret)) {
		t.Error("the signing secret reached the store in clear")
	}
	if stored.Secret.KMSKeyRef == "" {
		t.Error("no key reference stored: the row opens today and cannot be placed after a key rotation")
	}
	if got := sealer.open(t, stored.Secret); string(got) != secret {
		t.Errorf("the stored secret opens to %q, want %q", got, secret)
	}
}

func TestRotatingAWebhookSecretResealsIt(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	store := newFakeWebhookStore()
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{Webhooks: store, Accounts: accounts, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"first-secret-long-enough"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("create: status = %d; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	whID := uuid.MustParse(created["id"].(string))
	before := store.mustGet(t, whID).Secret

	const rotated = "second-secret-long-enough"
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, webhookPath(id, whID.String()),
		`{"secret":"`+rotated+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("patch: status = %d; body=%s", w.Code, w.Body)
	}

	after := store.mustGet(t, whID).Secret
	if bytes.Equal(after.Sealed, before.Sealed) {
		t.Fatal("the rotation left the stored ciphertext untouched: it answered 200 and changed nothing")
	}
	if bytes.Contains(after.Sealed, []byte(rotated)) {
		t.Error("the rotated secret went to the store in clear")
	}
	if got := sealer.open(t, after); string(got) != rotated {
		t.Errorf("the rotated secret opens to %q, want %q", got, rotated)
	}
}

func TestCreateWebhookRefusesWhenSealingFails(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	store := newFakeWebhookStore()
	sealer := newKMSSealer()
	sealer.fail = context.DeadlineExceeded
	api := newTestAPIWith(t, adminapi.Deps{Webhooks: store, Accounts: accounts, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"whsec-canary-long-enough"}`))

	if w.Code < 500 {
		t.Errorf("status = %d, want a 5xx when the secret cannot be sealed; body=%s", w.Code, w.Body)
	}
	if len(store.byID) != 0 {
		t.Error("the webhook was stored although its secret could not be sealed")
	}
}

func TestReadingAWebhookNeverReturnsItsSealedSecret(t *testing.T) {
	accounts := newFakeAccountStore()
	id := seedAccount(t, accounts)
	store := newFakeWebhookStore()
	sealer := newKMSSealer()
	api := newTestAPIWith(t, adminapi.Deps{Webhooks: store, Accounts: accounts, SecretSealer: sealer})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, webhookPath(id),
		`{"event_type":"mo","url":"https://acme.test/mo","secret":"whsec-canary-long-enough"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("create: status = %d; body=%s", w.Code, w.Body)
	}
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	stored := store.mustGet(t, uuid.MustParse(created["id"].(string)))

	for _, probe := range []struct{ name, needle string }{
		{"sealed bytes", string(stored.Secret.Sealed)},
		{"sealed bytes in base64", base64.StdEncoding.EncodeToString(stored.Secret.Sealed)},
		{"key reference", stored.Secret.KMSKeyRef},
	} {
		w = httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodGet, webhookPath(id), ""))
		if strings.Contains(w.Body.String(), probe.needle) {
			t.Errorf("list leaks the %s: %s", probe.name, w.Body)
		}
	}
}
