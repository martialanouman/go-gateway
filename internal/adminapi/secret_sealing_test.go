package adminapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
