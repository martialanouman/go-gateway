package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestNewAdminAppAuthenticatesAgainstTheConfiguredProvider proves, on the graph newAdminApp builds, that a
// configured provider is the verifier in service: its token writes under its sub, and a static token the
// same config still carries is refused.
func TestNewAdminAppAuthenticatesAgainstTheConfiguredProvider(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
			{Key: &key.PublicKey, KeyID: "k1", Algorithm: string(jose.RS256), Use: "sig"},
		}})
	}))
	defer jwks.Close()

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.OIDC = config.OIDC{Issuer: "https://idp.test", Audience: "gateway-admin", JWKSURL: jwks.URL}
	pool := pgtest.Pool(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	subject := "service-account-" + uuid.NewString()
	create := func(bearer string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/admin/customers",
			strings.NewReader(fmt.Sprintf(`{"name":"oidc-%s"}`, uuid.NewString())))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		rec := httptest.NewRecorder()
		app.http.Handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := create(signedToken(t, key, subject)); code != http.StatusCreated {
		t.Fatalf("create customer with the provider's token = %d, want 201", code)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.audit_log
		WHERE operation_id = 'create-customer' AND operator = $1`, subject).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("audit rows under the token's sub = %d, %v; want 1", rows, err)
	}

	if code := create("test-token"); code != http.StatusUnauthorized {
		t.Errorf("create customer with a static token = %d, want 401: the provider is the only verifier", code)
	}
}

func signedToken(t *testing.T, key *rsa.PrivateKey, subject string) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"iss": "https://idp.test", "aud": "gateway-admin", "sub": subject,
		"exp": time.Now().Add(time.Hour).Unix(), "scope": "admin:write",
	})
	if err != nil {
		t.Fatal(err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jws.CompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}
