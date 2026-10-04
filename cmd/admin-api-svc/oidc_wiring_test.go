package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// TestNewAdminAppAuthenticatesAgainstTheConfiguredProvider proves, on the graph newAdminApp builds, that a
// configured provider is the verifier in service: its key set is read under OIDC_JWKS_CA_FILE, its token
// writes under its sub — audit row and created_by — and a static token the same config still carries is
// refused.
func TestNewAdminAppAuthenticatesAgainstTheConfiguredProvider(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ca := tlstest.NewCA(t)
	jwksURL := serveKeySet(t, ca, &key.PublicKey)

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.OIDC = config.OIDC{Issuer: "https://idp.test", Audience: "gateway-admin", JWKSURL: jwksURL, JWKSCAFile: ca.CAFile}
	pool := pgtest.Pool(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	subject := uuid.NewString()
	post := func(path, bearer string) int {
		req := httptest.NewRequest(http.MethodPost, path,
			strings.NewReader(fmt.Sprintf(`{"name":"oidc-%s"}`, uuid.NewString())))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		rec := httptest.NewRecorder()
		app.http.Handler.ServeHTTP(rec, req)
		return rec.Code
	}
	create := func(bearer string) int { return post("/v1/admin/customers", bearer) }

	if code := create(signedToken(t, key, subject)); code != http.StatusCreated {
		t.Fatalf("create customer with the provider's token = %d, want 201", code)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.audit_log
		WHERE operation_id = 'create-customer' AND operator = $1`, subject).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("audit rows under the token's sub = %d, %v; want 1", rows, err)
	}

	if code := post("/v1/admin/customer-groups", signedToken(t, key, subject)); code != http.StatusCreated {
		t.Fatalf("create customer group with the provider's token = %d, want 201", code)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.customer_groups
		WHERE created_by = $1`, subject).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("customer groups created by the token's sub = %d, %v; want 1", rows, err)
	}

	if code := create("test-token"); code != http.StatusUnauthorized {
		t.Errorf("create customer with a static token = %d, want 401: the provider is the only verifier", code)
	}
}

// TestNewAdminAppRefusesToBootOnAnUnusableKeySetAuthority: a CA that cannot be read would otherwise
// surface as a 503 on every operator token, long after the rollout looked healthy.
func TestNewAdminAppRefusesToBootOnAnUnusableKeySetAuthority(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.OIDC = config.OIDC{Issuer: "https://idp.test", Audience: "gateway-admin", JWKSURL: "https://bff.test/jwks",
		JWKSCAFile: filepath.Join(t.TempDir(), "absent.crt")}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newAdminApp() error = nil, want the unreadable authority to stop the boot")
	}
	if !strings.Contains(err.Error(), "OIDC_JWKS_CA_FILE") {
		t.Errorf("newAdminApp() error = %v, want it to name OIDC_JWKS_CA_FILE", err)
	}
}

// serveKeySet serves pub as a JWKS over https, under a certificate ca signed for localhost.
func serveKeySet(t *testing.T, ca *tlstest.CA, pub *rsa.PublicKey) string {
	t.Helper()
	certFile, keyFile := ca.Issue(t, "jwks", "localhost")
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
			{Key: pub, KeyID: "k1", Algorithm: string(jose.RS256), Use: "sig"},
		}})
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return "https://localhost:" + srv.URL[strings.LastIndex(srv.URL, ":")+1:]
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
