package auth_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

const (
	testIssuer   = "https://idp.test/realms/gw"
	testAudience = "gateway-admin"
	testKeyID    = "k1"
	testECKeyID  = "k2"
)

type idp struct {
	key   *rsa.PrivateKey
	ecKey *ecdsa.PrivateKey
	srv   *httptest.Server
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p := &idp{key: key, ecKey: ecKey}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{
			{Key: &key.PublicKey, KeyID: testKeyID, Algorithm: string(jose.RS256), Use: "sig"},
			{Key: &ecKey.PublicKey, KeyID: testECKeyID, Algorithm: string(jose.ES256), Use: "sig"},
		}})
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *idp) verifier() *auth.OIDCVerifier {
	return auth.NewOIDCVerifier(context.Background(), discard(), testIssuer, testAudience, p.srv.URL)
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func validClaims() map[string]any {
	return map[string]any{
		"iss":   testIssuer,
		"aud":   testAudience,
		"sub":   "7d1c2f0e-service-account",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"iat":   time.Now().Unix(),
		"scope": "admin:read email profile",
	}
}

func sign(t *testing.T, alg jose.SignatureAlgorithm, key any, keyID string, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", keyID))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
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

func TestOIDCVerifierKeepsTheKnownScopesOfAValidToken(t *testing.T) {
	p := newIDP(t)

	for _, tt := range []struct {
		scope string
		want  []auth.Scope
	}{
		{"admin:read email profile", []auth.Scope{auth.ScopeAdminRead}},
		{"email admin:read admin:write", []auth.Scope{auth.ScopeAdminRead, auth.ScopeAdminWrite}},
		// A principal without scopes, not an error: the missing scope is the middleware's 403.
		{"email profile", nil},
	} {
		t.Run(tt.scope, func(t *testing.T) {
			claims := validClaims()
			claims["scope"] = tt.scope

			got, err := p.verifier().Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, claims))
			if err != nil {
				t.Fatalf("Verify() error = %v", err)
			}
			if got.Subject != "7d1c2f0e-service-account" {
				t.Errorf("Subject = %q, want the token's sub", got.Subject)
			}
			if !slices.Equal(got.Scopes, tt.want) {
				t.Errorf("Scopes = %v, want %v", got.Scopes, tt.want)
			}
		})
	}
}

func TestOIDCVerifierAcceptsAnECSignedToken(t *testing.T) {
	p := newIDP(t)

	if _, err := p.verifier().Verify(context.Background(), sign(t, jose.ES256, p.ecKey, testECKeyID, validClaims())); err != nil {
		t.Errorf("Verify() error = %v, want ES256 accepted for a provider with EC keys", err)
	}
}

func TestOIDCVerifierRejectsAnInvalidToken(t *testing.T) {
	p := newIDP(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	with := func(k string, v any) map[string]any {
		c := validClaims()
		c[k] = v
		return c
	}

	for _, tt := range []struct {
		name  string
		token string
	}{
		{"expired", sign(t, jose.RS256, p.key, testKeyID, with("exp", time.Now().Add(-time.Hour).Unix()))},
		{"another issuer", sign(t, jose.RS256, p.key, testKeyID, with("iss", "https://evil.test"))},
		{"another audience", sign(t, jose.RS256, p.key, testKeyID, with("aud", "some-other-client"))},
		{"empty subject", sign(t, jose.RS256, p.key, testKeyID, with("sub", ""))},
		{"unreadable scope claim", sign(t, jose.RS256, p.key, testKeyID, with("scope", []string{"admin:write"}))},
		// Refetched, the key set still holds no key for it: a bad token, not an unavailable provider.
		{"signed by another key", sign(t, jose.RS256, otherKey, testKeyID, validClaims())},
		// The provider's own key, under an algorithm we do not accept: only the algorithm list refuses it.
		{"algorithm outside the accepted list", sign(t, jose.PS256, p.key, testKeyID, validClaims())},
		{"not a jwt", "opaque-token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.verifier().Verify(context.Background(), tt.token)
			if !errors.Is(err, errs.ErrUnauthenticated) {
				t.Errorf("Verify() error = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestOIDCVerifierReportsAnUnreachableKeySetAsUnavailable(t *testing.T) {
	answering := func(status int, body string) func(t *testing.T) string {
		return func(t *testing.T) string {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(srv.Close)
			return srv.URL
		}
	}

	for _, tt := range []struct {
		name   string
		keySet func(t *testing.T) string
	}{
		{"key set down", func(t *testing.T) string {
			srv := httptest.NewServer(http.NotFoundHandler())
			srv.Close()
			return srv.URL
		}},
		{"key set answering 500", answering(http.StatusInternalServerError, "boom")},
		{"key set answering 404", answering(http.StatusNotFound, "no such realm")},
		// A proxy or WAF page in front of the provider.
		{"key set answering HTML", answering(http.StatusOK, "<html>maintenance</html>")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newIDP(t)
			var logged bytes.Buffer
			v := auth.NewOIDCVerifier(context.Background(), slog.New(slog.NewTextHandler(&logged, nil)),
				testIssuer, testAudience, tt.keySet(t))

			_, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, validClaims()))
			if !errors.Is(err, errs.ErrServiceUnavailable) {
				t.Errorf("Verify() error = %v, want ErrServiceUnavailable: a retry can succeed", err)
			}
			if !strings.Contains(logged.String(), "oidc") {
				t.Errorf("log = %q, want the fetch failure's cause: the 503 alone does not say why", logged.String())
			}
		})
	}
}

// TestOIDCVerifierBoundsAHangingKeySet: the fetch runs in the key set's own goroutine, which every token
// with an unknown kid then waits on. Unbounded, one silent provider parks them all.
func TestOIDCVerifierBoundsAHangingKeySet(t *testing.T) {
	release := make(chan struct{})
	hanging := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer hanging.Close()
	defer close(release)
	p := newIDP(t)

	token := sign(t, jose.RS256, p.key, testKeyID, validClaims())
	done := make(chan error, 1)
	go func() {
		_, err := auth.NewOIDCVerifier(context.Background(), discard(), testIssuer, testAudience, hanging.URL).
			Verify(context.Background(), token)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, errs.ErrServiceUnavailable) {
			t.Errorf("Verify() error = %v, want ErrServiceUnavailable", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Verify() still waiting after 10s, want the key set fetch bounded")
	}
}

func TestOIDCVerifierFollowsARedirectedKeySet(t *testing.T) {
	p := newIDP(t)
	redirect := httptest.NewServer(http.RedirectHandler(p.srv.URL, http.StatusFound))
	defer redirect.Close()

	v := auth.NewOIDCVerifier(context.Background(), discard(), testIssuer, testAudience, redirect.URL)
	if _, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, validClaims())); err != nil {
		t.Errorf("Verify() error = %v, want the redirect followed", err)
	}
}
