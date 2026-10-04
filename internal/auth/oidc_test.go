package auth_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

const (
	testIssuer   = "https://idp.test/realms/gw"
	testAudience = "gateway-admin"
	testKeyID    = "k1"
	testECKeyID  = "k2"
	// testSubject is an operator id, the only sub ADR-0019 lets the BFF sign.
	testSubject = "0199a1b2-7c3d-7e4f-8a5b-6c7d8e9f0a1b"
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

func (p *idp) verifier(t *testing.T) *auth.OIDCVerifier {
	t.Helper()
	return newVerifier(t, discard(), p.srv.URL, "")
}

// servedUnder serves the provider's key set over https, under a certificate ca signed for localhost.
func (p *idp) servedUnder(t *testing.T, ca *tlstest.CA) string {
	t.Helper()
	return serveTLS(t, ca, p.srv.Config.Handler)
}

func serveTLS(t *testing.T, ca *tlstest.CA, handler http.Handler) string {
	t.Helper()
	certFile, keyFile := ca.Issue(t, "jwks", "localhost")
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return "https://localhost:" + srv.URL[strings.LastIndex(srv.URL, ":")+1:]
}

func newVerifier(t *testing.T, logger *slog.Logger, jwksURL, jwksCAFile string) *auth.OIDCVerifier {
	t.Helper()
	v, err := auth.NewOIDCVerifier(context.Background(), logger, testIssuer, testAudience, jwksURL, jwksCAFile)
	if err != nil {
		t.Fatalf("NewOIDCVerifier: %v", err)
	}
	return v
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func validClaims() map[string]any {
	return map[string]any{
		"iss":   testIssuer,
		"aud":   testAudience,
		"sub":   testSubject,
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

			got, err := p.verifier(t).Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, claims))
			if err != nil {
				t.Fatalf("Verify() error = %v", err)
			}
			if got.Subject != testSubject {
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

	if _, err := p.verifier(t).Verify(context.Background(), sign(t, jose.ES256, p.ecKey, testECKeyID, validClaims())); err != nil {
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
			_, err := p.verifier(t).Verify(context.Background(), tt.token)
			if !errors.Is(err, errs.ErrUnauthenticated) {
				t.Errorf("Verify() error = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

// TestOIDCVerifierRefusesASubjectThatIsNotAnOperatorID: ADR-0019 makes every sub the BFF signs an
// operator id. Any other is a misconfigured issuer, refused on every call rather than recorded nowhere,
// and named in the log, which the bare 401 is not.
func TestOIDCVerifierRefusesASubjectThatIsNotAnOperatorID(t *testing.T) {
	p := newIDP(t)
	var logged bytes.Buffer
	v := newVerifier(t, slog.New(slog.NewTextHandler(&logged, nil)), p.srv.URL, "")
	claims := validClaims()
	claims["sub"] = "service-account-dashboard"

	_, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, claims))
	if !errors.Is(err, errs.ErrUnauthenticated) {
		t.Errorf("Verify() error = %v, want ErrUnauthenticated", err)
	}
	if !strings.Contains(logged.String(), "service-account-dashboard") {
		t.Errorf("log = %q, want the refused sub named", logged.String())
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
			v := newVerifier(t, slog.New(slog.NewTextHandler(&logged, nil)), tt.keySet(t), "")

			_, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, validClaims()))
			if !errors.Is(err, errs.ErrServiceUnavailable) {
				t.Errorf("Verify() error = %v, want ErrServiceUnavailable: a retry can succeed", err)
			}
			if !strings.Contains(logged.String(), "fetching keys") {
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
	v := newVerifier(t, discard(), hanging.URL, "")
	done := make(chan error, 1)
	go func() {
		_, err := v.Verify(context.Background(), token)
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

	v := newVerifier(t, discard(), redirect.URL, "")
	if _, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, validClaims())); err != nil {
		t.Errorf("Verify() error = %v, want the redirect followed", err)
	}
}

// TestOIDCVerifierTrustsTheConfiguredAuthorityForTheKeySet: the BFF serves its JWKS under the operator's
// internal PKI (ADR-0019), which the system roots do not know.
func TestOIDCVerifierTrustsTheConfiguredAuthorityForTheKeySet(t *testing.T) {
	p := newIDP(t)
	ca := tlstest.NewCA(t)
	jwksURL := p.servedUnder(t, ca)
	token := sign(t, jose.RS256, p.key, testKeyID, validClaims())

	if _, err := newVerifier(t, discard(), jwksURL, ca.CAFile).Verify(context.Background(), token); err != nil {
		t.Errorf("Verify() under the configured authority error = %v", err)
	}
	_, err := newVerifier(t, discard(), jwksURL, "").Verify(context.Background(), token)
	if !errors.Is(err, errs.ErrServiceUnavailable) {
		t.Errorf("Verify() on the system roots error = %v, want ErrServiceUnavailable", err)
	}
}

// TestOIDCVerifierRefusesAKeySetRedirectedToPlaintext proves, through NewOIDCVerifier, what
// TestKeySetRedirectsNeverDowngradeToPlaintext proves of the policy alone: the verifier fetches with it.
func TestOIDCVerifierRefusesAKeySetRedirectedToPlaintext(t *testing.T) {
	p := newIDP(t)
	ca := tlstest.NewCA(t)
	downgrade := serveTLS(t, ca, http.RedirectHandler(p.srv.URL, http.StatusFound))

	_, err := newVerifier(t, discard(), downgrade, ca.CAFile).
		Verify(context.Background(), sign(t, jose.RS256, p.key, testKeyID, validClaims()))
	if !errors.Is(err, errs.ErrServiceUnavailable) {
		t.Errorf("Verify() error = %v, want ErrServiceUnavailable: the plaintext key set must not be read", err)
	}
}

func TestNewOIDCVerifierRefusesAnUnusableAuthority(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "empty.crt")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, caFile := range map[string]string{
		"missing file":      filepath.Join(t.TempDir(), "absent.crt"),
		"no certificate in": empty,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := auth.NewOIDCVerifier(context.Background(), discard(), testIssuer, testAudience, "https://bff/jwks", caFile)
			if err == nil {
				t.Error("NewOIDCVerifier() error = nil, want the unusable authority refused")
			}
		})
	}
}
