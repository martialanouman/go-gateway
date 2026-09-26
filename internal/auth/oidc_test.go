package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
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

func (p *idp) verifier(t *testing.T) *auth.OIDCVerifier {
	t.Helper()
	return auth.NewOIDCVerifier(context.Background(), testIssuer, testAudience, p.srv.URL)
}

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

func sign(t *testing.T, alg jose.SignatureAlgorithm, key any, claims map[string]any) string {
	t.Helper()
	return signWithKeyID(t, alg, key, testKeyID, claims)
}

func signWithKeyID(t *testing.T, alg jose.SignatureAlgorithm, key any, keyID string, claims map[string]any) string {
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

func TestOIDCVerifierAcceptsAValidTokenAndKeepsOnlyKnownScopes(t *testing.T) {
	p := newIDP(t)

	got, err := p.verifier(t).Verify(context.Background(), sign(t, jose.RS256, p.key, validClaims()))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if got.Subject != "7d1c2f0e-service-account" {
		t.Errorf("Subject = %q, want the token's sub", got.Subject)
	}
	if !slices.Equal(got.Scopes, []auth.Scope{auth.ScopeAdminRead}) {
		t.Errorf("Scopes = %v, want only admin:read (email and profile are the provider's, not ours)", got.Scopes)
	}
}

func TestOIDCVerifierAcceptsAnECSignedToken(t *testing.T) {
	p := newIDP(t)

	if _, err := p.verifier(t).Verify(context.Background(), signWithKeyID(t, jose.ES256, p.ecKey, testECKeyID, validClaims())); err != nil {
		t.Errorf("Verify() error = %v, want ES256 accepted for a provider with EC keys", err)
	}
}

func TestOIDCVerifierGrantsNoScopeWithoutAKnownOne(t *testing.T) {
	p := newIDP(t)
	claims := validClaims()
	claims["scope"] = "email profile"

	got, err := p.verifier(t).Verify(context.Background(), sign(t, jose.RS256, p.key, claims))
	if err != nil {
		t.Fatalf("Verify() error = %v, want a principal: the missing scope is the middleware's 403", err)
	}
	if len(got.Scopes) != 0 {
		t.Errorf("Scopes = %v, want none", got.Scopes)
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
		{"expired", sign(t, jose.RS256, p.key, with("exp", time.Now().Add(-time.Hour).Unix()))},
		{"another issuer", sign(t, jose.RS256, p.key, with("iss", "https://evil.test"))},
		{"another audience", sign(t, jose.RS256, p.key, with("aud", "some-other-client"))},
		{"empty subject", sign(t, jose.RS256, p.key, with("sub", ""))},
		{"signed by another key", sign(t, jose.RS256, otherKey, validClaims())},
		{"symmetric algorithm", sign(t, jose.HS256, []byte("a-shared-secret-of-at-least-32-bytes"), validClaims())},
		// The provider's own key, under an algorithm we do not accept: only the algorithm list refuses it.
		{"algorithm outside the accepted list", sign(t, jose.PS256, p.key, validClaims())},
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

func TestOIDCVerifierReportsAnUnreachableKeySetAsUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name   string
		keySet func(t *testing.T) string
	}{
		{"key set down", func(t *testing.T) string {
			srv := httptest.NewServer(http.NotFoundHandler())
			srv.Close()
			return srv.URL
		}},
		{"key set answering 500", func(t *testing.T) string {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "boom", http.StatusInternalServerError)
			}))
			t.Cleanup(srv.Close)
			return srv.URL
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := newIDP(t)
			v := auth.NewOIDCVerifier(context.Background(), testIssuer, testAudience, tt.keySet(t))

			_, err := v.Verify(context.Background(), sign(t, jose.RS256, p.key, validClaims()))
			if !errors.Is(err, errs.ErrServiceUnavailable) {
				t.Errorf("Verify() error = %v, want ErrServiceUnavailable: a retry can succeed", err)
			}
		})
	}
}
