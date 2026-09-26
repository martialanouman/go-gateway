package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

const keySetFetchTimeout = 5 * time.Second

// OIDCVerifier accepts JWT access tokens signed by the configured identity provider, for the configured
// audience. The subject is the token's sub; the scopes are the ones of ours its space-separated scope
// claim carries (RFC 9068), the provider's own being ignored.
type OIDCVerifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewOIDCVerifier builds a verifier over the JWKS at jwksURL. Nothing is fetched here: the keys load on
// the first token, so the service boots and passes readiness while the identity provider is down.
func NewOIDCVerifier(ctx context.Context, issuer, audience, jwksURL string) *OIDCVerifier {
	client := &http.Client{Timeout: keySetFetchTimeout, Transport: failNonOK{http.DefaultTransport}}
	keys := classifyingKeySet{oidc.NewRemoteKeySet(oidc.ClientContext(ctx, client), jwksURL)}
	return &OIDCVerifier{verifier: oidc.NewVerifier(issuer, keys, &oidc.Config{
		ClientID:             audience,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.ES256},
	})}
}

// Verify returns the token's Principal, ErrServiceUnavailable when the key set cannot be fetched — a
// retry can succeed, so a caller must not take it for a dead session — and ErrUnauthenticated otherwise.
func (v *OIDCVerifier) Verify(ctx context.Context, token string) (Principal, error) {
	var fetchFailure error
	idToken, err := v.verifier.Verify(context.WithValue(ctx, fetchFailureKey{}, &fetchFailure), token)
	if fetchFailure != nil {
		return Principal{}, fmt.Errorf("%w: %w", errs.ErrServiceUnavailable, fetchFailure)
	}
	if err != nil {
		return Principal{}, errs.ErrUnauthenticated
	}

	var claims struct {
		Scope string `json:"scope"`
	}
	if idToken.Subject == "" || idToken.Claims(&claims) != nil {
		return Principal{}, errs.ErrUnauthenticated
	}
	var scopes []Scope
	for _, s := range strings.Fields(claims.Scope) {
		if knownScope(Scope(s)) {
			scopes = append(scopes, Scope(s))
		}
	}
	return Principal{Subject: idToken.Subject, Scopes: scopes}, nil
}

type fetchFailureKey struct{}

// classifyingKeySet exists because IDTokenVerifier flattens the key set's error with %v: the transport
// failure is only still typed here, so it is set aside for Verify to find.
type classifyingKeySet struct{ keys *oidc.RemoteKeySet }

func (k classifyingKeySet) VerifySignature(ctx context.Context, jwt string) ([]byte, error) {
	payload, err := k.keys.VerifySignature(ctx, jwt)
	var transport *url.Error
	if errors.As(err, &transport) {
		if slot, ok := ctx.Value(fetchFailureKey{}).(*error); ok {
			*slot = transport
		}
	}
	return payload, err
}

// failNonOK makes a non-200 key set response a transport error, as a connection failure is, because
// go-oidc formats the status with %s and would otherwise leave it indistinguishable from a bad token.
type failNonOK struct{ next http.RoundTripper }

func (t failNonOK) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("key set answered %s", resp.Status)
	}
	return resp, nil
}
