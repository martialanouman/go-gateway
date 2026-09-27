package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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
	logger   *slog.Logger
}

// NewOIDCVerifier builds a verifier over the JWKS at jwksURL. Nothing is fetched here: the keys load on
// the first token, so the service boots and passes readiness while the identity provider is down.
func NewOIDCVerifier(ctx context.Context, logger *slog.Logger, issuer, audience, jwksURL string) *OIDCVerifier {
	keys := classifyingKeySet{oidc.NewRemoteKeySet(oidc.ClientContext(ctx, keySetClient()), jwksURL)}
	return &OIDCVerifier{logger: logger, verifier: oidc.NewVerifier(issuer, keys, &oidc.Config{
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
		v.logger.WarnContext(ctx, "operator token not judged: identity provider key set unavailable", "err", fetchFailure)
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

func keySetClient() *http.Client {
	return &http.Client{Timeout: keySetFetchTimeout, CheckRedirect: refuseDowngrade}
}

// refuseDowngrade keeps a redirect from fetching in plaintext what config required over https; past that it
// is net/http's default policy.
func refuseDowngrade(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return fmt.Errorf("key set redirected from https to %s", req.URL.Scheme)
	}
	return nil
}

// classifyingKeySet exists because IDTokenVerifier flattens the key set's error with %v: whether the
// keys could be fetched is only still visible here, so the failure is set aside for Verify to find.
type classifyingKeySet struct{ keys *oidc.RemoteKeySet }

// VerifySignature relies on go-oidc v3 wrapping every fetch failure — transport, status, body, decoding
// — as "fetching keys %w", while a token no fetched key verifies is a bare errors.New.
func (k classifyingKeySet) VerifySignature(ctx context.Context, jwt string) ([]byte, error) {
	payload, err := k.keys.VerifySignature(ctx, jwt)
	if errors.Unwrap(err) != nil {
		if slot, ok := ctx.Value(fetchFailureKey{}).(*error); ok {
			*slot = err
		}
	}
	return payload, err
}
