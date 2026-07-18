package restapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// bearerScheme is the security scheme name shared by the contract and the middleware.
const bearerScheme = "BearerApiKey"

type principalKey struct{}

// apiKeyMiddleware authenticates the bearer API key on operations that declare the scheme, and
// attaches the resolved principal to the context. An operation with no security (health) passes
// through. The key is hashed (deterministic SHA-256, §1.9) and looked up by hash; the specific
// failure decides the code: an unknown/invalid key is 401, a known key on a disabled channel or a
// suspended account is 403.
func apiKeyMiddleware(api huma.API, store PrincipalStore) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		if !schemeRequired(ctx.Operation().Security) {
			next(ctx)
			return
		}

		token, ok := bearerToken(ctx.Header("Authorization"))
		if !ok {
			writeErr(api, ctx, errs.ErrUnauthenticated, "missing or malformed bearer token")
			return
		}

		principal, found, err := store.PrincipalByAPIKeyHash(ctx.Context(), credential.HashAPIKey(token))
		if err != nil {
			writeErr(api, ctx, errs.ErrInternal, "authentication failed")
			return
		}
		if !found {
			writeErr(api, ctx, errs.ErrUnauthenticated, "invalid api key")
			return
		}
		if !principal.RESTEnabled {
			writeErr(api, ctx, errs.ErrChannelDisabled, "the REST channel is disabled for this account")
			return
		}
		if principal.EffectiveStatus() != cp.AccountActive {
			writeErr(api, ctx, errs.ErrAccountSuspended, "account is not active")
			return
		}

		next(huma.WithValue(ctx, principalKey{}, principal))
	}
}

// principalFromContext returns the principal attached by the middleware. It is present on every
// authenticated operation; the second result is false only if read from an unauthenticated one.
func principalFromContext(ctx context.Context) (cp.APIKeyPrincipal, bool) {
	p, ok := ctx.Value(principalKey{}).(cp.APIKeyPrincipal)
	return p, ok
}

// schemeRequired reports whether any security requirement references the bearer scheme.
func schemeRequired(requirements []map[string][]string) bool {
	for _, req := range requirements {
		if _, ok := req[bearerScheme]; ok {
			return true
		}
	}
	return false
}

// bearerToken extracts the token from "Authorization: Bearer <token>", case-insensitively on the
// scheme.
func bearerToken(header string) (string, bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

// writeErr renders a coded error through the installed flat model.
func writeErr(api huma.API, ctx huma.Context, code errs.Code, message string) {
	status, ok := errs.HTTPStatus(code)
	if !ok {
		status = http.StatusInternalServerError
	}
	_ = huma.WriteErr(api, ctx, status, message, code)
}
