package auth

import (
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// Middleware authenticates the bearer token and enforces the scopes the OPERATION ITSELF declares
// under schemeName. Deriving the requirement from ctx.Operation().Security is the point: the
// published contract and the enforcement are the same data, so they cannot drift — a handler that
// forgets its scopes fails the contract test, not production.
//
// Behaviour: an operation with no security declaration passes through; a missing or malformed
// bearer token, or one the verifier rejects, is 401 unauthenticated; a valid token whose principal
// lacks a required scope is 403 forbidden_scope. On success the Principal is attached to the
// request context for handlers to read.
func Middleware(api huma.API, v TokenVerifier, schemeName string) func(huma.Context, func(huma.Context)) {
	return func(ctx huma.Context, next func(huma.Context)) {
		requirements := ctx.Operation().Security
		if !schemeRequired(requirements, schemeName) {
			// No security tied to our scheme: the operation is public (none are, in M1).
			next(ctx)
			return
		}

		token, ok := bearerToken(ctx.Header("Authorization"))
		if !ok {
			writeErr(api, ctx, errs.ErrUnauthenticated, "missing or malformed bearer token")
			return
		}

		principal, err := v.Verify(ctx.Context(), token)
		if err != nil {
			writeErr(api, ctx, errs.ErrUnauthenticated, "invalid operator token")
			return
		}

		if !authorized(requirements, schemeName, principal) {
			writeErr(api, ctx, errs.ErrForbiddenScope, "operator token lacks a required scope")
			return
		}

		next(huma.WithValue(ctx, principalKey{}, principal))
	}
}

// schemeRequired reports whether any security requirement references schemeName.
func schemeRequired(requirements []map[string][]string, schemeName string) bool {
	for _, req := range requirements {
		if _, ok := req[schemeName]; ok {
			return true
		}
	}
	return false
}

// authorized reports whether principal satisfies at least one security requirement (OpenAPI ORs the
// alternatives; the scopes within one are ANDed).
func authorized(requirements []map[string][]string, schemeName string, principal Principal) bool {
	for _, req := range requirements {
		scopes, ok := req[schemeName]
		if !ok {
			continue
		}
		if principalHasAll(principal, scopes) {
			return true
		}
	}
	return false
}

func principalHasAll(principal Principal, scopes []string) bool {
	for _, s := range scopes {
		if !principal.Has(Scope(s)) {
			return false
		}
	}
	return true
}

// bearerToken extracts the token from an "Authorization: Bearer <token>" header, case-insensitively
// on the scheme.
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

// writeErr renders a coded error through the installed flat model. Passing the code as the detail
// error lets the model resolve it exactly (and without adding a spurious errors[] entry). It
// swallows WriteErr's own error return (a serialization failure), which cannot be handled from
// inside a middleware anyway.
func writeErr(api huma.API, ctx huma.Context, code errs.Code, message string) {
	status, ok := errs.HTTPStatus(code)
	if !ok {
		status = http.StatusInternalServerError
	}
	_ = huma.WriteErr(api, ctx, status, message, code)
}
