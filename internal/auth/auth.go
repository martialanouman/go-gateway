// Package auth authenticates operators on the Admin API and enforces the scopes each operation
// declares.
//
// At M1 the identity provider does not exist yet: StaticVerifier compares bearer tokens against a
// configured list, so the authorization path is real — 401 and 403 genuinely happen and are
// genuinely tested — while OIDC/mTLS is deferred to M12. Nothing above the TokenVerifier interface
// changes when the real provider lands.
package auth

import (
	"context"
)

// Scope is an OAuth2 scope from the OperatorBearer scheme of api/openapi-admin.yaml.
type Scope string

// The operator scopes. M1 uses admin:read on reads and admin:write on mutations; the content and
// GDPR scopes exist for the operations later milestones add.
const (
	ScopeAdminRead    Scope = "admin:read"
	ScopeAdminWrite   Scope = "admin:write"
	ScopeContentRead  Scope = "content:read"
	ScopeContentErase Scope = "content:erase"
	ScopeGDPRErase    Scope = "gdpr:erase"
	// ScopeMSISDNReveal shows subscriber numbers unmasked. Separate from content:read on purpose: reading a
	// body is a greater act than seeing a destination, so one must not imply the other.
	ScopeMSISDNReveal Scope = "msisdn:reveal"
)

// Principal is the authenticated operator behind a request.
type Principal struct {
	Subject string
	Scopes  []Scope
}

// Has reports whether the principal holds scope s.
func (p Principal) Has(s Scope) bool {
	for _, held := range p.Scopes {
		if held == s {
			return true
		}
	}
	return false
}

// TokenVerifier turns a bearer token into a Principal, or an error when the token is unknown. M1
// ships StaticVerifier; M12 replaces it with OIDC discovery + JWKS behind this same interface.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (Principal, error)
}

// principalKey is the unexported context key under which the middleware stores the Principal, so no
// other package can collide with or forge it.
type principalKey struct{}

// PrincipalFrom returns the Principal the middleware attached to ctx, if the request was
// authenticated. The middleware stores it with huma.WithValue under principalKey{}.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
