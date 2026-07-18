package adminapi

import (
	"context"
	"log/slog"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/platform/humaspec"
)

// operatorScheme is the security scheme name shared by the contract and the middleware.
const operatorScheme = "OperatorBearer"

// serverURL is the contract's declared server. Mounting chi at /v1 and registering operations at
// /admin/... makes the registered paths compare literally against api/openapi-admin.yaml, whose
// server carries the /v1 prefix.
const serverURL = "https://admin.gateway.internal/v1"

// New builds the Admin API: a chi router serving the operations of api/openapi-admin.yaml under
// /v1, and the huma API that generated them. The huma API is returned so the contract test can read
// the generated spec without opening a socket.
//
// A nil store is tolerated: registration wires handlers but calls no store, so the contract test
// can build the API purely to read its spec. A running server passes real repositories.
func New(deps Deps) (*chi.Mux, huma.API) {
	humaerr.Install()

	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	root := chi.NewMux()
	// RealIP is deliberately omitted: it trusts client-supplied forwarding headers and is a spoofing
	// vector (chi GHSA-3fxj-6jh8-hvhx). The Admin API sits behind an ingress that sets the peer
	// address; if a trusted client IP is ever needed, derive it from that ingress, not from headers.
	root.Use(chimiddleware.RequestID, chimiddleware.Recoverer)

	v1 := chi.NewMux()
	root.Mount("/v1", v1)

	cfg := huma.DefaultConfig("Admin API", "1.0.0")
	cfg.Servers = []*huma.Server{{URL: serverURL, Description: "Internal (mTLS + operator bearer)"}}
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		operatorScheme: operatorSecurityScheme(),
	}
	cfg.Security = []map[string][]string{{operatorScheme: {}}}

	api := humachi.New(v1, cfg)

	// Enforce the scopes each operation declares. Skipped only when no verifier is supplied — that
	// is the spec-only construction the contract test uses, which issues no requests.
	if deps.Verifier != nil {
		api.UseMiddleware(auth.Middleware(api, deps.Verifier, operatorScheme))
	}

	registerCustomers(api, deps.Customers)
	registerAccounts(api, deps.Accounts)
	registerCredentials(api, deps.Credentials, deps.Accounts)
	registerConnectors(api, deps.Connectors)
	registerRoutes(api, deps.Routes)
	registerSenderIDs(api, deps.SenderIDs, deps.Customers)

	humaspec.Prune(api, codesMetaKey)

	return root, api
}

// operatorSecurityScheme mirrors the OperatorBearer scheme of api/openapi-admin.yaml: OAuth2
// client-credentials with the operator scopes. Huma's DefaultConfig would emit an http/bearer
// scheme; the contract says oauth2, and the contract wins.
func operatorSecurityScheme() *huma.SecurityScheme {
	return &huma.SecurityScheme{
		Type: "oauth2",
		Flows: &huma.OAuthFlows{
			// #nosec G101 -- a documented token endpoint URL, not an embedded credential.
			ClientCredentials: &huma.OAuthFlow{
				TokenURL: "https://admin.gateway.internal/oauth/token",
				Scopes: map[string]string{
					string(auth.ScopeAdminRead):    "Read control-plane configuration.",
					string(auth.ScopeAdminWrite):   "Modify control-plane configuration.",
					string(auth.ScopeContentRead):  "Read message content under audit.",
					string(auth.ScopeContentErase): "Erase stored message content.",
					string(auth.ScopeGDPRErase):    "Erase a customer's data (GDPR).",
				},
			},
		},
	}
}

// codesMetaKey is the metadata key under which register stashes an operation's intended error codes
// for humaspec.Prune. It is per-API: the public and admin contracts declare different error sets.
const codesMetaKey = "m1_error_codes"

// register wires an operation, recording its intended error codes so humaspec.Prune can strip the
// boilerplate responses huma injects. It is a thin, package-specific binding of humaspec.Register to
// this API's metadata key (Go has no generic methods, so the key cannot be carried on a receiver).
func register[I, O any](api huma.API, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	humaspec.Register(api, codesMetaKey, op, handler)
}

// scopeSecurity is the per-operation security block for a required scope. It is both the published
// contract and, via auth.Middleware reading op.Security, the enforcement.
func scopeSecurity(scope auth.Scope) []map[string][]string {
	return []map[string][]string{{operatorScheme: {string(scope)}}}
}
