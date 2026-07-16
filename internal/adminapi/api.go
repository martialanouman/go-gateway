package adminapi

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
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

	pruneBoilerplateResponses(api)

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

// pruneBoilerplateResponses narrows each operation's documented responses to exactly the set the
// contract declares: its success status plus the error codes passed in Operation.Errors. Huma
// otherwise injects a 500, a default, and a 422 (for input validation) that the M1 contract does not
// enumerate, and the strict contract test compares the exact set of codes. This makes the served
// spec, the compared spec, and the contract identical. It documents nothing away that matters: a
// handler can still return any of those statuses; they are simply not enumerated, exactly as in the
// contract.
func pruneBoilerplateResponses(api huma.API) {
	for _, item := range api.OpenAPI().Paths {
		for _, op := range operationsOf(item) {
			if op == nil {
				continue
			}
			allowed := declaredCodes(op)
			for code := range op.Responses {
				if !allowed[code] {
					delete(op.Responses, code)
				}
			}
		}
	}
}

// declaredCodes is the set of response codes an operation is meant to expose: its success status
// (2xx/3xx already present) plus the codes recorded at registration. Reading op.Errors here would
// not work — Huma appends its own 422 and 500 to that slice during registration, so the intended set
// is captured separately in metadata by register().
func declaredCodes(op *huma.Operation) map[string]bool {
	allowed := map[string]bool{}
	for code := range op.Responses {
		if len(code) == 3 && (code[0] == '2' || code[0] == '3') {
			allowed[code] = true
		}
	}
	if codes, ok := op.Metadata[codesMetaKey].([]int); ok {
		for _, code := range codes {
			allowed[strconv.Itoa(code)] = true
		}
	}
	return allowed
}

// codesMetaKey is where register() stashes an operation's intended error codes, before Huma mixes
// its own into Operation.Errors.
const codesMetaKey = "m1_error_codes"

// register wires an operation and records the error codes it is meant to expose, so
// pruneBoilerplateResponses can later strip the codes Huma injects on top.
func register[I, O any](api huma.API, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	if op.Metadata == nil {
		op.Metadata = map[string]any{}
	}
	op.Metadata[codesMetaKey] = append([]int(nil), op.Errors...)
	huma.Register(api, op, handler)
}

// operationsOf returns the operations present on a path item.
func operationsOf(item *huma.PathItem) []*huma.Operation {
	return []*huma.Operation{
		item.Get, item.Post, item.Put, item.Patch, item.Delete,
		item.Head, item.Options, item.Trace,
	}
}

// scopeSecurity is the per-operation security block for a required scope. It is both the published
// contract and, via auth.Middleware reading op.Security, the enforcement.
func scopeSecurity(scope auth.Scope) []map[string][]string {
	return []map[string][]string{{operatorScheme: {string(scope)}}}
}
