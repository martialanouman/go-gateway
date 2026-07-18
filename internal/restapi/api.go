package restapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/platform/humaspec"
)

// serverURL is the contract's declared server. Mounting chi at /v1 and registering operations at
// /messages, /messages/{id} and /health makes the registered paths compare literally against
// api/openapi-public.yaml, whose server carries the /v1 prefix.
const serverURL = "https://api.gateway.example.com/v1"

// server holds the handler dependencies.
type server struct {
	deps Deps
}

func (s *server) now() time.Time {
	if s.deps.Now != nil {
		return s.deps.Now()
	}
	return time.Now().UTC()
}

// New builds the public REST API: a chi router serving the M2 operations of api/openapi-public.yaml
// under /v1, and the huma API that generated them (returned so the contract test can read the spec
// without opening a socket).
//
// A nil Deps.Principals is tolerated: the auth middleware is then skipped, which is the spec-only
// construction the contract test uses.
func New(deps Deps) (*chi.Mux, huma.API) {
	humaerr.Install()

	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	root := chi.NewMux()
	// RealIP is deliberately omitted: it trusts client-supplied forwarding headers and is a spoofing
	// vector (chi GHSA-3fxj-6jh8-hvhx). The ingress sets the peer address.
	root.Use(chimiddleware.RequestID, chimiddleware.Recoverer)

	v1 := chi.NewMux()
	root.Mount("/v1", v1)

	cfg := huma.DefaultConfig("Public API", "1.0.0")
	cfg.Servers = []*huma.Server{{URL: serverURL, Description: "Production"}}
	cfg.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		bearerScheme: {Type: "http", Scheme: "bearer", BearerFormat: "API-Key"},
	}
	cfg.Security = []map[string][]string{{bearerScheme: {}}}

	api := humachi.New(v1, cfg)

	if deps.Principals != nil {
		api.UseMiddleware(apiKeyMiddleware(api, deps.Principals))
	}

	srv := &server{deps: deps}
	registerMessages(api, srv)
	registerHealth(api, srv)

	humaspec.Prune(api, codesMetaKey)

	return root, api
}

// bearerSecurity is the per-operation security block requiring the API key.
func bearerSecurity() []map[string][]string {
	return []map[string][]string{{bearerScheme: {}}}
}

func registerMessages(api huma.API, s *server) {
	register(api, huma.Operation{
		OperationID: "submit-messages",
		Method:      http.MethodPost,
		Path:        "/messages",
		Summary:     "Submit MT SMS (single or batch)",
		Tags:        []string{"Messages"},
		Security:    bearerSecurity(),
		// M2 serves single submissions; batch (BatchAcceptResult) arrives later.
		DefaultStatus: http.StatusAccepted,
		// 500 (encode failure) and 503 (ingest log unavailable) are real handler outcomes, so they
		// belong in the served contract — a client generated from the spec must know to retry them.
		Errors: []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable},
	}, s.submit)

	register(api, huma.Operation{
		OperationID: "get-message",
		Method:      http.MethodGet,
		Path:        "/messages/{id}",
		Summary:     "Get message status",
		Tags:        []string{"Messages"},
		Security:    bearerSecurity(),
		// 403 (suspended account / disabled REST channel, from the shared apiKeyMiddleware), 422
		// (malformed id) and 500 (CDR read failure) are all real outcomes of an authenticated get;
		// declare them so the served contract matches what the endpoint can return.
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity, http.StatusInternalServerError},
	}, s.getMessage)
}

func registerHealth(api huma.API, s *server) {
	register(api, huma.Operation{
		OperationID: "health",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Liveness/health of the public API",
		Tags:        []string{"System"},
		// Public: no security requirement, overriding the global default.
		Security: []map[string][]string{},
	}, s.health)
}

// codesMetaKey is the metadata key under which register stashes an operation's intended error codes
// for humaspec.Prune. It is per-API: the public and admin contracts declare different error sets.
const codesMetaKey = "m2_error_codes"

// register wires an operation, recording its intended error codes so humaspec.Prune can strip the
// boilerplate responses huma injects. It is a thin, package-specific binding of humaspec.Register to
// this API's metadata key (Go has no generic methods, so the key cannot be carried on a receiver).
func register[I, O any](api huma.API, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	humaspec.Register(api, codesMetaKey, op, handler)
}
