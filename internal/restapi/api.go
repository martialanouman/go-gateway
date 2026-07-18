package restapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
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

	pruneBoilerplateResponses(api)

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
		Errors:        []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusTooManyRequests},
	}, s.submit)

	register(api, huma.Operation{
		OperationID: "get-message",
		Method:      http.MethodGet,
		Path:        "/messages/{id}",
		Summary:     "Get message status",
		Tags:        []string{"Messages"},
		Security:    bearerSecurity(),
		Errors:      []int{http.StatusUnauthorized, http.StatusNotFound},
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

// The registration helpers below mirror internal/adminapi: they record each operation's intended
// error codes so pruneBoilerplateResponses can strip the extras huma injects, making the served
// spec match the contract exactly for the operations M2 implements.

const codesMetaKey = "m2_error_codes"

func register[I, O any](api huma.API, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	if op.Metadata == nil {
		op.Metadata = map[string]any{}
	}
	op.Metadata[codesMetaKey] = append([]int(nil), op.Errors...)
	huma.Register(api, op, handler)
}

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

func operationsOf(item *huma.PathItem) []*huma.Operation {
	return []*huma.Operation{
		item.Get, item.Post, item.Put, item.Patch, item.Delete,
		item.Head, item.Options, item.Trace,
	}
}
