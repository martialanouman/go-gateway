package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

const scheme = "OperatorBearer"

// middlewareAPI builds a huma API guarded by the auth middleware, with three operations: a
// read (admin:read), a write (admin:write), and one that echoes the authenticated subject so a
// test can confirm the principal reached the handler.
func middlewareAPI(t *testing.T, v auth.TokenVerifier) huma.API {
	t.Helper()
	humaerr.Install()

	mux := chi.NewMux()
	api := humachi.New(mux, huma.DefaultConfig("Test", "1.0.0"))
	api.UseMiddleware(auth.Middleware(api, v, scheme))

	huma.Register(api, huma.Operation{
		OperationID: "read", Method: http.MethodGet, Path: "/read",
		Security: []map[string][]string{{scheme: {string(auth.ScopeAdminRead)}}},
	}, func(ctx context.Context, _ *struct{}) (*struct {
		Body struct {
			Subject string `json:"subject"`
		}
	}, error) {
		out := &struct {
			Body struct {
				Subject string `json:"subject"`
			}
		}{}
		if p, ok := auth.PrincipalFrom(ctx); ok {
			out.Body.Subject = p.Subject
		}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "write", Method: http.MethodPost, Path: "/write",
		Security: []map[string][]string{{scheme: {string(auth.ScopeAdminWrite)}}},
	}, func(ctx context.Context, _ *struct{}) (*struct{}, error) {
		return nil, nil
	})

	return api
}

func request(t *testing.T, api huma.API, method, path, authHeader string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(method, path, http.NoBody)
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	api.Adapter().ServeHTTP(w, r)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

// TestMissingBearerIs401: an operation that declares a scope rejects an unauthenticated request
// with 401 unauthenticated.
func TestMissingBearerIs401(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"reader:admin:read"})
	api := middlewareAPI(t, v)

	status, m := request(t, api, http.MethodGet, "/read", "")
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
	if m["code"] != "unauthenticated" {
		t.Errorf("code = %v, want unauthenticated", m["code"])
	}
}

// TestUnknownTokenIs401: a token the verifier does not recognise is also 401.
func TestUnknownTokenIs401(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"reader:admin:read"})
	api := middlewareAPI(t, v)

	status, m := request(t, api, http.MethodGet, "/read", "Bearer nope")
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
	if m["code"] != "unauthenticated" {
		t.Errorf("code = %v, want unauthenticated", m["code"])
	}
}

// TestValidTokenMissingScopeIs403: a recognised operator lacking the operation's scope is
// forbidden_scope, not unauthenticated — the distinction the contract's 401 vs 403 encodes.
func TestValidTokenMissingScopeIs403(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"reader:admin:read"})
	api := middlewareAPI(t, v)

	status, m := request(t, api, http.MethodPost, "/write", "Bearer reader")
	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", status)
	}
	if m["code"] != "forbidden_scope" {
		t.Errorf("code = %v, want forbidden_scope", m["code"])
	}
}

// TestAuthorizedRequestReachesTheHandlerWithItsPrincipal: the right token and scope pass, and the
// principal is on the context for the handler.
func TestAuthorizedRequestReachesTheHandlerWithItsPrincipal(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"admin:admin:read|admin:write"})
	api := middlewareAPI(t, v)

	status, m := request(t, api, http.MethodGet, "/read", "Bearer admin")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if m["subject"] != "admin" {
		t.Errorf("subject = %v, want admin — the principal did not reach the handler", m["subject"])
	}
}

// TestCaseInsensitiveBearerScheme: "bearer" and "Bearer" both work, per RFC 7235.
func TestCaseInsensitiveBearerScheme(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"admin:admin:read"})
	api := middlewareAPI(t, v)

	status, _ := request(t, api, http.MethodGet, "/read", "bearer admin")
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200 for lowercase scheme", status)
	}
}
