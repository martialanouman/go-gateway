package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

const operatorToken = "operator-token-for-tests"

// newTestAPI builds the Admin API wired to the given customer store (and no others), with a real
// static verifier that grants both admin scopes to operatorToken.
func newTestAPI(t *testing.T, customers adminapi.CustomerStore) http.Handler {
	t.Helper()
	return newTestAPIWith(t, adminapi.Deps{Customers: customers})
}

// newTestAPIWith builds the Admin API from deps, filling in the operator-token verifier.
func newTestAPIWith(t *testing.T, deps adminapi.Deps) http.Handler {
	t.Helper()
	return newTestAPIWithScopes(t, deps, "admin:read|admin:write")
}

// newTestAPIWithScopes builds the API granting operatorToken exactly the given scopes, for the endpoints
// whose behaviour depends on which ones the caller holds.
func newTestAPIWithScopes(t *testing.T, deps adminapi.Deps, scopes string) http.Handler {
	t.Helper()
	verifier, err := auth.NewStaticVerifier([]string{operatorToken + ":" + scopes})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	deps.Verifier = verifier
	mux, _ := adminapi.New(deps)
	return mux
}

func authed(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, http.NoBody)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Authorization", "Bearer "+operatorToken)
	return r
}

func newCustomerInput(name string) cp.NewCustomer { return cp.NewCustomer{Name: name} }

// TestCreateCustomerReturns201WithTheCreatedCustomer exercises the happy path through the real HTTP
// stack down to the fake store.
func TestCreateCustomerReturns201WithTheCreatedCustomer(t *testing.T) {
	store := newFakeCustomerStore()
	api := newTestAPI(t, store)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers", `{"name":"Acme"}`))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "Acme" {
		t.Errorf("name = %v, want Acme", got["name"])
	}
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
}

// TestCreateCustomerConflictBecomes409 proves a store ErrConflict surfaces as the flat
// {"code":"conflict"} with a 409 — the mapping M1's acceptance criteria rely on.
func TestCreateCustomerConflictBecomes409(t *testing.T) {
	store := newFakeCustomerStore()
	store.createErr = errs.ErrConflict
	api := newTestAPI(t, store)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers", `{"name":"Dup"}`))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestGetMissingCustomerIs404 checks the not-found path returns the flat model with a 404.
func TestGetMissingCustomerIs404(t *testing.T) {
	api := newTestAPI(t, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/00000000-0000-7000-8000-000000000000", ""))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "not_found" {
		t.Errorf("code = %v, want not_found", got["code"])
	}
}

// TestInvalidBodyIsFlat422 checks a validation failure carries the flat error shape with a
// per-field entry.
func TestInvalidBodyIsFlat422(t *testing.T) {
	api := newTestAPI(t, newFakeCustomerStore())

	// billing_mode has an enum; an unknown value fails huma validation.
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers", `{"name":"X","billing_mode":"bogus"}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
	if _, ok := got["errors"].([]any); !ok {
		t.Errorf("errors[] missing on a validation failure: %v", got)
	}
}

// TestListAlwaysCarriesThePageEnvelope guards a subtle Huma bug: an embedded UNEXPORTED page-meta
// type is dropped from the serialized body, so has_more and next_cursor silently vanish. The
// contract requires has_more on every page; this pins that it is present.
func TestListAlwaysCarriesThePageEnvelope(t *testing.T) {
	store := newFakeCustomerStore()
	_, _ = store.Create(t.Context(), newCustomerInput("A"))
	api := newTestAPI(t, store)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	// has_more is required by the contract's PageMeta and must always appear; its absence was the
	// symptom of the dropped-embedded-struct bug. next_cursor is optional (absent on the last page).
	if _, ok := got["has_more"]; !ok {
		t.Errorf("response is missing has_more; the page envelope was dropped: %s", w.Body)
	}
	if _, ok := got["data"]; !ok {
		t.Errorf("response is missing data: %s", w.Body)
	}
}

// TestMissingTokenIs401 confirms the middleware guards the resource.
func TestMissingTokenIs401(t *testing.T) {
	api := newTestAPI(t, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/admin/customers", http.NoBody))

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", w.Code, w.Body)
	}
}

// TestSuspendCustomerReturnsSuspended checks the suspend endpoint flips the status.
func TestSuspendCustomerReturnsSuspended(t *testing.T) {
	store := newFakeCustomerStore()
	created, _ := store.Create(t.Context(), newCustomerInput("ToSuspend"))
	api := newTestAPI(t, store)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+created.ID.String()+"/suspend", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "suspended" {
		t.Errorf("status = %v, want suspended", got["status"])
	}
}
