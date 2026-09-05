package humaerr_test

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// body is a minimal request body used to exercise validation.
type body struct {
	Name string `json:"name" minLength:"1"`
}

// testAPI builds a throwaway huma API with the flat error model installed and a handful of
// operations, each provoking one error path.
func testAPI(t *testing.T) huma.API {
	t.Helper()
	humaerr.Install()

	mux := chi.NewMux()
	api := humachi.New(mux, huma.DefaultConfig("Test", "1.0.0"))

	// Returns a coded failure directly.
	huma.Register(api, huma.Operation{
		OperationID: "conflict", Method: http.MethodGet, Path: "/conflict",
		Errors: []int{http.StatusConflict},
	}, func(ctx context.Context, _ *struct{}) (*struct{}, error) {
		return nil, humaerr.Fail(errs.ErrConflict, "already exists")
	})

	// A coded error must override the status huma is told to use: huma.Error404NotFound would be a
	// 404, but the wrapped ErrConflict is a 409.
	huma.Register(api, huma.Operation{
		OperationID: "code-wins", Method: http.MethodGet, Path: "/code-wins",
		Errors: []int{http.StatusConflict, http.StatusNotFound},
	}, func(ctx context.Context, _ *struct{}) (*struct{}, error) {
		return nil, huma.Error404NotFound("not found", fmt.Errorf("dup: %w", errs.ErrConflict))
	})

	// An error with no code is an internal_error / 500.
	huma.Register(api, huma.Operation{
		OperationID: "uncoded", Method: http.MethodGet, Path: "/uncoded",
	}, func(ctx context.Context, _ *struct{}) (*struct{}, error) {
		return nil, humaerr.FromError(goerrors.New("something broke"))
	})

	// Has a required body, to provoke validation and malformed-body errors.
	huma.Register(api, huma.Operation{
		OperationID: "create", Method: http.MethodPost, Path: "/create",
		Errors: []int{http.StatusUnprocessableEntity},
	}, func(ctx context.Context, _ *struct{ Body body }) (*struct{}, error) {
		return nil, nil
	})

	return api
}

func do(t *testing.T, api huma.API, method, path, reqBody string) (int, string, map[string]any) {
	t.Helper()
	var r *http.Request
	if reqBody == "" {
		r = httptest.NewRequest(method, path, http.NoBody)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(reqBody))
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	api.Adapter().ServeHTTP(w, r)

	var parsed map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &parsed)
	return w.Code, w.Header().Get("Content-Type"), parsed
}

// TestInstalledErrorModelSerializesFlatWithoutTheHTTPStatus: the body is exactly {code, message}
// (plus errors[] when present) — flat, and with no status field duplicating the status line.
func TestInstalledErrorModelSerializesFlatWithoutTheHTTPStatus(t *testing.T) {
	api := testAPI(t)
	status, _, m := do(t, api, http.MethodGet, "/conflict", "")

	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	if m["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", m["code"])
	}
	if m["message"] != "already exists" {
		t.Errorf("message = %v, want \"already exists\"", m["message"])
	}
	for _, forbidden := range []string{"status", "type", "title", "detail"} {
		if _, ok := m[forbidden]; ok {
			t.Errorf("body carries RFC 9457 field %q; the model must be flat: %v", forbidden, m)
		}
	}
}

// TestErrorResponsesAreApplicationJSONNotProblemJSON pins the content type: the override exists to
// avoid application/problem+json.
func TestErrorResponsesAreApplicationJSONNotProblemJSON(t *testing.T) {
	api := testAPI(t)
	_, ct, _ := do(t, api, http.MethodGet, "/conflict", "")

	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if strings.Contains(ct, "problem") {
		t.Errorf("Content-Type = %q, still problem+json", ct)
	}
}

// TestACodeCarryingErrorPicksItsOwnStatusOverHumas: when huma is told 404 but the wrapped error
// carries conflict, the code and its 409 win.
func TestACodeCarryingErrorPicksItsOwnStatusOverHumas(t *testing.T) {
	api := testAPI(t)
	status, _, m := do(t, api, http.MethodGet, "/code-wins", "")

	if status != http.StatusConflict {
		t.Errorf("status = %d, want 409 (the code overrides huma's 404)", status)
	}
	if m["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", m["code"])
	}
}

// TestAMalformedBodyIsReportedAs422ValidationErrorNot400: huma answers a broken JSON body with 400,
// which the contract never declares; the override rewrites it to 422/validation_error.
func TestAMalformedBodyIsReportedAs422ValidationErrorNot400(t *testing.T) {
	api := testAPI(t)
	status, _, m := do(t, api, http.MethodPost, "/create", "{ this is not json")

	if status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422 (400 rewritten)", status)
	}
	if m["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", m["code"])
	}
}

// TestAValidationFailurePopulatesErrorsPerField: a semantically invalid body yields 422 with the
// offending field named in errors[].
func TestAValidationFailurePopulatesErrorsPerField(t *testing.T) {
	api := testAPI(t)
	status, _, m := do(t, api, http.MethodPost, "/create", `{"name":""}`)

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", status)
	}
	if m["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", m["code"])
	}
	list, ok := m["errors"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("errors[] is empty; want a per-field entry: %v", m)
	}
	first, _ := list[0].(map[string]any)
	if field, _ := first["field"].(string); !strings.Contains(field, "name") {
		t.Errorf("errors[0].field = %v, want it to name \"name\"", first["field"])
	}
}

// TestAnErrorWithNoCodeBecomesInternalErrorAnd500: an unmapped error must not leak as some other
// status; it is an internal_error and a 500.
func TestAnErrorWithNoCodeBecomesInternalErrorAnd500(t *testing.T) {
	api := testAPI(t)
	status, _, m := do(t, api, http.MethodGet, "/uncoded", "")

	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if m["code"] != "internal_error" {
		t.Errorf("code = %v, want internal_error", m["code"])
	}
}

// TestFromErrorHidesInternalDetailOnA5xx pins the boundary: a coded-internal error wrapping
// infrastructure detail (as the postgres layer produces via errors.Join(pgErr, ErrInternal)) must
// reach the client as a static message — never the underlying string with host/table/constraint
// names. Regression guard for the info-disclosure fix.
func TestFromErrorHidesInternalDetailOnA5xx(t *testing.T) {
	leak := "create connector: failed to connect to host=db.internal user=gateway dbname=gw: refused"
	wrapped := fmt.Errorf("%s: %w", leak, goerrors.Join(goerrors.New(leak), errs.ErrInternal))

	model := humaerr.FromError(wrapped)
	if model.GetStatus() != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", model.GetStatus())
	}

	body, _ := json.Marshal(model)
	for _, secret := range []string{"host=", "db.internal", "gateway", "connect", "refused"} {
		if strings.Contains(string(body), secret) {
			t.Errorf("500 body leaked internal detail %q: %s", secret, body)
		}
	}
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	if m["code"] != "internal_error" || m["message"] != "internal error" {
		t.Errorf("body = %v, want {code:internal_error, message:\"internal error\"}", m)
	}
}

// TestFromErrorKeepsClient4xxContext: a 4xx keeps its wrapped operation label (benign) so operators
// still see which operation conflicted.
func TestFromErrorKeepsClient4xxContext(t *testing.T) {
	wrapped := fmt.Errorf("create credential: %w", errs.ErrConflict)
	model := humaerr.FromError(wrapped)

	if model.GetStatus() != http.StatusConflict {
		t.Fatalf("status = %d, want 409", model.GetStatus())
	}
	if !strings.Contains(model.Error(), "create credential") {
		t.Errorf("4xx message = %q, want it to keep the operation label", model.Error())
	}
}

// TestACodeWithNoHTTPSurfaceIsRenderedAsInternalError (step-260h): the contracts' Error.code enum is
// the HTTP-surfaced catalogue, so a code without an HTTP status must never reach a body — whichever
// of the three constructors it comes through.
func TestACodeWithNoHTTPSurfaceIsRenderedAsInternalError(t *testing.T) {
	humaerr.Install()
	for name, model := range map[string]huma.StatusError{
		"Fail":      humaerr.Fail(errs.ErrSubmitFailed, "smsc said no"),
		"FromError": humaerr.FromError(fmt.Errorf("bind: %w", errs.ErrMaxSessionsExceeded)),
		"huma":      huma.Error404NotFound("not found", errs.ErrSubmitFailed),
	} {
		if model.GetStatus() != http.StatusInternalServerError {
			t.Errorf("%s: status = %d, want 500", name, model.GetStatus())
		}
		body, _ := json.Marshal(model)
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if m["code"] != "internal_error" {
			t.Errorf("%s: code = %v, want internal_error: a code outside the contract's enum reached the body", name, m["code"])
		}
	}
}
