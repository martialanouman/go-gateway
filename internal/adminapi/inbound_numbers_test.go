package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// seedInboundNumber creates one number through the fake and returns its id, so assign/update tests
// have a real target.
func seedInboundNumber(t *testing.T, api http.Handler) string {
	t.Helper()
	w := httptest.NewRecorder()
	body := `{"address":"36000","number_type":"shortcode","country_code":"FR"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/inbound-numbers", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed create status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	id, _ := got["id"].(string)
	if id == "" {
		t.Fatalf("seed returned no id: %s", w.Body)
	}
	return id
}

// TestCreateInboundNumberReturns201 walks the happy path down to the fake, checking the DDL-default
// status is echoed and account_id is absent (shared).
func TestCreateInboundNumberReturns201(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: newFakeInboundNumberStore()})

	w := httptest.NewRecorder()
	body := `{"address":"36000","number_type":"shortcode","country_code":"FR"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/inbound-numbers", body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
	if v, ok := got["account_id"]; ok && v != nil {
		t.Errorf("account_id = %v, want absent/null (shared)", v)
	}
}

// TestCreateInboundNumberDuplicateIs409: the store's ErrConflict (an address+country UNIQUE
// violation) surfaces as a 409 with the flat error code.
func TestCreateInboundNumberDuplicateIs409(t *testing.T) {
	store := newFakeInboundNumberStore()
	store.createErr = errs.ErrConflict
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: store})

	w := httptest.NewRecorder()
	body := `{"address":"36000","number_type":"shortcode","country_code":"FR"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/inbound-numbers", body))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestCreateInboundNumberInvalidUUIDIs422: a malformed connector_id is rejected as a 422 before the
// store is touched (parseIDPtr).
func TestCreateInboundNumberInvalidUUIDIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: newFakeInboundNumberStore()})

	w := httptest.NewRecorder()
	body := `{"address":"36000","number_type":"shortcode","country_code":"FR","connector_id":"not-a-uuid"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/inbound-numbers", body))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestAssignInboundNumberDedicatesThenClearsToShared is the feature's signature behavior through the
// full HTTP stack: a UUID account_id dedicates the number, and an explicit JSON null clears it back
// to shared. The null path is what the repo-level test cannot exercise — it proves huma accepts the
// required-and-nullable body and the handler passes nil through.
func TestAssignInboundNumberDedicatesThenClearsToShared(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: newFakeInboundNumberStore()})
	id := seedInboundNumber(t, api)
	acct := uuid.New().String()

	// Dedicate.
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/inbound-numbers/"+id+"/assign",
		`{"account_id":"`+acct+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("dedicate status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["account_id"] != acct {
		t.Errorf("account_id = %v, want %s after dedication", got["account_id"], acct)
	}

	// Clear to shared with an explicit null.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/inbound-numbers/"+id+"/assign",
		`{"account_id":null}`))
	if w.Code != http.StatusOK {
		t.Fatalf("clear status = %d, want 200; body=%s", w.Code, w.Body)
	}
	got = map[string]any{}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if v, present := got["account_id"]; present && v != nil {
		t.Errorf("account_id = %v, want absent/null (shared) after clearing", v)
	}
}

// TestAssignInboundNumberMissingBodyIs422: account_id is required, so an empty body is rejected.
func TestAssignInboundNumberMissingBodyIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: newFakeInboundNumberStore()})
	id := seedInboundNumber(t, api)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/inbound-numbers/"+id+"/assign", `{}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestAssignInboundNumberUnknownIs404: assigning an unknown id is a 404 (the code the harmonized
// contract now declares).
func TestAssignInboundNumberUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundNumbers: newFakeInboundNumberStore()})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/inbound-numbers/"+uuid.New().String()+"/assign",
		`{"account_id":null}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}
