package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
)

// TestCreateStaticRouteRequiresATargetConnector: a static route without target_connector_id is a
// 422 naming the field, not a translated DB check-violation.
func TestCreateStaticRouteRequiresATargetConnector(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Routes: newFakeRouteStore()})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routes",
		`{"name":"r","distribution_strategy":"static"}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestCreateNonStaticRouteNeedsTwoTargets: a non-static route with fewer than two targets is a 422.
func TestCreateNonStaticRouteNeedsTwoTargets(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Routes: newFakeRouteStore()})

	body := `{"name":"r","distribution_strategy":"round_robin","targets":[{"connector_id":"` + uuid.New().String() + `"}]}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routes", body))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestCreateStaticRouteSucceeds: with a target connector, a static route is created.
func TestCreateStaticRouteSucceeds(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Routes: newFakeRouteStore()})

	body := `{"name":"primary","distribution_strategy":"static","target_connector_id":"` + uuid.New().String() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/routes", body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "primary" {
		t.Errorf("name = %v, want primary", got["name"])
	}
	if got["priority"] != float64(100) {
		t.Errorf("priority = %v, want 100 (the default)", got["priority"])
	}
}
