package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
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

// TestUpdateRouteRejectsShrinkingANonStaticRouteBelowTwoTargets pins the fix for the review
// finding: update-route must apply the same strategy invariant as create-route, against the
// effective post-update state. A PATCH that empties a weighted route's targets is a 422, not a
// silent persist of an invalid route.
func TestUpdateRouteRejectsShrinkingANonStaticRouteBelowTwoTargets(t *testing.T) {
	store := newFakeRouteStore()
	created, _ := store.Create(t.Context(), cp.NewRoute{
		Name:                 "balanced",
		DistributionStrategy: cp.DistributionWeighted,
		Targets: []cp.RouteTarget{
			{ConnectorID: uuid.New(), Weight: 1},
			{ConnectorID: uuid.New(), Weight: 1},
		},
	})
	api := newTestAPIWith(t, adminapi.Deps{Routes: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routes/"+created.ID.String(), `{"targets":[]}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestUpdateRouteAllowsAnUnrelatedFieldChange: a PATCH that touches only the name leaves the
// existing (valid) targets untouched and succeeds — the effective-state check must not false-reject.
func TestUpdateRouteAllowsAnUnrelatedFieldChange(t *testing.T) {
	store := newFakeRouteStore()
	created, _ := store.Create(t.Context(), cp.NewRoute{
		Name:                 "balanced",
		DistributionStrategy: cp.DistributionWeighted,
		Targets: []cp.RouteTarget{
			{ConnectorID: uuid.New(), Weight: 1},
			{ConnectorID: uuid.New(), Weight: 1},
		},
	})
	api := newTestAPIWith(t, adminapi.Deps{Routes: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/routes/"+created.ID.String(), `{"name":"renamed"}`))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
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
