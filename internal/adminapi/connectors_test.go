package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestCreateConnectorNeverReturnsThePassword: the password is write-only; the 201 body must not echo
// it back under any key.
func TestCreateConnectorNeverReturnsThePassword(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Connectors: newFakeConnectorStore()})

	body := `{"name":"smsc-1","host":"smsc.example","port":2775,"bind_type":"trx","system_id":"sys","password":"s3cr3t-canary"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors", body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "s3cr3t-canary") || strings.Contains(w.Body.String(), "\"password\"") {
		t.Errorf("connector response leaked the password: %s", w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "smsc-1" {
		t.Errorf("name = %v, want smsc-1", got["name"])
	}
}

// TestCreateConnectorDuplicateNameIs409: the store's ErrConflict (a name UNIQUE violation) surfaces
// as a 409.
func TestCreateConnectorDuplicateNameIs409(t *testing.T) {
	store := newFakeConnectorStore()
	store.createErr = errs.ErrConflict
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store})

	body := `{"name":"dup","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"p"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors", body))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestUpdateConnectorThroughputBelowRateLimitIs422: a connector's throughput_limit_per_sec is its hard
// ceiling and must stay at or above its operational rate_limit; lowering it below rejects with 422.
func TestUpdateConnectorThroughputBelowRateLimitIs422(t *testing.T) {
	store := newFakeConnectorStore()
	id := uuid.New()
	store.byID[id] = cp.Connector{ID: id, Name: "smsc", Status: cp.ConnectorActive}
	maxPerSec := 100
	store.rateLimit[id] = cp.RateLimit{MaxPerSec: &maxPerSec}
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id.String(), `{"throughput_limit_per_sec":50}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	if !strings.Contains(w.Body.String(), "throughput_limit_per_sec") {
		t.Errorf("422 body should name the offending field: %s", w.Body)
	}

	// At or above the operational limit is accepted.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id.String(), `{"throughput_limit_per_sec":100}`))
	if w.Code != http.StatusOK {
		t.Fatalf("throughput == rate_limit should be accepted, got %d; body=%s", w.Code, w.Body)
	}
}

// TestGetConnectorStatusExposesBothStatesDistinctly: the response carries per-bind link_status AND
// breaker_state separately (never conflated), plus the connector-wide breaker aggregate.
func TestGetConnectorStatusExposesBothStatesDistinctly(t *testing.T) {
	store := newFakeConnectorStore()
	id := uuid.New()
	store.byID[id] = cp.Connector{ID: id, Name: "smsc", Status: cp.ConnectorActive}
	control := newFakeConnectorControl()
	control.statusByID[id] = status.Connector{
		ConnectorID:  id,
		BreakerState: "open",
		Binds: []status.Bind{
			{PodID: "pod-a", BindIndex: 0, LinkStatus: "up", BreakerState: "open", InFlight: 3},
			{PodID: "pod-a", BindIndex: 1, LinkStatus: "reconnecting", BreakerState: "closed"},
		},
	}
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, ConnectorControl: control})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/connectors/"+id.String()+"/status", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got struct {
		BreakerState string `json:"breaker_state"`
		Binds        []struct {
			LinkStatus   string `json:"link_status"`
			BreakerState string `json:"breaker_state"`
		} `json:"binds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.BreakerState != "open" || len(got.Binds) != 2 {
		t.Fatalf("status = %+v, want breaker open + 2 binds", got)
	}
	// bind 0: link up but breaker open — the two are distinct, not conflated.
	if got.Binds[0].LinkStatus != "up" || got.Binds[0].BreakerState != "open" {
		t.Errorf("bind 0 = %+v, want link up / breaker open (distinct)", got.Binds[0])
	}
	if got.Binds[1].LinkStatus != "reconnecting" || got.Binds[1].BreakerState != "closed" {
		t.Errorf("bind 1 = %+v, want link reconnecting / breaker closed", got.Binds[1])
	}
}

// TestRebindSignalsReconfigure: rebind returns 202 and bumps the reconfigure generation.
func TestRebindSignalsReconfigure(t *testing.T) {
	store := newFakeConnectorStore()
	id := uuid.New()
	store.byID[id] = cp.Connector{ID: id, Name: "smsc", Status: cp.ConnectorActive}
	control := newFakeConnectorControl()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, ConnectorControl: control})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors/"+id.String()+"/rebind", ""))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body)
	}
	if control.reconfigs != 1 {
		t.Errorf("reconfigure signals = %d, want 1", control.reconfigs)
	}
}

// TestRebindUnknownConnectorIs404.
func TestRebindUnknownConnectorIs404(t *testing.T) {
	control := newFakeConnectorControl()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: newFakeConnectorStore(), ConnectorControl: control})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors/"+uuid.New().String()+"/rebind", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
	if control.reconfigs != 0 {
		t.Errorf("signalled reconfigure for an unknown connector (%d)", control.reconfigs)
	}
}

// TestSetBindPoolPersistsAndSignals: resizing 1→4 persists the new size and signals the pods.
func TestSetBindPoolPersistsAndSignals(t *testing.T) {
	store := newFakeConnectorStore()
	id := uuid.New()
	store.byID[id] = cp.Connector{ID: id, Name: "smsc", Status: cp.ConnectorActive, BindPoolSize: 1}
	control := newFakeConnectorControl()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, ConnectorControl: control})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id.String()+"/bind-pool", `{"bind_pool_size":4}`))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if store.byID[id].BindPoolSize != 4 {
		t.Errorf("persisted bind_pool_size = %d, want 4", store.byID[id].BindPoolSize)
	}
	if control.reconfigs != 1 {
		t.Errorf("reconfigure signals = %d, want 1", control.reconfigs)
	}
}

// TestSetReconnectPolicyPersistsAndSignals.
func TestSetReconnectPolicyPersistsAndSignals(t *testing.T) {
	store := newFakeConnectorStore()
	id := uuid.New()
	store.byID[id] = cp.Connector{ID: id, Name: "smsc", Status: cp.ConnectorActive}
	control := newFakeConnectorControl()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store, ConnectorControl: control})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id.String()+"/reconnect-policy",
		`{"auto_reconnect_enabled":true,"reconnect_max_attempts":5}`))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if !store.byID[id].AutoReconnectEnabled {
		t.Error("auto_reconnect_enabled not persisted")
	}
	if control.reconfigs != 1 {
		t.Errorf("reconfigure signals = %d, want 1", control.reconfigs)
	}
}

// TestConnectorPriorityFlagDefaultIsWritable: the pool sends it for a message of effective priority 0
// (ADR-0020 §3), so create and update must reach the store with it, and the SMPP range bounds it.
func TestConnectorPriorityFlagDefaultIsWritable(t *testing.T) {
	store := newFakeConnectorStore()
	api := newTestAPIWith(t, adminapi.Deps{Connectors: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-1","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"p","priority_flag_default":2}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}
	if store.created.PriorityFlagDefault == nil || *store.created.PriorityFlagDefault != 2 {
		t.Errorf("created priority_flag_default = %v, want 2", store.created.PriorityFlagDefault)
	}

	var id string
	for k := range store.byID {
		id = k.String()
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id, `{"priority_flag_default":1}`))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if store.patched.PriorityFlagDefault == nil || *store.patched.PriorityFlagDefault != 1 {
		t.Errorf("patched priority_flag_default = %v, want 1", store.patched.PriorityFlagDefault)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+id, `{"priority_flag_default":4}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("update with priority_flag_default 4: status = %d, want 422; body=%s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-2","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"p","priority_flag_default":4}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("create with priority_flag_default 4: status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestConnectorPriorityTierIsBounded: a tier above 2 reserves the connector for a rank no category has, so
// every message routed to it would fail (ADR-0020 §4); the API refuses it.
func TestConnectorPriorityTierIsBounded(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Connectors: newFakeConnectorStore()})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/connectors",
		`{"name":"smsc-1","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"p","priority_tier":3}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("create with priority_tier 3: status = %d, want 422; body=%s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/connectors/"+uuid.NewString(), `{"priority_tier":3}`))
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("update with priority_tier 3: status = %d, want 422; body=%s", w.Code, w.Body)
	}
}
