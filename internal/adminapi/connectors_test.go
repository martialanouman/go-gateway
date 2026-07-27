package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
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
