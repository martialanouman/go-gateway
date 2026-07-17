package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/adminapi"
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
