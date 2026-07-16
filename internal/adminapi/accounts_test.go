package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestCreateAccountReturns201 exercises the happy path with the customer_id in the body.
func TestCreateAccountReturns201(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Accounts: newFakeAccountStore()})

	body := `{"customer_id":"` + uuid.New().String() + `","name":"app-1"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/smpp-accounts", body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "app-1" {
		t.Errorf("name = %v, want app-1", got["name"])
	}
	if got["allowed_bind_types"] != "trx" {
		t.Errorf("allowed_bind_types = %v, want trx (the default)", got["allowed_bind_types"])
	}
}

// TestCreateAccountDuplicateNameIs409: the store's ErrConflict (a smpp_accounts_name_uq violation)
// surfaces as a 409.
func TestCreateAccountDuplicateNameIs409(t *testing.T) {
	store := newFakeAccountStore()
	store.createErr = errs.ErrConflict
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store})

	body := `{"customer_id":"` + uuid.New().String() + `","name":"dup"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/smpp-accounts", body))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestSetChannelsBothFalseIs422 checks the handler's pre-validation of the channel rule, naming the
// field for a clean 422.
func TestSetChannelsBothFalseIs422(t *testing.T) {
	store := newFakeAccountStore()
	created, _ := store.Create(t.Context(), newAccountInput())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch,
		"/v1/admin/smpp-accounts/"+created.ID.String()+"/channels",
		`{"smpp_enabled":false,"rest_enabled":false}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "validation_error" {
		t.Errorf("code = %v, want validation_error", got["code"])
	}
}

// TestSetSessionLimitsUpdatesTheAccount checks the session-limits endpoint round-trips.
func TestSetSessionLimitsUpdatesTheAccount(t *testing.T) {
	store := newFakeAccountStore()
	created, _ := store.Create(t.Context(), newAccountInput())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch,
		"/v1/admin/smpp-accounts/"+created.ID.String()+"/session-limits",
		`{"max_sessions":5,"allowed_bind_types":"tx"}`))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["max_sessions"] != float64(5) {
		t.Errorf("max_sessions = %v, want 5", got["max_sessions"])
	}
	if got["allowed_bind_types"] != "tx" {
		t.Errorf("allowed_bind_types = %v, want tx", got["allowed_bind_types"])
	}
}

func newAccountInput() cp.NewAccount { return cp.NewAccount{CustomerID: uuid.New(), Name: "app"} }
