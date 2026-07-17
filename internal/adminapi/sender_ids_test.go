package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/martialanouman/go-gateway/internal/adminapi"
)

// TestCreateSenderIDStartsPending: a new sender ID begins pending carrier approval.
func TestCreateSenderIDStartsPending(t *testing.T) {
	store := newFakeCustomerStore()
	customer, _ := store.Create(t.Context(), newCustomerInput("Owner"))
	api := newTestAPIWith(t, adminapi.Deps{Customers: store, SenderIDs: newFakeSenderIDStore()})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost,
		"/v1/admin/customers/"+customer.ID.String()+"/sender-ids", `{"address":"ACME"}`))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "pending_carrier_approval" {
		t.Errorf("status = %v, want pending_carrier_approval", got["status"])
	}
	if got["address"] != "ACME" {
		t.Errorf("address = %v, want ACME", got["address"])
	}
}

// TestListSenderIDsForUnknownCustomerIs404: with no sender id to miss, an unknown customer is a 404.
func TestListSenderIDsForUnknownCustomerIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Customers: newFakeCustomerStore(), SenderIDs: newFakeSenderIDStore()})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet,
		"/v1/admin/customers/00000000-0000-7000-8000-000000000000/sender-ids", ""))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}
