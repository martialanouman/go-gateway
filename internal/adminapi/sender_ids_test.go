package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
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

type failingMismatchCounter struct{}

func (failingMismatchCounter) RecentCategoryMismatches(context.Context, []cp.SenderAddress) ([]int, error) {
	return nil, errors.New("redis down")
}

// TestSenderIDMismatchCountIsNullWhenTheCounterCannotBeRead: an unread counter is unknown, not zero, and
// it does not fail the list the operator came for.
func TestSenderIDMismatchCountIsNullWhenTheCounterCannotBeRead(t *testing.T) {
	store := newFakeCustomerStore()
	customer, _ := store.Create(t.Context(), newCustomerInput("Owner"))
	senders := newFakeSenderIDStore()
	_, _ = senders.Create(t.Context(), cp.NewSenderID{CustomerID: customer.ID, Address: "BANK"})
	api := newTestAPIWith(t, adminapi.Deps{Customers: store, SenderIDs: senders, CategoryMismatches: failingMismatchCounter{}})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+customer.ID.String()+"/sender-ids", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var list []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &list)
	if len(list) != 1 {
		t.Fatalf("list = %v, want one sender", list)
	}
	if v, present := list[0]["recent_category_mismatches_24h"]; !present || v != nil {
		t.Fatalf("recent_category_mismatches_24h = %v (present %t), want null", v, present)
	}
}

type countingMismatchCounter struct{ calls [][]cp.SenderAddress }

func (c *countingMismatchCounter) RecentCategoryMismatches(_ context.Context, senders []cp.SenderAddress) ([]int, error) {
	c.calls = append(c.calls, senders)
	return make([]int, len(senders)), nil
}

// TestSenderIDListReadsEveryCounterInOneCall: the list asks the counter once for all its rows, not once
// per row.
func TestSenderIDListReadsEveryCounterInOneCall(t *testing.T) {
	store := newFakeCustomerStore()
	customer, _ := store.Create(t.Context(), newCustomerInput("Owner"))
	senders := newFakeSenderIDStore()
	for _, address := range []string{"BANK", "SHOP", "INFO"} {
		_, _ = senders.Create(t.Context(), cp.NewSenderID{CustomerID: customer.ID, Address: address})
	}
	counter := &countingMismatchCounter{}
	api := newTestAPIWith(t, adminapi.Deps{Customers: store, SenderIDs: senders, CategoryMismatches: counter})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+customer.ID.String()+"/sender-ids", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	if len(counter.calls) != 1 || len(counter.calls[0]) != 3 {
		t.Fatalf("counter calls = %v, want one call for the three senders", counter.calls)
	}
}
