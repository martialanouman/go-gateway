package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func seedCustomerAccount(t *testing.T, store *fakeAccountStore, customerID uuid.UUID) cp.Account {
	t.Helper()
	a, err := store.Create(context.Background(), cp.NewAccount{CustomerID: customerID, Name: "acct-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return a
}

func call(t *testing.T, api http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, method, "/v1/admin/"+path, body))
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestSetSenderIDPolicyChangesItWithoutDisconnecting(t *testing.T) {
	store, disc := newFakeAccountStore(), &fakeDisconnector{}
	a := seedCustomerAccount(t, store, uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store, Disconnector: disc})

	code, body := call(t, api, http.MethodPatch, "smpp-accounts/"+a.ID.String()+"/sender-id-policy", `{"sender_id_policy":"disabled"}`)
	if code != http.StatusOK || body["sender_id_policy"] != "disabled" {
		t.Fatalf("status = %d policy = %v, want 200 disabled", code, body["sender_id_policy"])
	}
	if got, _ := store.Get(context.Background(), a.ID); got.SenderIDPolicy != cp.SenderIDPolicyDisabled {
		t.Fatalf("stored policy = %s, want disabled", got.SenderIDPolicy)
	}
	if calls := disc.accountCalls(); len(calls) != 0 {
		t.Fatalf("disconnects = %+v, want none: the policy is checked per message", calls)
	}
}

func TestSetSmppOpsDisconnectsTheAccountSoItRebindsUnderTheNewFlags(t *testing.T) {
	store, disc := newFakeAccountStore(), &fakeDisconnector{}
	a := seedCustomerAccount(t, store, uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store, Disconnector: disc})

	code, body := call(t, api, http.MethodPatch, "smpp-accounts/"+a.ID.String()+"/smpp-ops", `{"cancel_sm_enabled":false}`)
	if code != http.StatusOK || body["cancel_sm_enabled"] != false || body["query_sm_enabled"] != true {
		t.Fatalf("status = %d body = %v, want cancel_sm off and query_sm untouched", code, body)
	}
	calls := disc.accountCalls()
	if len(calls) != 1 || calls[0].id != a.ID || calls[0].reason != "account_smpp_ops_changed" {
		t.Fatalf("disconnects = %+v, want one for %s with account_smpp_ops_changed", calls, a.ID)
	}
}

func TestSetSmppOpsWithNothingToSetIs422AndDisconnectsNobody(t *testing.T) {
	store, disc := newFakeAccountStore(), &fakeDisconnector{}
	a := seedCustomerAccount(t, store, uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store, Disconnector: disc})

	if code, _ := call(t, api, http.MethodPatch, "smpp-accounts/"+a.ID.String()+"/smpp-ops", `{}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", code)
	}
	if calls := disc.accountCalls(); len(calls) != 0 {
		t.Fatalf("disconnects = %+v, want none", calls)
	}
}

func TestSuspendAccountFellsItsBinds(t *testing.T) {
	store, disc := newFakeAccountStore(), &fakeDisconnector{}
	a := seedCustomerAccount(t, store, uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Accounts: store, Disconnector: disc})

	code, body := call(t, api, http.MethodPost, "smpp-accounts/"+a.ID.String()+"/suspend", "")
	if code != http.StatusOK || body["status"] != "suspended" {
		t.Fatalf("status = %d account status = %v, want 200 suspended", code, body["status"])
	}
	calls := disc.accountCalls()
	if len(calls) != 1 || calls[0].id != a.ID || calls[0].reason != "account_suspended" {
		t.Fatalf("disconnects = %+v, want one for %s with account_suspended", calls, a.ID)
	}
	if code, _ := call(t, api, http.MethodPost, "smpp-accounts/"+uuid.NewString()+"/suspend", ""); code != http.StatusNotFound {
		t.Fatalf("unknown account: status = %d, want 404", code)
	}
}

func TestListCustomerAccountsReturnsOnlyThatCustomersAccounts(t *testing.T) {
	customers, accounts := newFakeCustomerStore(), newFakeAccountStore()
	mine, err := customers.Create(context.Background(), cp.NewCustomer{Name: "mine"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	first, second := seedCustomerAccount(t, accounts, mine.ID), seedCustomerAccount(t, accounts, mine.ID)
	seedCustomerAccount(t, accounts, uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Customers: customers, Accounts: accounts})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+mine.ID.String()+"/smpp-accounts", ""))
	var got []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK {
		t.Fatalf("status = %d err = %v body = %s, want 200 and an array", w.Code, err, w.Body)
	}
	ids := map[string]bool{}
	for _, a := range got {
		ids[a.ID] = true
	}
	if len(got) != 2 || !ids[first.ID.String()] || !ids[second.ID.String()] {
		t.Fatalf("accounts = %v, want exactly %s and %s", ids, first.ID, second.ID)
	}
	if code, _ := call(t, api, http.MethodGet, "customers/"+uuid.NewString()+"/smpp-accounts", ""); code != http.StatusNotFound {
		t.Fatalf("unknown customer: status = %d, want 404, not an empty list", code)
	}
}
