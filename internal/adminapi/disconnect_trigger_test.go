package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
)

// fakeDisconnector records the force-disconnect orders the handlers emit, and can be scripted to fail
// so a test can assert the mutation still succeeds (best-effort).
type fakeDisconnector struct {
	mu       sync.Mutex
	accounts []disconnectCall
	custs    []disconnectCall
	err      error
}

type disconnectCall struct {
	id     uuid.UUID
	reason string
}

func (f *fakeDisconnector) DisconnectAccount(_ context.Context, accountID uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accounts = append(f.accounts, disconnectCall{accountID, reason})
	return f.err
}

func (f *fakeDisconnector) DisconnectCustomer(_ context.Context, customerID uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.custs = append(f.custs, disconnectCall{customerID, reason})
	return f.err
}

func (f *fakeDisconnector) accountCalls() []disconnectCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]disconnectCall(nil), f.accounts...)
}

func (f *fakeDisconnector) customerCalls() []disconnectCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]disconnectCall(nil), f.custs...)
}

// TestRevokeCredentialTriggersAccountDisconnect pins that revoking a credential force-disconnects the
// account's live binds, with the revocation reason.
func TestRevokeCredentialTriggersAccountDisconnect(t *testing.T) {
	store := newFakeCredentialStore()
	disc := &fakeDisconnector{}
	api := newTestAPIWith(t, adminapi.Deps{Credentials: store, Accounts: newFakeAccountStore(), Disconnector: disc})

	accountID := uuid.New()
	// Seed a credential to revoke.
	path := "/v1/admin/smpp-accounts/" + accountID.String() + "/credentials"
	create := httptest.NewRecorder()
	api.ServeHTTP(create, authed(t, http.MethodPost, path, `{"type":"api_key"}`))
	if create.Code != http.StatusCreated {
		t.Fatalf("seed create status = %d; body=%s", create.Code, create.Body)
	}
	credID := decodeID(t, create.Body.Bytes())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, path+"/"+credID, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d, want 204; body=%s", w.Code, w.Body)
	}

	calls := disc.accountCalls()
	if len(calls) != 1 {
		t.Fatalf("account disconnects = %d, want 1", len(calls))
	}
	if calls[0].id != accountID || calls[0].reason != "credential_revoked" {
		t.Errorf("disconnect = %+v, want {%s credential_revoked}", calls[0], accountID)
	}
}

// TestUpdateCredentialStatusDisabledTriggersDisconnect pins that flipping a credential to a non-active
// status disconnects, while re-activating one does not.
func TestUpdateCredentialStatusDisabledTriggersDisconnect(t *testing.T) {
	store := newFakeCredentialStore()
	disc := &fakeDisconnector{}
	api := newTestAPIWith(t, adminapi.Deps{Credentials: store, Accounts: newFakeAccountStore(), Disconnector: disc})

	accountID := uuid.New()
	path := "/v1/admin/smpp-accounts/" + accountID.String() + "/credentials"
	create := httptest.NewRecorder()
	api.ServeHTTP(create, authed(t, http.MethodPost, path, `{"type":"api_key"}`))
	credID := decodeID(t, create.Body.Bytes())

	// Disable → disconnect.
	dis := httptest.NewRecorder()
	api.ServeHTTP(dis, authed(t, http.MethodPatch, path+"/"+credID, `{"status":"disabled"}`))
	if dis.Code != http.StatusOK {
		t.Fatalf("disable status = %d; body=%s", dis.Code, dis.Body)
	}
	// Re-activate → no further disconnect.
	act := httptest.NewRecorder()
	api.ServeHTTP(act, authed(t, http.MethodPatch, path+"/"+credID, `{"status":"active"}`))
	if act.Code != http.StatusOK {
		t.Fatalf("activate status = %d; body=%s", act.Code, act.Body)
	}

	calls := disc.accountCalls()
	if len(calls) != 1 {
		t.Fatalf("account disconnects = %d, want 1 (only the disable)", len(calls))
	}
	if calls[0].reason != "credential_disabled" {
		t.Errorf("reason = %q, want credential_disabled", calls[0].reason)
	}
}

// TestSuspendCustomerTriggersCustomerDisconnect pins that suspending a customer disconnects all its
// live binds.
func TestSuspendCustomerTriggersCustomerDisconnect(t *testing.T) {
	store := newFakeCustomerStore()
	created, _ := store.Create(t.Context(), newCustomerInput("ToSuspend"))
	disc := &fakeDisconnector{}
	api := newTestAPIWith(t, adminapi.Deps{Customers: store, Disconnector: disc})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+created.ID.String()+"/suspend", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("suspend status = %d; body=%s", w.Code, w.Body)
	}

	calls := disc.customerCalls()
	if len(calls) != 1 || calls[0].id != created.ID || calls[0].reason != "customer_suspended" {
		t.Fatalf("customer disconnects = %+v, want one {%s customer_suspended}", calls, created.ID)
	}
}

// TestDisconnectFailureDoesNotFailTheMutation pins the best-effort contract: a Disconnector error is
// swallowed, so the control-plane change (here a revocation) still succeeds.
func TestDisconnectFailureDoesNotFailTheMutation(t *testing.T) {
	store := newFakeCredentialStore()
	disc := &fakeDisconnector{err: errors.New("session-manager down")}
	api := newTestAPIWith(t, adminapi.Deps{Credentials: store, Accounts: newFakeAccountStore(), Disconnector: disc})

	accountID := uuid.New()
	path := "/v1/admin/smpp-accounts/" + accountID.String() + "/credentials"
	create := httptest.NewRecorder()
	api.ServeHTTP(create, authed(t, http.MethodPost, path, `{"type":"api_key"}`))
	credID := decodeID(t, create.Body.Bytes())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, path+"/"+credID, ""))
	if w.Code != http.StatusNoContent {
		t.Errorf("revoke status = %d, want 204 despite disconnect failure; body=%s", w.Code, w.Body)
	}
}

// TestNilDisconnectorIsSafe pins that a handler tolerates an unwired Disconnector (the contract-test
// construction), performing the mutation without a fan-out.
func TestNilDisconnectorIsSafe(t *testing.T) {
	store := newFakeCustomerStore()
	created, _ := store.Create(t.Context(), newCustomerInput("NoDisc"))
	api := newTestAPIWith(t, adminapi.Deps{Customers: store}) // no Disconnector

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+created.ID.String()+"/suspend", ""))
	if w.Code != http.StatusOK {
		t.Errorf("suspend status = %d, want 200 with no disconnector wired; body=%s", w.Code, w.Body)
	}
}

// decodeID pulls the "id" field from a credential JSON response.
func decodeID(t *testing.T, body []byte) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode id: %v; body=%s", err, body)
	}
	id, _ := m["id"].(string)
	if id == "" {
		t.Fatalf("no id in response: %s", body)
	}
	return id
}
