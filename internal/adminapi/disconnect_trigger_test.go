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

func seedCredential(t *testing.T, api http.Handler, body string) (uuid.UUID, string, string) {
	t.Helper()
	accountID := uuid.New()
	path := "/v1/admin/smpp-accounts/" + accountID.String() + "/credentials"
	create := httptest.NewRecorder()
	api.ServeHTTP(create, authed(t, http.MethodPost, path, body))
	if create.Code != http.StatusCreated {
		t.Fatalf("seed create status = %d; body=%s", create.Code, create.Body)
	}
	return accountID, path, decodeID(t, create.Body.Bytes())
}

const (
	apiKeyBody   = `{"type":"api_key"}`
	smppBindBody = `{"type":"smpp_bind","system_id":"esme1"}`
)

// TestRevokeCredentialDisconnectsOnlyAnSMPPBind pins that revoking an smpp_bind force-disconnects the
// account's live binds (step-032), while revoking an api_key closes none: REST calls are stateless, and
// the account's binds authenticate with the other credential.
func TestRevokeCredentialDisconnectsOnlyAnSMPPBind(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantCalls int
	}{
		{"smpp_bind", smppBindBody, 1},
		{"api_key", apiKeyBody, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disc := &fakeDisconnector{}
			api := newTestAPIWith(t, adminapi.Deps{Credentials: newFakeCredentialStore(), Accounts: newFakeAccountStore(), Disconnector: disc})
			accountID, path, credID := seedCredential(t, api, tc.body)

			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, http.MethodDelete, path+"/"+credID, ""))
			if w.Code != http.StatusNoContent {
				t.Fatalf("revoke status = %d, want 204; body=%s", w.Code, w.Body)
			}

			calls := disc.accountCalls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("account disconnects = %+v, want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls == 1 && (calls[0].id != accountID || calls[0].reason != "credential_revoked") {
				t.Errorf("disconnect = %+v, want {%s credential_revoked}", calls[0], accountID)
			}
		})
	}
}

// TestUpdateCredentialStatusDisconnectsOnlyAnSMPPBind pins that disabling an smpp_bind disconnects
// while re-activating it does not, and that disabling an api_key disconnects nothing.
func TestUpdateCredentialStatusDisconnectsOnlyAnSMPPBind(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantCalls int
	}{
		{"smpp_bind", smppBindBody, 1},
		{"api_key", apiKeyBody, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disc := &fakeDisconnector{}
			api := newTestAPIWith(t, adminapi.Deps{Credentials: newFakeCredentialStore(), Accounts: newFakeAccountStore(), Disconnector: disc})
			_, path, credID := seedCredential(t, api, tc.body)

			for _, status := range []string{"disabled", "active"} {
				w := httptest.NewRecorder()
				api.ServeHTTP(w, authed(t, http.MethodPatch, path+"/"+credID, `{"status":"`+status+`"}`))
				if w.Code != http.StatusOK {
					t.Fatalf("%s status = %d; body=%s", status, w.Code, w.Body)
				}
			}

			calls := disc.accountCalls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("account disconnects = %+v, want %d (only the disable)", calls, tc.wantCalls)
			}
			if tc.wantCalls == 1 && calls[0].reason != "credential_disabled" {
				t.Errorf("reason = %q, want credential_disabled", calls[0].reason)
			}
		})
	}
}

// TestRotateCredentialDisconnectsOnlyAnSMPPBindCutover pins that a rotation without a grace window cuts
// the smpp_bind's live binds: a secret rotated without grace is presumed leaked, and a session already
// bound with it must fall. A grace window is a planned rotation, and an api_key has no bind to cut.
func TestRotateCredentialDisconnectsOnlyAnSMPPBindCutover(t *testing.T) {
	tests := []struct {
		name      string
		seed      string
		rotate    string
		wantCalls int
	}{
		{"smpp_bind without body", smppBindBody, ``, 1},
		{"smpp_bind with null grace", smppBindBody, `{"grace_period_sec":null}`, 1},
		{"smpp_bind with zero grace", smppBindBody, `{"grace_period_sec":0}`, 1},
		{"smpp_bind with a grace window", smppBindBody, `{"grace_period_sec":600}`, 0},
		{"api_key without grace", apiKeyBody, ``, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			disc := &fakeDisconnector{}
			api := newTestAPIWith(t, adminapi.Deps{Credentials: newFakeCredentialStore(), Accounts: newFakeAccountStore(), Disconnector: disc})
			accountID, path, credID := seedCredential(t, api, tc.seed)

			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, http.MethodPost, path+"/"+credID+"/rotate", tc.rotate))
			if w.Code != http.StatusOK {
				t.Fatalf("rotate status = %d, want 200; body=%s", w.Code, w.Body)
			}

			calls := disc.accountCalls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("account disconnects = %+v, want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls == 1 && (calls[0].id != accountID || calls[0].reason != "credential_rotated") {
				t.Errorf("disconnect = %+v, want {%s credential_rotated}", calls[0], accountID)
			}
		})
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
	disc := &fakeDisconnector{err: errors.New("session-manager down")}
	api := newTestAPIWith(t, adminapi.Deps{Credentials: newFakeCredentialStore(), Accounts: newFakeAccountStore(), Disconnector: disc})
	_, path, credID := seedCredential(t, api, smppBindBody)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, path+"/"+credID, ""))
	if w.Code != http.StatusNoContent {
		t.Errorf("revoke status = %d, want 204 despite disconnect failure; body=%s", w.Code, w.Body)
	}
	if len(disc.accountCalls()) != 1 {
		t.Errorf("account disconnects = %d, want 1: the failing disconnector was never reached", len(disc.accountCalls()))
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
