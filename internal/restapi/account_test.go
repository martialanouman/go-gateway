package restapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/restapi"
)

type fakeAccountReader struct {
	acc cp.Account
	err error
}

func (f fakeAccountReader) Get(context.Context, uuid.UUID) (cp.Account, error) {
	return f.acc, f.err
}

type fakeSenderIDReader struct {
	ids []cp.SenderID
	err error
}

func (f fakeSenderIDReader) ListByCustomer(context.Context, uuid.UUID) ([]cp.SenderID, error) {
	return f.ids, f.err
}

type fakeRateLimitReader struct {
	limit cp.RateLimit
	found bool
	err   error
}

func (f fakeRateLimitReader) RateLimit(context.Context, uuid.UUID) (cp.RateLimit, bool, error) {
	return f.limit, f.found, f.err
}

// newAccountHarness builds the public API wired only with the collaborators get-account needs. It
// deliberately omits the ingest/CDR plumbing the message endpoints require.
func newAccountHarness(t *testing.T, principals restapi.PrincipalStore, deps restapi.Deps) *harness {
	t.Helper()
	deps.Principals = principals
	deps.Version = "test"
	mux, _ := restapi.New(deps)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &harness{server: srv}
}

func sampleAccount() cp.Account {
	return cp.Account{
		ID:             uuid.New(),
		CustomerID:     uuid.New(),
		Name:           "acme-prod",
		Status:         cp.AccountActive,
		SMPPEnabled:    true,
		RESTEnabled:    true,
		SenderIDPolicy: cp.SenderIDStrict,
		MaxSessions:    4,
	}
}

func TestGetAccountRequiresAuth(t *testing.T) {
	h := newAccountHarness(t, fakePrincipals{found: false}, restapi.Deps{
		Accounts:   fakeAccountReader{},
		SenderIDs:  fakeSenderIDReader{},
		RateLimits: fakeRateLimitReader{},
	})
	resp := h.do(t, http.MethodGet, "/v1/account", "", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d want 401", resp.StatusCode)
	}
}

func TestGetAccountReturnsOwnAccount(t *testing.T) {
	acc := sampleAccount()
	principal := activePrincipal()
	principal.AccountID = acc.ID
	principal.CustomerID = acc.CustomerID

	maxPerSec := 200
	burst := 500
	h := newAccountHarness(t, fakePrincipals{principal: principal, found: true}, restapi.Deps{
		Accounts: fakeAccountReader{acc: acc},
		SenderIDs: fakeSenderIDReader{ids: []cp.SenderID{
			{Address: "ACME", Status: cp.SenderIDActive},
			{Address: "24999", Status: cp.SenderIDPendingCarrierApproval},
		}},
		RateLimits: fakeRateLimitReader{
			limit: cp.RateLimit{MaxPerSec: &maxPerSec, BurstCapacity: &burst},
			found: true,
		},
	})

	resp := h.do(t, http.MethodGet, "/v1/account", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	var body restapi.AccountInfo
	decode(t, resp, &body)

	if body.AccountID != acc.ID.String() || body.CustomerID != acc.CustomerID.String() {
		t.Errorf("scoping: got account=%q customer=%q", body.AccountID, body.CustomerID)
	}
	if body.Name != "acme-prod" || body.Status != "active" {
		t.Errorf("identity: got name=%q status=%q", body.Name, body.Status)
	}
	if !body.Channels.SMPPEnabled || !body.Channels.RESTEnabled {
		t.Errorf("channels: got %+v", body.Channels)
	}
	if body.SenderIDPolicy != "strict" {
		t.Errorf("sender_id_policy: got %q", body.SenderIDPolicy)
	}
	if body.MaxSessions != 4 {
		t.Errorf("max_sessions: got %d want 4", body.MaxSessions)
	}
	if len(body.SenderIDs) != 2 || body.SenderIDs[0].Address != "ACME" || body.SenderIDs[0].Status != "active" {
		t.Errorf("sender_ids: got %+v", body.SenderIDs)
	}
	if body.RateLimits == nil || body.RateLimits.MaxPerSec == nil || *body.RateLimits.MaxPerSec != 200 {
		t.Errorf("rate_limits: got %+v", body.RateLimits)
	}
	if body.RateLimits.MaxPerDay != nil {
		t.Errorf("rate_limits.max_per_day should be null, got %v", *body.RateLimits.MaxPerDay)
	}
}

func TestGetAccountRateLimitsNullWhenAbsent(t *testing.T) {
	acc := sampleAccount()
	principal := activePrincipal()
	principal.AccountID = acc.ID
	principal.CustomerID = acc.CustomerID

	h := newAccountHarness(t, fakePrincipals{principal: principal, found: true}, restapi.Deps{
		Accounts:   fakeAccountReader{acc: acc},
		SenderIDs:  fakeSenderIDReader{},
		RateLimits: fakeRateLimitReader{found: false},
	})

	resp := h.do(t, http.MethodGet, "/v1/account", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	// sender_ids must be an empty array (not null): the contract lists it as required, non-nullable.
	var raw map[string]json.RawMessage
	decode(t, resp, &raw)
	if got := string(raw["rate_limits"]); got != "null" {
		t.Errorf("rate_limits: got %q want null", got)
	}
	if got := string(raw["sender_ids"]); got != "[]" {
		t.Errorf("sender_ids: got %q want []", got)
	}
}

// TestGetAccountLeaksNoSecret guards invariant (a): the projection must never carry a credential. The
// DTO does not model one, but we assert on the raw wire body — at every nesting depth, since a secret
// could be added inside channels, a sender_ids item or rate_limits — so a future field addition
// cannot slip a secret through unnoticed.
func TestGetAccountLeaksNoSecret(t *testing.T) {
	acc := sampleAccount()
	principal := activePrincipal()
	principal.AccountID = acc.ID
	principal.CustomerID = acc.CustomerID

	maxPerSec := 200
	h := newAccountHarness(t, fakePrincipals{principal: principal, found: true}, restapi.Deps{
		Accounts:   fakeAccountReader{acc: acc},
		SenderIDs:  fakeSenderIDReader{ids: []cp.SenderID{{Address: "ACME", Status: cp.SenderIDActive}}},
		RateLimits: fakeRateLimitReader{limit: cp.RateLimit{MaxPerSec: &maxPerSec}, found: true},
	})

	resp := h.do(t, http.MethodGet, "/v1/account", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()

	var raw any
	decode(t, resp, &raw)
	for _, key := range collectKeys(raw) {
		lower := strings.ToLower(key)
		for _, forbidden := range []string{"hash", "password", "api_key", "apikey", "secret", "token", "credential", "bearer", "key"} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("account view leaks a secret-like field: %q", key)
			}
		}
	}
}

// collectKeys walks a decoded JSON value and returns every object key found at any depth.
func collectKeys(v any) []string {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k, child := range t {
			keys = append(keys, k)
			keys = append(keys, collectKeys(child)...)
		}
		return keys
	case []any:
		var keys []string
		for _, child := range t {
			keys = append(keys, collectKeys(child)...)
		}
		return keys
	default:
		return nil
	}
}

// TestGetAccountReaderErrorReturns500 covers the three error branches: any control-plane read failing
// is a 500, not a partial or leaked response. One case per collaborator so a swallowed error on any
// of the three is caught.
func TestGetAccountReaderErrorReturns500(t *testing.T) {
	acc := sampleAccount()
	principal := activePrincipal()
	principal.AccountID = acc.ID
	principal.CustomerID = acc.CustomerID
	boom := errors.New("boom")

	cases := map[string]restapi.Deps{
		"account read fails": {
			Accounts:   fakeAccountReader{err: boom},
			SenderIDs:  fakeSenderIDReader{},
			RateLimits: fakeRateLimitReader{},
		},
		"sender id read fails": {
			Accounts:   fakeAccountReader{acc: acc},
			SenderIDs:  fakeSenderIDReader{err: boom},
			RateLimits: fakeRateLimitReader{},
		},
		"rate limit read fails": {
			Accounts:   fakeAccountReader{acc: acc},
			SenderIDs:  fakeSenderIDReader{},
			RateLimits: fakeRateLimitReader{err: boom},
		},
	}

	for name, deps := range cases {
		t.Run(name, func(t *testing.T) {
			h := newAccountHarness(t, fakePrincipals{principal: principal, found: true}, deps)
			resp := h.do(t, http.MethodGet, "/v1/account", "sgw_key", nil)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("status: got %d want 500", resp.StatusCode)
			}
		})
	}
}

// TestGetAccountForbiddenWhenRestDisabled confirms the shared apiKeyMiddleware gates get-account like
// every other authenticated endpoint: a disabled REST channel is 403 before the handler runs.
func TestGetAccountForbiddenWhenRestDisabled(t *testing.T) {
	principal := activePrincipal()
	principal.RESTEnabled = false

	h := newAccountHarness(t, fakePrincipals{principal: principal, found: true}, restapi.Deps{
		Accounts:   fakeAccountReader{acc: sampleAccount()},
		SenderIDs:  fakeSenderIDReader{},
		RateLimits: fakeRateLimitReader{},
	})
	resp := h.do(t, http.MethodGet, "/v1/account", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status: got %d want 403", resp.StatusCode)
	}
}
