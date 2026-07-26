package senderid_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type fakePolicyLister struct{ rows []cp.AccountSenderIDPolicy }

func (f fakePolicyLister) ListSenderIDPolicies(context.Context) ([]cp.AccountSenderIDPolicy, error) {
	return f.rows, nil
}

type fakeSenderIDLister struct{ rows []cp.SenderID }

func (f fakeSenderIDLister) ListActive(context.Context) ([]cp.SenderID, error) { return f.rows, nil }

func buildAuthorizer(t *testing.T, policies []cp.AccountSenderIDPolicy, ids []cp.SenderID) *senderid.Authorizer {
	t.Helper()
	a, err := senderid.LoadSnapshot(context.Background(), fakePolicyLister{policies}, fakeSenderIDLister{ids})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return a
}

func TestAuthorizeByPolicy(t *testing.T) {
	cust := uuid.New()
	strictAcct := uuid.New()
	numericAcct := uuid.New()
	disabledAcct := uuid.New()

	policies := []cp.AccountSenderIDPolicy{
		{AccountID: strictAcct, CustomerID: cust, Policy: cp.SenderIDStrict},
		{AccountID: numericAcct, CustomerID: cust, Policy: cp.SenderIDAllowUnregisteredNum},
		{AccountID: disabledAcct, CustomerID: cust, Policy: cp.SenderIDPolicyDisabled},
	}
	// Only "BANK" is a registered, active sender ID for the customer.
	ids := []cp.SenderID{
		{CustomerID: cust, Address: "BANK", Status: cp.SenderIDActive},
	}
	a := buildAuthorizer(t, policies, ids)

	cases := []struct {
		name    string
		account uuid.UUID
		from    string
		wantErr bool
	}{
		{"strict registered alpha ok", strictAcct, "BANK", false},
		// Exact match by design: a case variant of an approved ID is NOT the approved ID (the wire must
		// carry exactly what the carrier approved). See the note in Authorize.
		{"strict case-variant rejected", strictAcct, "Bank", true},
		{"strict padded variant rejected", strictAcct, "BANK ", true},
		{"strict unregistered alpha rejected", strictAcct, "PROMO", true},
		{"strict unregistered numeric rejected", strictAcct, "36000", true},
		{"numeric registered alpha ok", numericAcct, "BANK", false},
		{"numeric unregistered numeric ok", numericAcct, "36000", false},
		{"numeric plus-prefixed msisdn ok", numericAcct, "+22507000001", false},
		{"numeric unregistered alpha rejected", numericAcct, "PROMO", true},
		{"disabled anything ok", disabledAcct, "PROMO", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Authorize(context.Background(), tc.account, cust, tc.from)
			if tc.wantErr {
				if !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
					t.Fatalf("Authorize(%q) = %v, want ErrSenderIDNotAuthorized", tc.from, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize(%q) = %v, want nil", tc.from, err)
			}
		})
	}
}

// TestAuthorizeUnknownAccountIsStrict: an account absent from the (cold) snapshot must fail safe —
// treated as strict, so an unregistered source is rejected rather than silently allowed.
func TestAuthorizeUnknownAccountIsStrict(t *testing.T) {
	cust := uuid.New()
	a := buildAuthorizer(t, nil, []cp.SenderID{{CustomerID: cust, Address: "BANK", Status: cp.SenderIDActive}})

	if err := a.Authorize(context.Background(), uuid.New(), cust, "PROMO"); !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
		t.Fatalf("unknown account = %v, want strict rejection", err)
	}
	if err := a.Authorize(context.Background(), uuid.New(), cust, "BANK"); err != nil {
		t.Fatalf("unknown account, registered sender = %v, want nil (strict allows registered)", err)
	}
}

// TestSnapshotExcludesNonActiveSenderIDs: LoadSnapshot must key only active registrations. A row that
// slipped in non-active must not authorize (defense in depth over the query's WHERE clause).
func TestSnapshotExcludesNonActiveSenderIDs(t *testing.T) {
	cust := uuid.New()
	acct := uuid.New()
	a := buildAuthorizer(t,
		[]cp.AccountSenderIDPolicy{{AccountID: acct, CustomerID: cust, Policy: cp.SenderIDStrict}},
		[]cp.SenderID{{CustomerID: cust, Address: "PENDING", Status: cp.SenderIDDisabled}},
	)
	if err := a.Authorize(context.Background(), acct, cust, "PENDING"); !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
		t.Fatalf("non-active sender id authorized %v, want rejection", err)
	}
}
