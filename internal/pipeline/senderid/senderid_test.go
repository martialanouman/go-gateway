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

type fakeSenderIDLister struct{ rows []cp.SenderID }

func (f fakeSenderIDLister) ListActive(context.Context) ([]cp.SenderID, error) { return f.rows, nil }

func buildAuthorizer(t *testing.T, ids []cp.SenderID) *senderid.Authorizer {
	t.Helper()
	a, err := senderid.LoadSnapshot(context.Background(), fakeSenderIDLister{ids})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return a
}

// TestEverySenderMustBeRegistered: ADR-0020 removed the per-account policy, so a numeric source is
// rejected like an alphanumeric one unless the customer holds an active registration for it.
func TestEverySenderMustBeRegistered(t *testing.T) {
	cust := uuid.New()
	other := uuid.New()
	a := buildAuthorizer(t, []cp.SenderID{
		{CustomerID: cust, Address: "BANK", Status: cp.SenderIDActive},
		{CustomerID: cust, Address: "36000", Status: cp.SenderIDActive},
		{CustomerID: other, Address: "OTHER", Status: cp.SenderIDActive},
	})

	cases := []struct {
		name    string
		from    string
		wantErr bool
	}{
		{"registered alpha ok", "BANK", false},
		{"registered numeric ok", "36000", false},
		// Exact match by design: a case variant of an approved ID is NOT the approved ID (the wire must
		// carry exactly what the carrier approved). See the note in Authorize.
		{"case-variant rejected", "Bank", true},
		{"padded variant rejected", "BANK ", true},
		{"unregistered alpha rejected", "PROMO", true},
		{"unregistered numeric rejected", "36001", true},
		{"unregistered plus-prefixed msisdn rejected", "+22507000001", true},
		{"another customer's sender rejected", "OTHER", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Authorize(context.Background(), cust, tc.from)
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

// TestSnapshotExcludesNonActiveSenderIDs: LoadSnapshot must key only active registrations. A row that
// slipped in non-active must not authorize (defense in depth over the query's WHERE clause).
func TestSnapshotExcludesNonActiveSenderIDs(t *testing.T) {
	cust := uuid.New()
	a := buildAuthorizer(t, []cp.SenderID{{CustomerID: cust, Address: "PENDING", Status: cp.SenderIDDisabled}})
	if err := a.Authorize(context.Background(), cust, "PENDING"); !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
		t.Fatalf("non-active sender id authorized %v, want rejection", err)
	}
}
