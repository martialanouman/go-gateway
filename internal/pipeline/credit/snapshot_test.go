package credit_test

import (
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/credit"
)

// TestSnapshotLookup proves the gate: a seeded customer is billing-enabled and carries its scope, an
// absent customer is disabled, and a nil snapshot disables everyone (the pre-load pass-through).
func TestSnapshotLookup(t *testing.T) {
	cust := uuid.New()
	acct := uuid.New()
	snap := credit.BuildSnapshot([]cp.CustomerBillingScope{
		{CustomerID: cust, Scope: cp.BalanceScopeCustomer},
		{CustomerID: acct, Scope: cp.BalanceScopeSMPPAccount},
	})

	if scope, enabled := snap.Lookup(cust); !enabled || scope != cp.BalanceScopeCustomer {
		t.Errorf("Lookup(customer) = (%q, %v), want (customer, true)", scope, enabled)
	}
	if scope, enabled := snap.Lookup(acct); !enabled || scope != cp.BalanceScopeSMPPAccount {
		t.Errorf("Lookup(account) = (%q, %v), want (smpp_account, true)", scope, enabled)
	}
	if _, enabled := snap.Lookup(uuid.New()); enabled {
		t.Error("Lookup(absent) enabled=true, want false (no billing for an unknown customer)")
	}

	var nilSnap *credit.Snapshot
	if _, enabled := nilSnap.Lookup(cust); enabled {
		t.Error("Lookup on a nil snapshot enabled=true, want false (billing disabled until first load)")
	}
}

// TestHolderStoreLoad proves the atomic holder: before any Store it disables everyone, and after a Store it
// serves the stored snapshot.
func TestHolderStoreLoad(t *testing.T) {
	var h credit.Holder
	cust := uuid.New()
	if _, enabled := h.Lookup(cust); enabled {
		t.Error("zero Holder reports enabled=true, want false until first Store")
	}
	h.Store(credit.BuildSnapshot([]cp.CustomerBillingScope{{CustomerID: cust, Scope: cp.BalanceScopeCustomer}}))
	if scope, enabled := h.Lookup(cust); !enabled || scope != cp.BalanceScopeCustomer {
		t.Errorf("after Store, Lookup = (%q, %v), want (customer, true)", scope, enabled)
	}
}
