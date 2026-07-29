package billing_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func intptr(v int) *int { return &v }

// TestConfigSnapshotFloorFor covers the billing_customers → (has_floor, floor) mapping through the
// immutable snapshot, plus the fail-closed rule for an unknown customer. floor is the minimum the balance
// may reach in reserve.lua (strict prepaid = 0; overdraft/hard-limit = negative; soft postpaid = no floor).
func TestConfigSnapshotFloorFor(t *testing.T) {
	prepaid := uuid.New()
	prepaidOverdraft := uuid.New()
	prepaidOverdraftNoLimit := uuid.New()
	postpaidHard := uuid.New()
	postpaidHardNoLimit := uuid.New()
	postpaidSoft := uuid.New()
	unknown := uuid.New()

	snap := billing.BuildConfigSnapshot([]cp.BillingCustomer{
		{CustomerID: prepaid, BillingMode: cp.BillingPrepaid},
		{CustomerID: prepaidOverdraft, BillingMode: cp.BillingPrepaid, OverdraftEnabled: true, OverdraftLimit: intptr(500)},
		// overdraft enabled but no limit configured → fail-closed to strict prepaid (never an open overdraft).
		{CustomerID: prepaidOverdraftNoLimit, BillingMode: cp.BillingPrepaid, OverdraftEnabled: true, OverdraftLimit: nil},
		{CustomerID: postpaidHard, BillingMode: cp.BillingPostpaid, CreditLimit: intptr(1000), CreditLimitIsHard: true},
		// postpaid hard limit but no limit value → fail-closed to strict prepaid.
		{CustomerID: postpaidHardNoLimit, BillingMode: cp.BillingPostpaid, CreditLimit: nil, CreditLimitIsHard: true},
		// postpaid soft limit → no reserve floor (the soft limit is advisory, enforced by alerting elsewhere).
		{CustomerID: postpaidSoft, BillingMode: cp.BillingPostpaid, CreditLimit: intptr(1000), CreditLimitIsHard: false},
	})

	cases := []struct {
		name       string
		customerID uuid.UUID
		wantHas    bool
		wantFloor  int
	}{
		{"prepaid strict", prepaid, true, 0},
		{"prepaid overdraft", prepaidOverdraft, true, -500},
		{"prepaid overdraft no limit → strict", prepaidOverdraftNoLimit, true, 0},
		{"postpaid hard limit", postpaidHard, true, -1000},
		{"postpaid hard no limit → strict", postpaidHardNoLimit, true, 0},
		{"postpaid soft → no floor", postpaidSoft, false, 0},
		{"unknown customer → strict prepaid (fail-closed)", unknown, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			has, floor := snap.FloorFor(tc.customerID)
			if has != tc.wantHas || floor != tc.wantFloor {
				t.Errorf("FloorFor = (%v, %d), want (%v, %d)", has, floor, tc.wantHas, tc.wantFloor)
			}
		})
	}
}

// TestConfigSnapshotNil proves a nil snapshot fails closed to strict prepaid — so a provider whose
// snapshot has never been built (startup, before config-sync's first push) never opens an overdraft.
func TestConfigSnapshotNil(t *testing.T) {
	var snap *billing.ConfigSnapshot
	if has, floor := snap.FloorFor(uuid.New()); !has || floor != 0 {
		t.Errorf("nil snapshot FloorFor = (%v, %d), want (true, 0)", has, floor)
	}
}
