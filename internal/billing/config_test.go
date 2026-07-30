package billing_test

import (
	"testing"
	"time"

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
	}, nil)

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

// TestConfigSnapshotExternalFor covers the external-billing overlay (§6.10): a customer joined to an active
// provider carries its compiled config (ms → durations); a customer with no provider, and a nil snapshot,
// report no external provider (pure internal billing).
func TestConfigSnapshotExternalFor(t *testing.T) {
	ext := uuid.New()
	plain := uuid.New()
	provider := uuid.New()
	timeoutMs := 120

	snap := billing.BuildConfigSnapshot(
		[]cp.BillingCustomer{{CustomerID: ext, BillingMode: cp.BillingPrepaid}, {CustomerID: plain, BillingMode: cp.BillingPrepaid}},
		[]cp.CustomerExternalBilling{{
			CustomerID: ext, ProviderID: provider, Mode: cp.ExternalModeConsumeSync,
			SyncTimeoutMs: &timeoutMs, FailurePolicy: cp.FailClosed, CacheTTLMs: 1000,
		}},
	)

	cfg, ok := snap.ExternalFor(ext)
	if !ok {
		t.Fatal("ExternalFor(ext) ok=false, want an active provider")
	}
	if cfg.ProviderID != provider || cfg.Mode != cp.ExternalModeConsumeSync ||
		cfg.SyncTimeout != 120*time.Millisecond || cfg.FailurePolicy != cp.FailClosed || cfg.CacheTTL != time.Second {
		t.Errorf("ExternalFor(ext) = %+v, want provider/sync/120ms/fail_closed/1s", cfg)
	}
	if _, ok := snap.ExternalFor(plain); ok {
		t.Error("ExternalFor(plain) ok=true, want false (no provider = pure internal)")
	}

	var nilSnap *billing.ConfigSnapshot
	if _, ok := nilSnap.ExternalFor(ext); ok {
		t.Error("ExternalFor on a nil snapshot ok=true, want false")
	}
}

// TestConfigSnapshotExternalOnlyCustomerStaysFailClosed guards the M1 money-safety hole: an externals row with
// no matching billing customer (a read-skew window) must NOT insert a bare, unfloored entry — that would read
// as an unbounded overdraft. The customer stays absent, so FloorFor fails closed to strict prepaid.
func TestConfigSnapshotExternalOnlyCustomerStaysFailClosed(t *testing.T) {
	orphan := uuid.New()
	snap := billing.BuildConfigSnapshot(
		nil, // no billing customers
		[]cp.CustomerExternalBilling{{CustomerID: orphan, ProviderID: uuid.New(), Mode: cp.ExternalModeBalanceCheck}},
	)
	if has, floor := snap.FloorFor(orphan); !has || floor != 0 {
		t.Errorf("FloorFor(externals-only orphan) = (%v, %d), want (true, 0) strict prepaid — never unbounded", has, floor)
	}
	if _, ok := snap.ExternalFor(orphan); ok {
		t.Error("ExternalFor(externals-only orphan) ok=true, want false (no billing config = no external overlay)")
	}
}
