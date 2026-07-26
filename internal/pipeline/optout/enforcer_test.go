package optout_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
)

type fakeInboundLister struct{ rows []cp.InboundNumber }

func (f fakeInboundLister) List(context.Context) ([]cp.InboundNumber, error) { return f.rows, nil }

// enforcerFor builds an Enforcer whose Bloom is seeded from `sup` and whose exact checker confirms
// everything the Bloom admits (so the Bloom effectively decides), over the given inbound numbers.
func enforcerFor(t *testing.T, sup []cp.Suppression, nums []cp.InboundNumber) *optout.Enforcer {
	t.Helper()
	snap, err := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{sup})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	idx, err := optout.LoadInboundNumberIndex(context.Background(), fakeInboundLister{nums})
	if err != nil {
		t.Fatalf("LoadInboundNumberIndex: %v", err)
	}
	return optout.NewEnforcer(optout.NewGuard(snap, &recordingChecker{suppressed: true}), idx)
}

// TestEnforcerBlocksOnAnyScope: a suppression in ANY applicable scope blocks the MT (spec §6.20). Each
// scope is exercised in isolation, plus the inbound_number scope resolved from the sender.
func TestEnforcerBlocksOnAnyScope(t *testing.T) {
	cust := uuid.New()
	acct := uuid.New()
	inbID := uuid.New()
	const dest = "2250700000001"
	const shortcode = "36000"

	nums := []cp.InboundNumber{{ID: inbID, Address: shortcode}}

	cases := []struct {
		name string
		sup  cp.Suppression
		from string
	}{
		{"platform", cp.Suppression{Scope: cp.SuppressionScopePlatform, MSISDN: dest}, "ACME"},
		{"customer", cp.Suppression{Scope: cp.SuppressionScopeCustomer, ScopeID: &cust, MSISDN: dest}, "ACME"},
		{"account", cp.Suppression{Scope: cp.SuppressionScopeAccount, ScopeID: &acct, MSISDN: dest}, "ACME"},
		{"inbound_number via sender", cp.Suppression{Scope: cp.SuppressionScopeInboundNumber, ScopeID: &inbID, MSISDN: dest}, shortcode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := enforcerFor(t, []cp.Suppression{tc.sup}, nums)
			blocked, err := e.IsOptedOut(context.Background(), acct, cust, tc.from, dest)
			if err != nil {
				t.Fatalf("IsOptedOut: %v", err)
			}
			if !blocked {
				t.Errorf("scope %s: expected the MT to be blocked", tc.name)
			}
		})
	}
}

// TestEnforcerPassesWhenNoScopeMatches: a destination suppressed for a DIFFERENT customer/account, or
// on an inbound number that is not the sender, must not block this MT.
func TestEnforcerPassesWhenNoScopeMatches(t *testing.T) {
	cust, otherCust := uuid.New(), uuid.New()
	acct := uuid.New()
	inbID := uuid.New()
	const dest = "2250700000001"

	nums := []cp.InboundNumber{{ID: inbID, Address: "36000"}}
	sup := []cp.Suppression{
		// Suppressed, but for another customer.
		{Scope: cp.SuppressionScopeCustomer, ScopeID: &otherCust, MSISDN: dest},
		// Suppressed on an inbound number, but this MT is sent from an alphanumeric sender, not 36000.
		{Scope: cp.SuppressionScopeInboundNumber, ScopeID: &inbID, MSISDN: dest},
	}
	e := enforcerFor(t, sup, nums)

	blocked, err := e.IsOptedOut(context.Background(), acct, cust, "ACME", dest)
	if err != nil {
		t.Fatalf("IsOptedOut: %v", err)
	}
	if blocked {
		t.Error("no applicable scope matches: the MT must pass")
	}
}

// TestEnforcerInboundNumberScopeIgnoredForUnknownSender: a sender that is not one of our inbound
// numbers resolves to no inbound_number scope, so a suppression under some inbound number does not
// leak to it.
func TestEnforcerInboundNumberScopeIgnoredForUnknownSender(t *testing.T) {
	inbID := uuid.New()
	const dest = "2250700000001"
	e := enforcerFor(t,
		[]cp.Suppression{{Scope: cp.SuppressionScopeInboundNumber, ScopeID: &inbID, MSISDN: dest}},
		[]cp.InboundNumber{{ID: inbID, Address: "36000"}})

	blocked, err := e.IsOptedOut(context.Background(), uuid.New(), uuid.New(), "44999", dest)
	if err != nil {
		t.Fatalf("IsOptedOut: %v", err)
	}
	if blocked {
		t.Error("sender 44999 is not an inbound number of ours: the inbound_number scope must not apply")
	}
}

// TestEnforcerPropagatesExactError: a store fault during confirmation is returned, never swallowed
// into a pass — a message must not be treated as deliverable because the store blinked.
func TestEnforcerPropagatesExactError(t *testing.T) {
	cust := uuid.New()
	const dest = "2250700000001"
	snap, _ := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{[]cp.Suppression{
		{Scope: cp.SuppressionScopeCustomer, ScopeID: &cust, MSISDN: dest},
	}})
	sentinel := errors.New("db down")
	e := optout.NewEnforcer(optout.NewGuard(snap, &recordingChecker{err: sentinel}), mustIndex(t))

	if _, err := e.IsOptedOut(context.Background(), uuid.New(), cust, "ACME", dest); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the store fault propagated", err)
	}
}

func mustIndex(t *testing.T) *optout.InboundNumberIndex {
	t.Helper()
	idx, err := optout.LoadInboundNumberIndex(context.Background(), fakeInboundLister{})
	if err != nil {
		t.Fatalf("LoadInboundNumberIndex: %v", err)
	}
	return idx
}
