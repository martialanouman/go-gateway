package optout_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
)

type fakeSuppressionLister struct{ rows []cp.Suppression }

func (f fakeSuppressionLister) ListSuppressions(context.Context) ([]cp.Suppression, error) {
	return f.rows, nil
}

func ptr(u uuid.UUID) *uuid.UUID { return &u }

// TestBloomNoFalseNegatives is the load-bearing invariant: every MSISDN that was inserted into a
// scope must test positive. A false negative would let a suppressed destination through — never
// acceptable. (False positives are allowed and confirmed exactly downstream.)
func TestBloomNoFalseNegatives(t *testing.T) {
	cust := uuid.New()
	acct := uuid.New()
	inbound := uuid.New()

	var rows []cp.Suppression
	// A large, varied population across every scope, so a real Bloom (not a set) is exercised.
	for i := 0; i < 5000; i++ {
		rows = append(rows,
			cp.Suppression{Scope: cp.SuppressionScopePlatform, MSISDN: fmt.Sprintf("2250700%06d", i)},
			cp.Suppression{Scope: cp.SuppressionScopeCustomer, ScopeID: ptr(cust), MSISDN: fmt.Sprintf("2250701%06d", i)},
			cp.Suppression{Scope: cp.SuppressionScopeAccount, ScopeID: ptr(acct), MSISDN: fmt.Sprintf("2250702%06d", i)},
			cp.Suppression{Scope: cp.SuppressionScopeInboundNumber, ScopeID: ptr(inbound), MSISDN: fmt.Sprintf("2250703%06d", i)},
		)
	}
	snap, err := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{rows})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	for _, s := range rows {
		if !snap.MightBeSuppressed(s.Scope, s.ScopeID, s.MSISDN) {
			t.Fatalf("false negative: %s/%v/%s tested absent after insert", s.Scope, s.ScopeID, s.MSISDN)
		}
	}
}

// TestBloomScopeIsolation: a suppression in one scope must not answer positive in another, nor across
// scope entities. (A cross-scope false positive is possible in principle but vanishingly unlikely; we
// assert on values chosen not to collide.)
func TestBloomScopeIsolation(t *testing.T) {
	custA, custB := uuid.New(), uuid.New()
	snap, err := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{[]cp.Suppression{
		{Scope: cp.SuppressionScopeCustomer, ScopeID: ptr(custA), MSISDN: "2250700000001"},
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if !snap.MightBeSuppressed(cp.SuppressionScopeCustomer, ptr(custA), "2250700000001") {
		t.Fatal("the inserted entry must test positive")
	}
	// Same number, different customer: must not be suppressed.
	if snap.MightBeSuppressed(cp.SuppressionScopeCustomer, ptr(custB), "2250700000001") {
		t.Error("customer B must not inherit customer A's suppression")
	}
	// Same number, platform scope (never populated): definitively not suppressed.
	if snap.MightBeSuppressed(cp.SuppressionScopePlatform, nil, "2250700000001") {
		t.Error("platform scope holds no filter and must answer not-suppressed")
	}
}

// --- Guard: Bloom gates the exact confirmation ---

type recordingChecker struct {
	suppressed bool
	err        error
	calls      int
}

func (c *recordingChecker) IsSuppressed(context.Context, cp.SuppressionScope, *uuid.UUID, string) (bool, error) {
	c.calls++
	return c.suppressed, c.err
}

func TestGuardSkipsExactWhenBloomNegative(t *testing.T) {
	snap, _ := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{nil}) // empty
	checker := &recordingChecker{suppressed: true}                                   // would say yes IF asked
	g := optout.NewGuard(snap, checker)

	ok, err := g.IsSuppressed(context.Background(), cp.SuppressionScopeCustomer, ptr(uuid.New()), "2250700000009")
	if err != nil {
		t.Fatalf("IsSuppressed: %v", err)
	}
	if ok {
		t.Error("empty Bloom must short-circuit to not-suppressed")
	}
	if checker.calls != 0 {
		t.Errorf("exact checker called %d times, want 0 (Bloom gated it out)", checker.calls)
	}
}

func TestGuardConfirmsBloomHitExactly(t *testing.T) {
	cust := uuid.New()
	snap, _ := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{[]cp.Suppression{
		{Scope: cp.SuppressionScopeCustomer, ScopeID: ptr(cust), MSISDN: "2250700000001"},
	}})

	// A Bloom hit that the source of truth denies (e.g. a false positive, or the row was removed) must
	// resolve to not-suppressed via the exact check.
	deny := &recordingChecker{suppressed: false}
	g := optout.NewGuard(snap, deny)
	ok, err := g.IsSuppressed(context.Background(), cp.SuppressionScopeCustomer, ptr(cust), "2250700000001")
	if err != nil {
		t.Fatalf("IsSuppressed: %v", err)
	}
	if ok {
		t.Error("a Bloom hit denied by the exact check must be not-suppressed")
	}
	if deny.calls != 1 {
		t.Errorf("exact checker called %d times, want 1 (Bloom hit → confirm)", deny.calls)
	}

	// A confirmed hit is suppressed.
	confirm := &recordingChecker{suppressed: true}
	if ok, _ := optout.NewGuard(snap, confirm).IsSuppressed(context.Background(), cp.SuppressionScopeCustomer, ptr(cust), "2250700000001"); !ok {
		t.Error("a Bloom hit confirmed by the exact check must be suppressed")
	}
}

func TestGuardPropagatesExactError(t *testing.T) {
	cust := uuid.New()
	snap, _ := optout.LoadSnapshot(context.Background(), fakeSuppressionLister{[]cp.Suppression{
		{Scope: cp.SuppressionScopeCustomer, ScopeID: ptr(cust), MSISDN: "2250700000001"},
	}})
	sentinel := errors.New("db down")
	_, err := optout.NewGuard(snap, &recordingChecker{err: sentinel}).
		IsSuppressed(context.Background(), cp.SuppressionScopeCustomer, ptr(cust), "2250700000001")
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the exact checker's error propagated", err)
	}
}
