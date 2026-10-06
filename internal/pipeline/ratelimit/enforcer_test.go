package ratelimit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

func iptr(n int) *int { return &n }

type stubRates struct{ entries []cp.RateLimitEntry }

func (s stubRates) List(context.Context) ([]cp.RateLimitEntry, error) { return s.entries, nil }

type stubConns struct{ conns []cp.Connector }

func (s stubConns) List(context.Context) ([]cp.Connector, error) { return s.conns, nil }

// newEnforcer builds an Enforcer over the given limits/connectors with a frozen clock (no refill), so a
// test counts admissions against a fixed capacity deterministically.
func newEnforcer(t *testing.T, entries []cp.RateLimitEntry, conns []cp.Connector) *ratelimit.Enforcer {
	t.Helper()
	snap, err := ratelimit.LoadSnapshot(context.Background(), stubRates{entries}, stubConns{conns})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	frozen := time.Now()
	lim := ratelimit.NewLimiter(redistest.Client(t), ratelimit.WithClock(func() time.Time { return frozen }))
	return ratelimit.NewEnforcer(snap, lim)
}

// admitted counts how many of n unit-cost submissions the account admits, and asserts every refusal is
// the rate_limited code (never some other error).
func admitted(t *testing.T, e *ratelimit.Enforcer, account uuid.UUID, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		err := e.Admit(context.Background(), account, uuid.Nil, "", 1)
		if err == nil {
			ok++
			continue
		}
		if code, _ := errs.CodeOf(err); code != errs.ErrRateLimited {
			t.Fatalf("submission %d refused with %v, want rate_limited", i+1, err)
		}
	}
	return ok
}

// sent counts how many of n unit-cost sends get the connector's token without waiting past a short
// deadline: with the clock frozen nothing refills, so a send that has to wait never gets one.
func sent(t *testing.T, e *ratelimit.Enforcer, connector uuid.UUID, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		err := e.WaitConnector(ctx, connector)
		cancel()
		if err == nil {
			ok++
			continue
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("send %d failed with %v, want to wait until the deadline", i+1, err)
		}
	}
	return ok
}

// TestEnforcerAccountLimitRefuses: a submission flow past the account's own limit is refused.
func TestEnforcerAccountLimitRefuses(t *testing.T) {
	account := uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityAccount, EntityID: account, Limit: cp.RateLimit{MaxPerSec: iptr(10)}},
	}, nil)

	if got := admitted(t, e, account, 15); got != 10 {
		t.Errorf("admitted %d of 15, want the account limit 10", got)
	}
}

// TestEnforcerSenderLimitRefusesOnlyItsOwnFlow: a sender ID's bucket caps that flow alone (ADR-0021 §3):
// another sender of the same account, and the same address under another customer, keep their room.
func TestEnforcerSenderLimitRefusesOnlyItsOwnFlow(t *testing.T) {
	account, customer, otherCustomer, otp := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{{
		EntityType: ratelimit.EntitySenderID, EntityID: otp,
		Sender: &cp.SenderAddress{CustomerID: customer, Address: "OTP"},
		Limit:  cp.RateLimit{MaxPerSec: iptr(2)},
	}}, nil)

	admit := func(customerID uuid.UUID, from string) error {
		return e.Admit(context.Background(), account, customerID, from, 1)
	}
	for i := range 2 {
		if err := admit(customer, "OTP"); err != nil {
			t.Fatalf("submission %d within the sender's limit = %v, want admitted", i+1, err)
		}
	}
	if err := admit(customer, "OTP"); !errors.Is(err, errs.ErrRateLimited) {
		t.Fatalf("submission past the sender's limit = %v, want rate_limited", err)
	}
	if err := admit(customer, "PROMO"); err != nil {
		t.Errorf("another sender of the account = %v, want admitted", err)
	}
	if err := admit(otherCustomer, "OTP"); err != nil {
		t.Errorf("the same address under another customer = %v, want admitted", err)
	}
}

// TestEnforcerSenderRefusalSparesTheAccountBucket: the sender is checked first, so a message its sender
// refuses does not spend the account's tokens that the customer's other flows need.
func TestEnforcerSenderRefusalSparesTheAccountBucket(t *testing.T) {
	account, customer, otp := uuid.New(), uuid.New(), uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityAccount, EntityID: account, Limit: cp.RateLimit{MaxPerSec: iptr(2)}},
		{
			EntityType: ratelimit.EntitySenderID, EntityID: otp,
			Sender: &cp.SenderAddress{CustomerID: customer, Address: "OTP"},
			Limit:  cp.RateLimit{MaxPerSec: iptr(1)},
		},
	}, nil)

	ctx := context.Background()
	if err := e.Admit(ctx, account, customer, "OTP", 1); err != nil {
		t.Fatalf("first OTP = %v, want admitted", err)
	}
	if err := e.Admit(ctx, account, customer, "OTP", 1); !errors.Is(err, errs.ErrRateLimited) {
		t.Fatalf("second OTP = %v, want refused by its sender", err)
	}
	if err := e.Admit(ctx, account, customer, "PROMO", 1); err != nil {
		t.Fatalf("PROMO after a sender refusal = %v, want admitted on the account's last token", err)
	}
}

// TestEnforcerConnectorThroughputFallback: a connector with NO operational rate_limit is still bounded
// by its throughput_limit_per_sec hard ceiling — a connector is never left un-limited.
func TestEnforcerConnectorThroughputFallback(t *testing.T) {
	connector := uuid.New()
	e := newEnforcer(t, nil, []cp.Connector{
		{ID: connector, ThroughputLimitPerSec: iptr(5)},
	})

	if got := sent(t, e, connector, 8); got != 5 {
		t.Errorf("sent %d of 8, want the connector throughput ceiling 5 (no rate_limit row configured)", got)
	}
}

// TestEnforcerNoLimitAllowsAll: an entity with no configured limit (and a connector with no ceiling) is
// not throttled.
func TestEnforcerNoLimitAllowsAll(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	e := newEnforcer(t, nil, nil)

	if got := admitted(t, e, account, 25); got != 25 {
		t.Errorf("admitted %d of 25, want all — nothing is configured to limit", got)
	}
	if got := sent(t, e, connector, 25); got != 25 {
		t.Errorf("sent %d of 25, want all — nothing is configured to limit", got)
	}
}

// TestEnforcerLongMessageExceedingBurstIsAdmitted: a submission with more segments than the account's
// burst must still be admissible — the ceiling is raised to the message's cost, so a legitimate long SMS
// is not throttled forever (which would masquerade a structural rejection as a retryable one).
func TestEnforcerLongMessageExceedingBurstIsAdmitted(t *testing.T) {
	account := uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityAccount, EntityID: account, Limit: cp.RateLimit{MaxPerSec: iptr(3)}},
	}, nil)
	if err := e.Admit(context.Background(), account, uuid.Nil, "", 6); err != nil {
		t.Errorf("a 6-segment submission against an account burst of 3 must be admitted, got %v", err)
	}
}

// TestEnforcerOperationalLimitWinsOverCeiling: an explicit operational rate_limit for a connector takes
// precedence over its (higher) throughput ceiling.
func TestEnforcerOperationalLimitWinsOverCeiling(t *testing.T) {
	connector := uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityConnector, EntityID: connector, Limit: cp.RateLimit{MaxPerSec: iptr(3)}},
	}, []cp.Connector{
		{ID: connector, ThroughputLimitPerSec: iptr(100)},
	})

	if got := sent(t, e, connector, 10); got != 3 {
		t.Errorf("sent %d of 10, want the operational limit 3 (not the 100 ceiling)", got)
	}
}

// TestWaitConnectorSendsOnceTheBucketRefills: a send past the ceiling is slowed, not refused — it goes
// as soon as the bucket has refilled its token.
func TestWaitConnectorSendsOnceTheBucketRefills(t *testing.T) {
	connector := uuid.New()
	snap, err := ratelimit.LoadSnapshot(context.Background(), stubRates{[]cp.RateLimitEntry{{
		EntityType: ratelimit.EntityConnector, EntityID: connector,
		Limit: cp.RateLimit{MaxPerSec: iptr(2), BurstCapacity: iptr(1)},
	}}}, stubConns{})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	e := ratelimit.NewEnforcer(snap, ratelimit.NewLimiter(redistest.Client(t)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := e.WaitConnector(ctx, connector); err != nil {
		t.Fatalf("first send: %v", err)
	}
	start := time.Now()
	if err := e.WaitConnector(ctx, connector); err != nil {
		t.Fatalf("the second send was refused instead of slowed: %v", err)
	}
	if waited := time.Since(start); waited < 300*time.Millisecond {
		t.Errorf("the second send went after %v, want it held until the 500 ms refill", waited)
	}
}

// TestTheRerouteBudgetIsNotTheSendBudget: the parking gate and the drainer pace the republication of a
// reroute; the send pays the connector's ceiling once more at the target. Sharing one bucket made a
// rerouted message pay twice.
func TestTheRerouteBudgetIsNotTheSendBudget(t *testing.T) {
	connector := uuid.New()
	e := newEnforcer(t, nil, []cp.Connector{{ID: connector, ThroughputLimitPerSec: iptr(2)}})

	for i := range 2 {
		if !e.AllowConnector(context.Background(), connector) {
			t.Fatalf("reroute %d refused within the ceiling — the control failed", i+1)
		}
	}
	if got := sent(t, e, connector, 2); got != 2 {
		t.Errorf("after two reroutes, sent %d of 2: the reroutes consumed the send budget", got)
	}
}

// TestWaitConnectorNeverSendsOnADeadContext: an ended context makes the Redis call fail, and the per-pod
// fallback would still hand out a token. A send on a shutdown or a rebalance must stop, not go.
func TestWaitConnectorNeverSendsOnADeadContext(t *testing.T) {
	connector := uuid.New()
	e := newEnforcer(t, nil, []cp.Connector{{ID: connector, ThroughputLimitPerSec: iptr(5)}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := e.WaitConnector(ctx, connector); !errors.Is(err, context.Canceled) {
		t.Errorf("WaitConnector on a cancelled context = %v, want context.Canceled", err)
	}
}
