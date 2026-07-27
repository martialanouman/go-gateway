package ratelimit_test

import (
	"context"
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

// admitted counts how many of n unit-cost messages the enforcer admits, and asserts every rejection is
// the rate_limited code (never some other error).
func admitted(t *testing.T, e *ratelimit.Enforcer, account, connector uuid.UUID, route *uuid.UUID, n int) int {
	t.Helper()
	ok := 0
	for i := 0; i < n; i++ {
		err := e.Check(context.Background(), account, connector, route, 1)
		if err == nil {
			ok++
			continue
		}
		if code, _ := errs.CodeOf(err); code != errs.ErrRateLimited {
			t.Fatalf("message %d rejected with %v, want rate_limited", i+1, err)
		}
	}
	return ok
}

// TestEnforcerConnectorCeilingBeatsRoute is the M6 acceptance criterion: a connector limited to 50/s
// caps the message flow even when the route allows 100/s — the tighter, higher-precedence connector
// ceiling is never exceeded.
func TestEnforcerConnectorCeilingBeatsRoute(t *testing.T) {
	account, connector, routeID := uuid.New(), uuid.New(), uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityRoute, EntityID: routeID, Limit: cp.RateLimit{MaxPerSec: iptr(100)}},
		{EntityType: ratelimit.EntityConnector, EntityID: connector, Limit: cp.RateLimit{MaxPerSec: iptr(50)}},
	}, nil)

	if got := admitted(t, e, account, connector, &routeID, 60); got != 50 {
		t.Errorf("admitted %d of 60, want the connector ceiling 50 (route allows 100 but the connector caps first)", got)
	}
}

// TestEnforcerAccountLimitRejects: a message flow past the account's own limit is throttled.
func TestEnforcerAccountLimitRejects(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityAccount, EntityID: account, Limit: cp.RateLimit{MaxPerSec: iptr(10)}},
	}, nil)

	if got := admitted(t, e, account, connector, nil, 15); got != 10 {
		t.Errorf("admitted %d of 15, want the account limit 10", got)
	}
}

// TestEnforcerConnectorThroughputFallback: a connector with NO operational rate_limit is still bounded
// by its throughput_limit_per_sec hard ceiling — a connector is never left un-limited.
func TestEnforcerConnectorThroughputFallback(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	e := newEnforcer(t, nil, []cp.Connector{
		{ID: connector, ThroughputLimitPerSec: iptr(5)},
	})

	if got := admitted(t, e, account, connector, nil, 8); got != 5 {
		t.Errorf("admitted %d of 8, want the connector throughput ceiling 5 (no rate_limit row configured)", got)
	}
}

// TestEnforcerNoLimitAllowsAll: an entity with no configured limit (and a connector with no ceiling) is
// not throttled.
func TestEnforcerNoLimitAllowsAll(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	e := newEnforcer(t, nil, nil)

	if got := admitted(t, e, account, connector, nil, 25); got != 25 {
		t.Errorf("admitted %d of 25, want all — nothing is configured to limit", got)
	}
}

// TestEnforcerLongMessageExceedingBurstIsAdmitted: a message with more segments than the configured
// burst must still be sendable — the ceiling is raised to the message's cost, so a legitimate long SMS
// is not throttled forever (which would masquerade a structural rejection as a retryable one).
func TestEnforcerLongMessageExceedingBurstIsAdmitted(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	// Connector burst 3 (derived from its throughput); a 6-segment message exceeds it.
	e := newEnforcer(t, nil, []cp.Connector{{ID: connector, ThroughputLimitPerSec: iptr(3)}})
	if err := e.Check(context.Background(), account, connector, nil, 6); err != nil {
		t.Errorf("a 6-segment message against a burst of 3 must be admitted, got %v", err)
	}
}

// TestEnforcerOperationalLimitWinsOverCeiling: an explicit operational rate_limit for a connector takes
// precedence over its (higher) throughput ceiling.
func TestEnforcerOperationalLimitWinsOverCeiling(t *testing.T) {
	account, connector := uuid.New(), uuid.New()
	e := newEnforcer(t, []cp.RateLimitEntry{
		{EntityType: ratelimit.EntityConnector, EntityID: connector, Limit: cp.RateLimit{MaxPerSec: iptr(3)}},
	}, []cp.Connector{
		{ID: connector, ThroughputLimitPerSec: iptr(100)},
	})

	if got := admitted(t, e, account, connector, nil, 10); got != 3 {
		t.Errorf("admitted %d of 10, want the operational limit 3 (not the 100 ceiling)", got)
	}
}
