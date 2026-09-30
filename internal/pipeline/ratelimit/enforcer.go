package ratelimit

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// The rate_limits entity kinds (control_plane.rate_limits.entity_type). The account is admitted before
// the acknowledgement, the connector paces the send (spec §6.4).
const (
	EntityAccount   = "smpp_account"
	EntityConnector = "connector"
)

// The bucket windows. A reroute's republication is paced on its own budget, so a rerouted message pays
// the connector's send budget once, when it is sent.
const (
	windowSend    = "sec"
	windowReroute = "reroute"
)

// minWaitRetry bounds how often a send waiting on a saturated connector polls Redis: without it, N waiters
// poll N x rate times a second. It loses no throughput while the burst holds 5 ms of tokens.
const minWaitRetry = 5 * time.Millisecond

// Lister loads every configured operational limit. *postgres.RateLimitRepo satisfies it.
type Lister interface {
	List(ctx context.Context) ([]cp.RateLimitEntry, error)
}

// ConnectorLister loads the connectors, for their throughput_limit_per_sec hard ceiling.
// *postgres.ConnectorRepo satisfies it.
type ConnectorLister interface {
	List(ctx context.Context) ([]cp.Connector, error)
}

// Snapshot is the immutable rate-limit configuration loaded once at boot (a hot reload is a later
// milestone), indexed by (entity_type, entity_id). A connector with no explicit operational limit gets
// one derived from its throughput_limit_per_sec, so the hard technical ceiling bounds it (spec §6.4,
// §10); a connector that also has no throughput_limit_per_sec (the column is nullable) has no ceiling
// and is not rate-limited — an operator that sets neither has opted out of throttling that connector.
type Snapshot struct {
	limits map[string]cp.RateLimit
}

// LoadSnapshot builds the rate-limit snapshot from the operational limits and the connectors' hard
// ceilings. An operational rate_limit takes precedence over the derived ceiling for the same connector
// (the admin write-validation guarantees the operational value never exceeds the ceiling).
func LoadSnapshot(ctx context.Context, rates Lister, connectors ConnectorLister) (*Snapshot, error) {
	entries, err := rates.List(ctx)
	if err != nil {
		return nil, err
	}
	limits := make(map[string]cp.RateLimit, len(entries))
	for _, e := range entries {
		limits[key(e.EntityType, e.EntityID)] = e.Limit
	}

	conns, err := connectors.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range conns {
		k := key(EntityConnector, c.ID)
		if _, ok := limits[k]; ok {
			continue // an explicit operational limit wins over the derived ceiling
		}
		if c.ThroughputLimitPerSec != nil {
			limits[k] = cp.RateLimit{MaxPerSec: c.ThroughputLimitPerSec}
		}
	}
	return &Snapshot{limits: limits}, nil
}

func key(entityType string, id uuid.UUID) string { return entityType + ":" + id.String() }

func (s *Snapshot) limit(entityType string, id uuid.UUID) (cp.RateLimit, bool) {
	l, ok := s.limits[key(entityType, id)]
	return l, ok
}

// Enforcer applies the account and connector limits, consuming a message's segment count from the
// applicable bucket. The account is checked at admission, the connector at the send.
type Enforcer struct {
	snap    *Snapshot
	limiter *Limiter
}

// NewEnforcer builds an Enforcer over a boot snapshot and the token-bucket limiter.
func NewEnforcer(snap *Snapshot, limiter *Limiter) *Enforcer {
	return &Enforcer{snap: snap, limiter: limiter}
}

// AdmitAccount consumes `segments` tokens from the account's bucket before the submission is
// acknowledged, and returns errs.ErrRateLimited when the account is over its limit. An account with no
// configured limit is admitted. A Redis outage does not surface here: the limiter fails closed against a
// per-pod ceiling (step-084).
func (e *Enforcer) AdmitAccount(ctx context.Context, accountID uuid.UUID, segments int) error {
	if !e.allow(ctx, EntityAccount, accountID, windowSend, segments) {
		return errs.ErrRateLimited
	}
	return nil
}

// WaitConnector blocks until the connector's ceiling has room for one submit_sm, consuming it, so a send
// past the ceiling is slowed rather than refused (spec §6.4 backpressure). It returns ctx.Err() when ctx
// ends first. A connector with no configured limit never waits.
func (e *Enforcer) WaitConnector(ctx context.Context, connectorID uuid.UUID) error {
	limit, ok := e.snap.limit(EntityConnector, connectorID)
	if !ok {
		return nil
	}
	rate, _, limited := toBucket(limit)
	if !limited {
		return nil
	}
	retry := max(time.Second/time.Duration(rate), minWaitRetry)
	for {
		allowed := e.allow(ctx, EntityConnector, connectorID, windowSend, 1)
		// A dead ctx fails the Redis call, and the per-pod fallback may still grant the token.
		if err := ctx.Err(); err != nil {
			return err
		}
		if allowed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// AllowConnector reports whether the connector's republication budget has room for one record right now,
// consuming it when it does. It is the reroute-parking / drain gate (step-126): a reroute that would
// exceed the target connector's throughput is parked rather than piled onto it, and the drainer paces
// the replay against the same rate. It is a budget of its own: the send pays the connector's ceiling
// again at the target. A connector with no configured limit is always allowed.
func (e *Enforcer) AllowConnector(ctx context.Context, connectorID uuid.UUID) bool {
	return e.allow(ctx, EntityConnector, connectorID, windowReroute, 1)
}

func (e *Enforcer) allow(ctx context.Context, entityType string, id uuid.UUID, window string, segments int) bool {
	limit, ok := e.snap.limit(entityType, id)
	if !ok {
		return true // no limit configured for this entity
	}
	rate, capacity, limited := toBucket(limit)
	if !limited {
		return true
	}
	// A message whose segment count exceeds the configured burst must never be PERMANENTLY unsendable —
	// that would masquerade a structural rejection as a retryable throttle (infinite retry / silent
	// drop). Raise the ceiling to the message's cost so it is admitted once that many tokens accrue; the
	// refill rate still bounds the average throughput.
	if capacity < segments {
		capacity = segments
	}
	d := e.limiter.Allow(ctx, entityType, id.String(), window, rate, capacity, segments)
	if d.FailClosed {
		// Redis was unreachable and the per-pod ceiling decided this (step-084). Surface the degraded
		// mode on the span so a throttle in an outage is distinguishable from a real one; the decision
		// itself is honoured either way.
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("rate_limit.fail_closed", true))
	}
	return d.Allowed
}

// toBucket maps an operational limit onto the token bucket's (rate, capacity). A nil MaxPerSec means NO
// per-second limit (the dimension is unlimited), NOT zero — so it reports limited=false and the check is
// skipped. The burst capacity defaults to one second's worth of tokens when it is unset or zero, so a
// configured burst of 0 (which the schema permits) never locks the bucket into denying every message.
// Only the per-second dimension is enforced here; MaxPerDay is carried in the snapshot and applied
// nowhere (debts/max-per-day-expose-jamais-applique.md).
func toBucket(l cp.RateLimit) (rate, capacity int, limited bool) {
	if l.MaxPerSec == nil {
		return 0, 0, false
	}
	rate = *l.MaxPerSec
	capacity = rate
	if l.BurstCapacity != nil && *l.BurstCapacity > 0 {
		capacity = *l.BurstCapacity
	}
	return rate, capacity, true
}
