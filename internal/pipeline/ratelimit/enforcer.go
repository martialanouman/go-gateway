package ratelimit

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// The rate_limits entity kinds (control_plane.rate_limits.entity_type). A message is checked against
// its account, its route and its connector in that order (spec §6.4).
const (
	EntityAccount   = "smpp_account"
	EntityConnector = "connector"
	EntityRoute     = "route"
)

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

// Enforcer applies the account >= route >= connector rate-limit precedence to a message, consuming its
// segment count from each applicable bucket. It implements the pipeline's rate-limit stage.
type Enforcer struct {
	snap    *Snapshot
	limiter *Limiter
}

// NewEnforcer builds an Enforcer over a boot snapshot and the token-bucket limiter.
func NewEnforcer(snap *Snapshot, limiter *Limiter) *Enforcer {
	return &Enforcer{snap: snap, limiter: limiter}
}

// Check applies the configured limits in precedence order — account, then route (if any), then
// connector — consuming `segments` tokens from each. It returns errs.ErrRateLimited as soon as one is
// exceeded (the connector's technical ceiling is therefore never crossed, even when a looser account or
// route limit would have allowed the message). An entity with no configured limit is skipped. A Redis
// outage does not surface here: the limiter fails closed against a per-pod ceiling (step-084).
func (e *Enforcer) Check(ctx context.Context, accountID, connectorID uuid.UUID, routeID *uuid.UUID, segments int) error {
	if err := e.check(ctx, EntityAccount, accountID, segments); err != nil {
		return err
	}
	if routeID != nil {
		if err := e.check(ctx, EntityRoute, *routeID, segments); err != nil {
			return err
		}
	}
	return e.check(ctx, EntityConnector, connectorID, segments)
}

func (e *Enforcer) check(ctx context.Context, entityType string, id uuid.UUID, segments int) error {
	limit, ok := e.snap.limit(entityType, id)
	if !ok {
		return nil // no limit configured for this entity
	}
	rate, capacity, limited := toBucket(limit)
	if !limited {
		return nil
	}
	// A message whose segment count exceeds the configured burst must never be PERMANENTLY unsendable —
	// that would masquerade a structural rejection as a retryable throttle (infinite retry / silent
	// drop). Raise the ceiling to the message's cost so it is admitted once that many tokens accrue; the
	// refill rate still bounds the average throughput.
	if capacity < segments {
		capacity = segments
	}
	d := e.limiter.Allow(ctx, entityType, id.String(), "sec", rate, capacity, segments)
	if d.FailClosed {
		// Redis was unreachable and the per-pod ceiling decided this (step-084). Surface the degraded
		// mode on the span so a throttle in an outage is distinguishable from a real one; the decision
		// itself is honoured either way.
		trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("rate_limit.fail_closed", true))
	}
	if !d.Allowed {
		return errs.ErrRateLimited
	}
	return nil
}

// toBucket maps an operational limit onto the token bucket's (rate, capacity). A nil MaxPerSec means NO
// per-second limit (the dimension is unlimited), NOT zero — so it reports limited=false and the check is
// skipped. The burst capacity defaults to one second's worth of tokens when it is unset or zero, so a
// configured burst of 0 (which the schema permits) never locks the bucket into denying every message.
// Only the per-second dimension is enforced here; MaxPerDay is carried in the snapshot but a daily
// window bucket is a later milestone (§6.4).
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
