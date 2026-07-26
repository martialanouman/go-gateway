// Package ratelimit is the atomic token-bucket rate limiter that protects accounts, connectors and
// routes from exceeding their configured throughput (spec §6.4, §10). The bucket lives in Redis so the
// limit is enforced across every pod; the refill-and-consume is one Lua script (the golden rule forbids
// a Go read-modify-write on shared rate state). When Redis is unreachable the limiter FAILS CLOSED
// against a per-pod static ceiling — it never fails open, because an unbounded connector is worse than a
// throttled one. Wiring into the pipeline order and the account >= route >= connector ceiling precedence
// land in step-085; this package is the mechanism.
package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

//go:embed bucket.lua
var bucketSrc string

// defaultTTL bounds how long an idle bucket lingers in Redis. It is refreshed on every call, so an
// active bucket never expires mid-flight; an entity that stops sending simply lets its bucket lapse.
const defaultTTL = time.Hour

// Decision is the outcome of a rate-limit check.
type Decision struct {
	// Allowed is whether the cost was admitted (the tokens were available).
	Allowed bool
	// Remaining is the tokens left in the bucket after this call: the floor of the fractional balance on
	// the healthy path (so it may under-report by up to one token), and 0 on the fail-closed path, which
	// keeps no per-key count worth reporting.
	Remaining int
	// FailClosed is true when Redis was unreachable and the decision came from the per-pod static
	// ceiling instead of the shared bucket. The caller may surface it (a degraded-mode signal) but must
	// treat Allowed the same either way.
	FailClosed bool
}

// Limiter enforces a token bucket per entity. It is safe for concurrent use.
type Limiter struct {
	script *redisstore.Script
	local  *localCeiling
	ttl    time.Duration
	now    func() time.Time
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithClock overrides the clock (tests advance it to exercise refill and the local ceiling
// deterministically).
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// WithTTL overrides the idle-bucket TTL.
func WithTTL(ttl time.Duration) Option {
	return func(l *Limiter) { l.ttl = ttl }
}

// NewLimiter builds a Limiter over a Redis client (any go-redis Scripter).
func NewLimiter(client goredis.Scripter, opts ...Option) *Limiter {
	l := &Limiter{
		script: redisstore.NewScript(client, bucketSrc),
		ttl:    defaultTTL,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(l)
	}
	l.local = &localCeiling{buckets: make(map[string]*localBucket), now: func() time.Time { return l.now() }}
	return l
}

// Allow consumes cost tokens from the bucket identified by (entityType, entityID, window). rate is the
// refill rate in tokens/second and capacity the burst ceiling — both come from the entity's configured
// limit. cost is the number of SEGMENTS the message occupies (step-082), not one per message, so a long
// message counts against the limit as the several SMS it really is. It returns whether the cost was
// admitted. A Redis fault does NOT return an error: it falls back to the per-pod static ceiling
// (fail-closed) and marks the Decision, so the caller's hot path never has to branch on infrastructure.
func (l *Limiter) Allow(ctx context.Context, entityType, entityID, window string, rate, capacity, cost int) Decision {
	nowMs := l.now().UnixMilli()
	res, err := l.script.Run(ctx, []string{redisKey(entityType, entityID, window)},
		nowMs, rate, capacity, cost, l.ttl.Milliseconds()).Result()
	if err != nil {
		// Redis is unreachable (or the context expired): fail closed against the per-pod ceiling.
		return l.failClosed(entityType, entityID, window, rate, capacity, cost)
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return l.failClosed(entityType, entityID, window, rate, capacity, cost)
	}
	allowed, okA := arr[0].(int64)
	remaining, okR := arr[1].(int64)
	if !okA || !okR {
		// A reply of the right length but wrong element types is as untrustworthy as an outage: fail
		// closed rather than silently deny (which would look like a real rate decision).
		return l.failClosed(entityType, entityID, window, rate, capacity, cost)
	}
	return Decision{Allowed: allowed == 1, Remaining: int(remaining)}
}

// failClosed is the per-pod fallback taken whenever the shared bucket cannot be trusted (Redis
// unreachable, or a malformed reply): distributed enforcement is lost, but the entity is still bounded
// per pod rather than wide open. It is never taken for a genuine deny (a well-formed {0, n} reply).
func (l *Limiter) failClosed(entityType, entityID, window string, rate, capacity, cost int) Decision {
	allowed := l.local.allow(localKey(entityType, entityID, window), float64(rate), float64(capacity), float64(cost))
	return Decision{Allowed: allowed, FailClosed: true}
}

// redisKey is the shared bucket key. The whole (entityType:entityID:window) tuple is the Redis Cluster
// hash tag ({...}) so the single-key script stays within one slot while distinct buckets spread across
// slots (key convention, spec Appendix B).
func redisKey(entityType, entityID, window string) string {
	return fmt.Sprintf("ratelimit:{%s:%s:%s}", entityType, entityID, window)
}

// localKey identifies a per-pod fallback bucket (no hash tag needed — it never reaches Redis).
func localKey(entityType, entityID, window string) string {
	return entityType + ":" + entityID + ":" + window
}

// localCeiling is the per-pod fail-closed fallback: an in-memory token bucket per entity, used only
// when Redis is unreachable. It is intentionally NOT distributed — each pod enforces the entity's rate
// independently, so the aggregate ceiling under a Redis outage is (pods x rate), bounded rather than
// open. The bucket set is bounded by the number of configured entities.
type localCeiling struct {
	mu      sync.Mutex
	buckets map[string]*localBucket
	now     func() time.Time
}

type localBucket struct {
	tokens float64
	ts     time.Time
}

// allow refills and consumes a per-pod bucket. It mirrors bucket.lua so the fail-closed path enforces
// the same shape as the shared one.
func (c *localCeiling) allow(key string, rate, capacity, cost float64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	b := c.buckets[key]
	if b == nil {
		b = &localBucket{tokens: capacity, ts: now}
		c.buckets[key] = b
	}
	elapsed := now.Sub(b.ts).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens = math.Min(capacity, b.tokens+elapsed*rate)
	b.ts = now

	if b.tokens >= cost {
		b.tokens -= cost
		return true
	}
	return false
}
