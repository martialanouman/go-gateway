package exact

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// DefaultCacheTTL bounds how long a cached exact route may outlive a durable change whose invalidation
// was lost. It is a safety net, not the coherence mechanism — the Admin API DELs the key after every
// commit (step-250e). Long enough that the steady-state Postgres read rate is one lookup per active
// ported number per TTL; short enough that a missed DEL heals the same working day.
const DefaultCacheTTL = 6 * time.Hour

// DefaultLookupTimeout bounds ONE durable lookup. The Redis leg already has an equivalent (the client's
// ReadTimeout); the durable leg had none — the pool sets only a connect timeout, the pipeline stage adds
// no deadline, and the consumer's context is long-lived. Without this, a Postgres that absorbs without
// answering hangs the lane, and the lane is the Kafka partition: no error, no redelivery, no metric.
// Generous on purpose — it is a hang detector, not a latency budget.
const DefaultLookupTimeout = 2 * time.Second

// cacheJitterPct spreads expiry across keys warmed in the same burst — after a Redis loss the whole
// working set repopulates within seconds, and a fixed TTL would then expire it in lockstep.
const cacheJitterPct = 10

// RedisCache is the surface the resolver needs from Redis: read a cached route, and populate one on a
// miss. A *goredis.Client satisfies it.
type RedisCache interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *goredis.StatusCmd
}

// LookupMeter counts L0 lookups by outcome, ONE observation per resolution — bloom_miss, redis_hit,
// cache_corrupt, pg_hit, pg_miss. The vocabulary is closed and declared in code, never derived from
// traffic, so it satisfies the bounded-label guard (§15). One-per-resolution is what makes the sum a
// lookup count and the ratios usable, which is the whole point of shipping it (ADR-0015).
// It is what makes the follow-ups this step defers decidable: whether a negative cache earns its keep
// depends on the pg_miss rate, and the TTL on the pg_hit rate.
type LookupMeter interface {
	Observe(outcome string)
}

// LookupOutcomes is the closed vocabulary of Observe, in code and nowhere else. It is exported so the
// wiring can seed every series at boot: a counter vector exposes nothing until it has a child, and a
// ratio like pg_miss/pg_hit is unusable while one of its terms is simply absent from /metrics.
func LookupOutcomes() []string {
	return []string{outcomeBloomMiss, outcomeRedisHit, outcomeCacheCorrupt, outcomePgHit, outcomePgMiss}
}

// Option overrides a Resolver default. Only the meter is optional: a resolver without one still routes.
type Option func(*Resolver)

// WithLookupMeter attaches the outcome counter.
func WithLookupMeter(m LookupMeter) Option { return func(r *Resolver) { r.meter = m } }

// WithLookupTimeout overrides the durable-lookup bound. Mainly for tests, which cannot wait out the
// default.
func WithLookupTimeout(d time.Duration) Option {
	return func(r *Resolver) {
		if d > 0 {
			r.lookupTimeout = d
		}
	}
}

// The outcome labels. Constants rather than literals: they are named in the resolver, in the boot
// seeding and in the catalogue's Help text, and a typo in any one of them would split a series in
// silence.
const (
	outcomeBloomMiss    = "bloom_miss"
	outcomeRedisHit     = "redis_hit"
	outcomeCacheCorrupt = "cache_corrupt"
	outcomePgHit        = "pg_hit"
	outcomePgMiss       = "pg_miss"
)

// nopMeter is the default, so the hot path needs no nil check.
type nopMeter struct{}

func (nopMeter) Observe(string) {}

// RouteStore is the durable source of truth behind the cache. *postgres.ExactRouteRepo satisfies it
// structurally; the lookup is by primary key.
type RouteStore interface {
	Get(ctx context.Context, msisdn string) (Route, bool, error)
}

// Resolver is the L0 exact-number lookup: a per-pod Bloom gate in front of exactroute:{msisdn}
// (Appendix B), which is a read-through cache over the durable exact_routes table.
//
// A Bloom miss is a definitive "no override" answered without any network call (~99% of traffic). A
// Bloom possible-hit reads Redis; on a cache miss the durable table decides, and a row found there
// populates the cache for the next message. The control plane never writes a value here — it DELs the
// key after its own commit — so Redis is a cache that can always be rebuilt, never a second source of
// truth (ADR-0015).
type Resolver struct {
	bloom         *Bloom
	cache         RedisCache
	store         RouteStore
	ttl           time.Duration
	lookupTimeout time.Duration
	meter         LookupMeter
}

// NewResolver builds the L0 resolver over a boot-loaded Bloom, the Redis cache and the durable store.
// A non-positive ttl falls back to DefaultCacheTTL.
func NewResolver(bloom *Bloom, cache RedisCache, store RouteStore, ttl time.Duration, opts ...Option) *Resolver {
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	r := &Resolver{
		bloom: bloom, cache: cache, store: store, ttl: ttl,
		lookupTimeout: DefaultLookupTimeout, meter: nopMeter{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// redisKey is the exact-route key for an MSISDN (Appendix B). The hash tag ({msisdn}) pins a number's
// key to one cluster slot — which also means keys cannot be written or deleted in one cross-slot
// command, hence the pipelining in the Invalidator.
func redisKey(msisdn string) string { return "exactroute:{" + msisdn + "}" }

// Resolve returns the exact-route target for an MSISDN. ok is false for the common "no override" case
// — a Bloom miss, or a possible-hit the durable table does not know (a false positive) — and the
// caller then falls back to normal route resolution.
//
// A non-nil error is transient and uncoded, from either leg: an exact route that cannot be reached
// must not degrade into default routing, which for a ported number means its former operator (§16).
// Only a MISSING key falls through to the store; a Redis fault is returned. Falling through on a fault
// would move the hot path onto the control-plane database at full message rate during an outage, and
// is an open question rather than a silent default.
func (r *Resolver) Resolve(ctx context.Context, msisdn string) (Target, bool, error) {
	if !r.bloom.MightContain(msisdn) {
		r.meter.Observe(outcomeBloomMiss)
		return Target{}, false, nil // definitive miss: no network call at all
	}

	val, err := r.cache.Get(ctx, redisKey(msisdn)).Result()
	switch {
	case err == nil:
		target, perr := parseTarget(val)
		if perr == nil {
			r.meter.Observe(outcomeRedisHit)
			return target, true, nil
		}
		// An illegible value is treated as a miss, not as a fault. Surfacing it would send the message
		// back to the same key on every redelivery, wedging the whole partition until the TTL expires;
		// the durable table is right here, and reading it rewrites a healthy value. Counted, so a drift
		// never heals invisibly.
		return r.loadAndCache(ctx, msisdn, outcomeCacheCorrupt)
	case !errors.Is(err, goredis.Nil):
		return Target{}, false, fmt.Errorf("exact: redis lookup: %w", err)
	}

	return r.loadAndCache(ctx, msisdn, "")
}

// loadAndCache resolves a cache miss against the durable table and populates the key on a hit. A miss
// in the table is a Bloom false positive: it falls back without caching anything, since this step
// ships no negative caching and a key written here would be a route to nowhere.
// outcome is the label to record instead of the usual pg_hit/pg_miss, empty for the ordinary cache
// miss. It keeps the metric at exactly one observation per resolution: a healed corrupt value did
// perform a durable read, but counting it as pg_hit too would inflate the very ratios ADR-0015 defers
// two decisions to.
func (r *Resolver) loadAndCache(ctx context.Context, msisdn, outcome string) (Target, bool, error) {
	route, found, err := r.lookup(ctx, msisdn)
	if err != nil {
		return Target{}, false, transient(err)
	}
	if !found {
		r.meter.Observe(cmp.Or(outcome, outcomePgMiss))
		return Target{}, false, nil
	}

	// A cache write that fails must not fail the message: the target is already known and correct, and
	// the next message simply pays another lookup. Only the read legs are fail-closed.
	_ = r.cache.Set(ctx, redisKey(msisdn), EncodeTarget(route.Target), jitterTTL(r.ttl)).Err()
	r.meter.Observe(cmp.Or(outcome, outcomePgHit))
	return route.Target, true, nil
}

// transient reports a durable-lookup failure WITHOUT the error code the repository attached.
//
// postgres.translate maps every infrastructure fault to errs.ErrInternal, and a CODED error means
// something precise downstream: router.handle writes a CDR `rejected` and COMMITS the Kafka offset,
// burying the message. On this path that is exactly backwards (§16) — an exact route that cannot be
// reached is transient, and the message must be redelivered rather than rejected.
//
// The chain is dropped on purpose, not by oversight. Re-wrapping this with %w restores the burial in
// silence; TestExactRouteFailsClosedWhenPostgresIsCut is what refuses it, against a really cut
// Postgres, because a fake returning a bare error has the shape of an outage and none of its contract.
func transient(err error) error {
	return errors.New("exact: durable lookup: " + err.Error())
}

// lookup reads the durable table under its own deadline. The bound is scoped to THIS call and released
// before the cache write that follows: sharing one budget would leave the populate whatever the lookup
// did not use — a sliver, after a slow Postgres — so the cache would start failing to fill exactly when
// the system is slow, and every following message would pay another slow lookup.
func (r *Resolver) lookup(ctx context.Context, msisdn string) (Route, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.lookupTimeout)
	defer cancel()

	return r.store.Get(ctx, msisdn)
}

// jitterTTL randomises d by ±cacheJitterPct%, uniformly. Same idiom as the reconnect backoff's own
// jitter, for the same reason: it de-synchronises expiry, and it is not security-sensitive.
func jitterTTL(d time.Duration) time.Duration {
	// A non-positive duration would reach go-redis as expiration 0, which means NO EXPIRY — the
	// unbounded-staleness state this design refuses. The constructor already clamps ttl, but the floor
	// belongs here too: it makes the property structural rather than a convention held eighty lines away.
	if d <= 0 {
		return DefaultCacheTTL
	}
	span := float64(cacheJitterPct) / 100.0
	//nolint:gosec // G404: TTL jitter de-synchronises cache expiry, it is not a secret
	factor := 1 + (rand.Float64()*2-1)*span // uniform in [1-span, 1+span]
	return time.Duration(float64(d) * factor)
}

// parseTarget decodes the Redis value "<target_type>:<uuid>" into a Target — the Appendix B encoding.
// A malformed or unknown value is an error, not a silent miss, so a sync bug surfaces rather than
// mis-routing.
func parseTarget(val string) (Target, error) {
	kind, id, ok := strings.Cut(val, ":")
	if !ok {
		return Target{}, fmt.Errorf("exact: malformed target %q", val)
	}
	tt := TargetType(kind)
	if tt != TargetConnector && tt != TargetRoute {
		return Target{}, fmt.Errorf("exact: unknown target type %q", kind)
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return Target{}, fmt.Errorf("exact: target id %q: %w", id, err)
	}
	return Target{Type: tt, ID: uid}, nil
}

// EncodeTarget renders a Target as the Redis cache value. It is the inverse of parseTarget and lives
// here so the encoding has exactly one owner: the resolver populating the cache and the Invalidator
// clearing it both go through this file, and no other package knows the wire form.
func EncodeTarget(t Target) string {
	return string(t.Type) + ":" + t.ID.String()
}
