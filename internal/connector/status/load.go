package status

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// LoadKey is the derived connector-wide in-flight gauge (Appendix B), written by PublishBind's script
// and read by the router's least_loaded strategy. It shares the {connector_id} hash tag of the other
// runtime keys.
func LoadKey(connectorID uuid.UUID) string { return "connectorload:{" + connectorID.String() + "}" }

// LoadStore is the consumer-side slice of *goredis.Client the reader needs; a counting fake in tests.
type LoadStore interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
}

// LoadMeter counts gauge reads by outcome — "hit", "missing" or "error" — so an unpublished gauge is
// visible instead of silently reading as 0 (the blind spot that hid an absent writer for two milestones).
type LoadMeter interface {
	ObserveLoadRead(outcome string)
}

// Load-read outcomes, the closed label set of LoadMeter.
const (
	LoadReadHit     = "hit"
	LoadReadMissing = "missing"
	LoadReadError   = "error"
)

// LoadReader serves a connector's derived in-flight gauge to least_loaded. The strategy resolves per
// message but the gauge is republished only every status heartbeat (2 s), so the reader caches each
// connector's value in memory for cacheTTL: two targets at 8 000 msg/s cost 2 GETs per second per
// pod instead of 16 000. A missing key reads 0 (nothing published yet); a Redis error reads 0 too —
// least_loaded then degrades to its deterministic tie-break, it never blocks routing.
type LoadReader struct {
	store    LoadStore
	cacheTTL time.Duration
	meter    LoadMeter
	logger   *slog.Logger
	now      func() time.Time
	cache    sync.Map // uuid.UUID -> loadEntry
}

type loadEntry struct {
	val   int
	until time.Time
}

// LoadOption tunes a LoadReader.
type LoadOption func(*LoadReader)

// WithLoadCacheTTL sets how long a read value is served from memory; 0 disables the cache (tests
// asserting the key itself).
func WithLoadCacheTTL(d time.Duration) LoadOption {
	return func(r *LoadReader) { r.cacheTTL = d }
}

// WithLoadMeter attaches the read-outcome counter.
func WithLoadMeter(m LoadMeter) LoadOption {
	return func(r *LoadReader) { r.meter = m }
}

// WithLoadClock injects the clock the cache expiry is measured on.
func WithLoadClock(now func() time.Time) LoadOption {
	return func(r *LoadReader) {
		if now != nil {
			r.now = now
		}
	}
}

// WithLoadLogger sets the logger a Redis read error is reported on (rate-limited by the cache TTL).
func WithLoadLogger(l *slog.Logger) LoadOption {
	return func(r *LoadReader) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewLoadReader builds a reader over the shared Redis client (or any LoadStore), caching for 1 s by
// default — half the publish cadence, so a read is never staler than the gauge itself.
func NewLoadReader(store LoadStore, opts ...LoadOption) *LoadReader {
	r := &LoadReader{store: store, cacheTTL: time.Second, logger: slog.Default(), now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

// InFlight satisfies routing.LoadReader: the connector's derived in-flight gauge, 0 when unpublished
// or unreadable.
func (r *LoadReader) InFlight(ctx context.Context, connectorID uuid.UUID) int {
	now := r.now()
	if r.cacheTTL > 0 {
		if e, ok := r.cache.Load(connectorID); ok && now.Before(e.(loadEntry).until) {
			return e.(loadEntry).val
		}
	}
	val := r.read(ctx, connectorID)
	if r.cacheTTL > 0 {
		r.cache.Store(connectorID, loadEntry{val: val, until: now.Add(r.cacheTTL)})
	}
	return val
}

func (r *LoadReader) read(ctx context.Context, connectorID uuid.UUID) int {
	n, err := r.store.Get(ctx, LoadKey(connectorID)).Int()
	switch {
	case err == nil:
		r.observe(LoadReadHit)
		return n
	case errors.Is(err, goredis.Nil):
		r.observe(LoadReadMissing)
		return 0
	default:
		r.observe(LoadReadError)
		r.logger.WarnContext(ctx, "connector load read failed; least_loaded reads 0", "connector_id", connectorID, "err", err)
		return 0
	}
}

func (r *LoadReader) observe(outcome string) {
	if r.meter != nil {
		r.meter.ObserveLoadRead(outcome)
	}
}
