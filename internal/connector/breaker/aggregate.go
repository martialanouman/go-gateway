package breaker

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

//go:embed aggregate.lua
var aggregateSrc string

// ackSrc records that an aggregate was successfully published, so the next report stops re-publishing
// it. It is deliberately a separate step run only after Publish succeeds — the aggregate is committed
// by aggregate.lua regardless, but the "published" marker advances only on success.
const ackSrc = `redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); return 1`

// Publisher publishes an invalidation signal on a pub/sub channel. It is the consumer-side view of
// *redisstore.PubSubPublisher, kept minimal so the aggregator does not depend on the concrete client.
type Publisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// Aggregator combines every pod's per-connector breaker state into one connector-wide aggregate. Each
// pod reports its own sub-bind states; the aggregate opens only when a strict majority of the LIVE
// sub-binds are open, so one isolated pod cannot fence the whole connector. Sub-binds that stop
// heartbeating are swept from the quorum after TTL. The recompute is a single Lua script (the golden
// rule forbids a read-modify-write from Go), and an invalidation is published on breaker:events only
// when the aggregate actually changes — the router (step-123) consumes it to rebuild its snapshot.
type Aggregator struct {
	script  *redisstore.Script
	ack     *redisstore.Script
	pub     Publisher
	podID   string
	channel string
	ttl     time.Duration // a sub-bind older than this is swept from the quorum
	keyTTL  time.Duration // idle expiry of the Redis keys themselves (a fully dead connector)
	now     func() time.Time
}

// Option tunes an Aggregator.
type Option func(*Aggregator)

// WithTTL sets how long a sub-bind's last report counts toward the quorum before it is swept.
func WithTTL(d time.Duration) Option {
	return func(a *Aggregator) {
		if d > 0 {
			a.ttl = d
		}
	}
}

// WithKeyTTL sets the idle expiry of the aggregation keys (defaults to 4×TTL).
func WithKeyTTL(d time.Duration) Option {
	return func(a *Aggregator) {
		if d > 0 {
			a.keyTTL = d
		}
	}
}

// WithClock injects the clock (tests drive heartbeat expiry deterministically).
func WithClock(now func() time.Time) Option {
	return func(a *Aggregator) {
		if now != nil {
			a.now = now
		}
	}
}

// NewAggregator builds an aggregator for one pod. client is any go-redis Scripter (a *redis.Client
// satisfies it); pub publishes the invalidation (may be nil to disable publishing); podID identifies
// this pod within the connector's quorum.
func NewAggregator(client goredis.Scripter, pub Publisher, podID string, opts ...Option) *Aggregator {
	a := &Aggregator{
		script:  redisstore.NewScript(client, aggregateSrc),
		ack:     redisstore.NewScript(client, ackSrc),
		pub:     pub,
		podID:   podID,
		channel: config.ChannelSnapshotInvalidation,
		ttl:     15 * time.Second,
		now:     time.Now,
	}
	for _, o := range opts {
		o(a)
	}
	if a.keyTTL <= 0 {
		a.keyTTL = 4 * a.ttl
	}
	return a
}

// Report records this pod's sub-bind state for connectorID, recomputes the connector aggregate
// atomically, and — while the aggregate differs from the last published value — publishes an
// invalidation on breaker:events and records the acknowledgement. Publishing is at-least-once: a failed
// publish is retried on the next report rather than dropped. It returns the (possibly unchanged)
// aggregate.
func (a *Aggregator) Report(ctx context.Context, connectorID string, bindIndex int, s State) (State, error) {
	field := fmt.Sprintf("%s:%d", a.podID, bindIndex)
	res, err := a.script.Run(ctx,
		[]string{bindsKey(connectorID), stateKey(connectorID), ackedKey(connectorID)},
		field, s.String(), a.now().UnixMilli(), a.ttl.Milliseconds(), a.keyTTL.Milliseconds(),
	).Result()
	if err != nil {
		return Closed, fmt.Errorf("breaker aggregate: %w", err)
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return Closed, fmt.Errorf("breaker aggregate: unexpected reply %#v", res)
	}
	needsPublish, ok := arr[0].(int64)
	if !ok {
		return Closed, fmt.Errorf("breaker aggregate: non-integer needs_publish %#v", arr[0])
	}
	token, ok := arr[1].(string)
	if !ok {
		return Closed, fmt.Errorf("breaker aggregate: non-string aggregate %#v", arr[1])
	}
	agg, ok := ParseState(token)
	if !ok {
		return Closed, fmt.Errorf("breaker aggregate: unknown state token %q", token)
	}

	if needsPublish == 1 && a.pub != nil {
		if err := a.pub.Publish(ctx, a.channel, []byte(connectorID)); err != nil {
			return agg, fmt.Errorf("breaker aggregate publish: %w", err) // acked not advanced → retried next report
		}
		if err := a.ack.Run(ctx, []string{ackedKey(connectorID)}, token, a.keyTTL.Milliseconds()).Err(); err != nil {
			return agg, fmt.Errorf("breaker aggregate ack: %w", err) // republishes next report (harmless duplicate)
		}
	}
	return agg, nil
}

// bindsKey, stateKey and ackedKey share the {connector_id} hash tag so every key lands on one Cluster
// slot and the recompute stays atomic across them.
func bindsKey(connectorID string) string { return "breaker:binds:{" + connectorID + "}" }
func stateKey(connectorID string) string { return "breaker:state:{" + connectorID + "}" }
func ackedKey(connectorID string) string { return "breaker:acked:{" + connectorID + "}" }
