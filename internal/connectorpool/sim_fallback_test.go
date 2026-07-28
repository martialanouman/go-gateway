package connectorpool_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

// TestSimBreakerFallbackParkReplay is the step-130d headline acceptance scenario (plan §12): a connector
// whose SMSC never answers in time drives its breaker open; because the message carries a fallback chain,
// the pool reroutes to a healthy connector; the healthy connector's throughput ceiling forces the excess
// onto the parking topic; and the drainer replays the parked messages until every one is delivered —
// proving degraded → open → fallback → park → replay end-to-end against the real simulator.
//
// The degraded connector uses SLOW-CARRIER (a 2s fixed latency) with the pool's response timeout at
// 300ms, so every submit RELIABLY times out (b.Submit's response timer fires long before the simulator
// answers) — a deterministic connector-health failure.
func TestSimBreakerFallbackParkReplay(t *testing.T) {
	rdb := redistest.Client(t)
	slowSim := smscsim.Launch(t, smscsim.SlowCarrierConfig("slow", "pw", 2000))
	healthySim := smscsim.Launch(t, smscsim.HealthyConfig("live", "pw"))

	connA, connB := uuid.New(), uuid.New()

	// Healthy connector B: submits whatever is rerouted/drained to it.
	poolB := startSimPool(t, simPoolConfig{
		BindAddr: healthySim.SMPPAddr, SystemID: "live", Password: "pw", ConnID: connB,
	})
	// Degraded connector A: breaker wired, and a reroute gate capping B at 1 segment/s so the burst cannot
	// all go straight through — the excess parks and drains.
	limiterB := rerouteLimiterFor(t, rdb, connB, 1)
	poolA := startSimPool(t, simPoolConfig{
		BindAddr: slowSim.SMPPAddr, SystemID: "slow", Password: "pw", ConnID: connA, PodID: "podA",
		BreakerConfig: scenarioBreakerConfig, RerouteLimiter: limiterB, ResponseTimeout: 300 * time.Millisecond,
		Redis: rdb,
	})
	startDrainer(t, rdb, connB, limiterB) // replays B's parked reroutes at B's ceiling
	parked := startParkCounter(t, connB)  // proves parking happened

	slowSim.WaitBindCount(t, "carrier", 1, 15*time.Second)
	healthySim.WaitBindCount(t, "carrier", 1, 15*time.Second)
	waitGroupStable(t, poolA.group, 1, 15*time.Second)
	waitGroupStable(t, poolB.group, 1, 15*time.Second)

	// Inject a burst addressed to A, each carrying the fallback chain [B].
	const burst = 8
	ids := make([]routedIdent, burst)
	for i := range ids {
		ids[i] = poolA.injectRoutedTo(t, connA, []uuid.UUID{connB})
	}

	// (a) A's breaker opened.
	waitBreakerState(t, rdb, connA, breaker.Open, 20*time.Second)
	// (b) at least one message was parked (B's ceiling forced the excess off the direct path).
	waitAtLeast(t, "parked records", func() int64 { return parked.Load() }, 1, 25*time.Second)
	// (c) every message eventually reached B and was delivered — nothing lost through park and replay.
	for _, id := range ids {
		poolB.waitCDRStatus(t, id, clickhouse.StatusEnroute, 40*time.Second)
	}
}

// rerouteLimiterFor builds a RerouteLimiter giving connID a hard throughput ceiling of perSec (and no
// other connector any limit), backed by the shared Redis token bucket — so a burst of reroutes to connID
// beyond perSec is parked and drained at that rate. It reuses the production Enforcer over an in-memory
// snapshot, so no Postgres is needed.
func rerouteLimiterFor(t *testing.T, rdb *goredis.Client, connID uuid.UUID, perSec int) connectorpool.RerouteLimiter {
	t.Helper()
	snap, err := ratelimit.LoadSnapshot(context.Background(),
		staticRates(nil),
		staticConnectors{{ID: connID, ThroughputLimitPerSec: &perSec}},
	)
	if err != nil {
		t.Fatalf("load rate snapshot: %v", err)
	}
	return ratelimit.NewEnforcer(snap, ratelimit.NewLimiter(rdb))
}

// staticRates / staticConnectors are in-memory Listers for the rate-limit snapshot (no operational
// limits; connectors carry only their technical ceiling).
type staticRates []cp.RateLimitEntry

func (s staticRates) List(context.Context) ([]cp.RateLimitEntry, error) { return s, nil }

type staticConnectors []cp.Connector

func (s staticConnectors) List(context.Context) ([]cp.Connector, error) { return s, nil }

// startDrainer runs a reroute-park drainer for connID: it consumes mt.reroute-park (AtStart — the topic's
// backlog is negligible, so a parked record produced before the consumer joined is still drained) and
// replays each to mt.routed, paced at the target's ceiling by limiter (step-126). It turns "parked" into
// "eventually delivered". Ordered teardown via t.Cleanup.
func startDrainer(t *testing.T, rdb *goredis.Client, connID uuid.UUID, limiter connectorpool.RerouteLimiter) {
	t.Helper()
	kafkaCfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		t.Fatalf("drainer producer: %v", err)
	}
	consumer, err := kafka.NewConsumer(kafkaCfg, "sim-drain-"+connID.String(), kafka.TopicMTReroutePark)
	if err != nil {
		t.Fatalf("drainer consumer: %v", err)
	}
	drainer := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer: consumer, Producer: producer, Limiter: limiter, ConnectorID: connID,
		Retry: 100 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = drainer.Run(ctx) }()
	t.Cleanup(func() { cancel(); wg.Wait(); producer.Close(); consumer.Close() })
}

// startParkCounter runs a dedicated consumer counting records parked on mt.reroute-park for connID (its
// own group, AtStart, filter by connID), so a test can assert parking happened independently of the
// drainer that consumes and replays them.
func startParkCounter(t *testing.T, connID uuid.UUID) *atomic.Int64 {
	t.Helper()
	kafkaCfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	consumer, err := kafka.NewConsumer(kafkaCfg, "sim-parkcount-"+uuid.NewString(), kafka.TopicMTReroutePark)
	if err != nil {
		t.Fatalf("park-counter consumer: %v", err)
	}
	var n atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.RunBatch(ctx, func(_ context.Context, recs []kafka.Record) []error {
			out := make([]error, len(recs))
			for _, rec := range recs {
				if routed, derr := pipeline.DecodeRouted(rec); derr == nil && routed.ConnectorID == connID {
					n.Add(1)
				}
			}
			return out
		})
	}()
	t.Cleanup(func() { cancel(); wg.Wait(); consumer.Close() })
	return &n
}

// waitAtLeast polls get() until it reaches want, or fails at the deadline — for threshold assertions on a
// live counter (never an exact count, which a token bucket or a race would make flaky).
func waitAtLeast(t *testing.T, what string, get func() int64, want int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last int64
	for time.Now().Before(deadline) {
		last = get()
		if last >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never reached %d within %s (last: %d)", what, want, within, last)
}
