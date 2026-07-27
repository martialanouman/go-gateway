// Command router-svc runs the MT pipeline: it consumes mt.inbound, normalises and routes each
// message, and publishes mt.routed (M2). A message rejected by the pipeline gets a rejected CDR row
// so get-message reflects it. The lifecycle — load and validate config, install telemetry, load the
// route snapshot, serve the ops port, run the consumer, drain on SIGTERM — follows the canonical
// skeleton (guide de codage §5).
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// serviceName identifies this binary in logs, traces and metrics, and is the consumer group id.
const serviceName = "router-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres for the startup route snapshot, Kafka for the data plane, ClickHouse for rejected
	// CDR rows. No HTTP: the router has no client-facing listener.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse, config.SectionRedis)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	shutdownTracing, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	//nolint:contextcheck // Detaching is the point: see DrainTracing's comment.
	defer observability.DrainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTInbound)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// The route snapshot is loaded once at startup (config-sync's hot reload is a later milestone).
	// Postgres is a hard boot dependency — the router cannot route without routes — so a transient
	// outage must not permanently brick a (re)starting pod: retry with backoff until Postgres answers
	// or the context is cancelled, rather than exiting on the first failure.
	snapshot, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load route snapshot: %w", err)
	}

	// The sender-ID authorization snapshot (§6.19) is a second immutable boot dependency, loaded with
	// the same retry discipline as the routes: the account policies and the customers' active sender
	// IDs, indexed once for lock-free per-message checks.
	senderIDs, err := loadSenderIDSnapshotWithRetry(ctx, postgres.NewAccountRepo(pool), postgres.NewSenderIDRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load sender-id snapshot: %w", err)
	}

	// The opt-out enforcer (§6.20) is a third immutable boot dependency: a per-scope Bloom over the
	// suppressions (exact confirmation behind it) and an inbound-number index to resolve the sending
	// number's scope. Same boot-retry discipline as the routes.
	suppressions := postgres.NewSuppressionRepo(pool)
	optOut, err := loadOptOutEnforcerWithRetry(ctx, suppressions, suppressions, postgres.NewInboundNumberRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load opt-out enforcer: %w", err)
	}

	// The anti-spam engine (§6.20): content rules compiled once at boot, duplicates checked in Redis.
	// Redis is a boot dependency (NewClient pings eagerly), so it retries with the same discipline as
	// the other snapshots — a transient outage must not crashloop a (re)starting pod.
	rdb, err := loadWithRetry(ctx, logger, "redis", func(ctx context.Context) (*goredis.Client, error) {
		return redisstore.NewClient(ctx, cfg.Redis)
	})
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()
	// anti_spam_fail_open_total: bounded (no labels) — counts messages let through because a
	// Redis-backed anti-spam check could not run (§1.5 fail-open). Never a MSISDN or body.
	failOpenTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "anti_spam_fail_open_total",
		Help: "Messages passed (flagged) because a Redis-backed anti-spam check failed open.",
	})
	spam, err := loadAntispamWithRetry(ctx, postgres.NewAntispamRuleRepo(pool), antispam.NewRedisState(rdb), failOpenMetric{c: failOpenTotal}, logger)
	if err != nil {
		return fmt.Errorf("load anti-spam engine: %w", err)
	}

	// The rate-limit snapshot (§6.4): the operational rate_limits plus each connector's
	// throughput_limit_per_sec hard ceiling, indexed once at boot. The token bucket that enforces it
	// lives in Redis (shared across pods) and fails closed on an outage (step-084). Same boot-retry
	// discipline as the other snapshots.
	rateSnap, err := loadWithRetry(ctx, logger, "rate-limit snapshot", func(ctx context.Context) (*ratelimit.Snapshot, error) {
		return ratelimit.LoadSnapshot(ctx, postgres.NewRateLimitRepo(pool), postgres.NewConnectorRepo(pool))
	})
	if err != nil {
		return fmt.Errorf("load rate-limit snapshot: %w", err)
	}
	rateLimiter := ratelimit.NewEnforcer(rateSnap, ratelimit.NewLimiter(rdb))

	// The L0 exact-number short-cut (§6.1): an in-memory Bloom over every exact_routes MSISDN, loaded
	// once at boot with the same retry discipline, in front of the shared Redis map. A ported number
	// routes straight to its target (skipping route resolution only, never compliance); every other
	// number falls through to the declarative resolver. Config-sync's hot reload is a later milestone.
	exactRepo := postgres.NewExactRouteRepo(pool)
	exactBloom, err := loadWithRetry(ctx, logger, "exact-route bloom", func(ctx context.Context) (*exact.Bloom, error) {
		return exact.LoadBloom(ctx, exactRepo)
	})
	if err != nil {
		return fmt.Errorf("load exact-route bloom: %w", err)
	}
	resolver := routing.NewL0Resolver(exact.NewResolver(exactBloom, rdb), snapshot)

	tracer := observability.Tracer(nil, serviceName)
	pl := pipeline.New(tracer, resolver, senderIDs, optOut, spam, rateLimiter)
	rtr := router.New(router.Deps{
		Consumer: consumer,
		Producer: producer,
		Pipeline: pl,
		CDR:      clickhouse.NewCDRWriter(chConn),
		Tracer:   tracer,
		Logger:   logger,
	})

	// Vital dependency (plan §1.5): Kafka alone. The router's core job — consume mt.inbound,
	// normalise, route, publish mt.routed — needs only Kafka and the in-memory snapshot. ClickHouse
	// is touched only to write a rejected CDR row, and Postgres is startup-only (immutable snapshot);
	// gating readiness on either would drain healthy pods over a store the happy path does not use.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(failOpenTotal)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router pipeline tear down together — neither must outlive the other — so the
	// unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("router", rtr.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// loadSnapshotWithRetry loads the route snapshot, retrying transient failures with capped
// exponential backoff until it succeeds or ctx is cancelled. Postgres is a hard boot dependency —
// the router cannot route without routes — so retrying (rather than exiting on the first error)
// keeps a (re)starting pod from being bricked by a transient Postgres outage.
func loadSnapshotWithRetry(ctx context.Context, lister routing.RouteLister, logger *slog.Logger) (*routing.SnapshotResolver, error) {
	return loadWithRetry(ctx, logger, "route snapshot", func(ctx context.Context) (*routing.SnapshotResolver, error) {
		return routing.LoadSnapshot(ctx, lister)
	})
}

// loadSenderIDSnapshotWithRetry loads the sender-ID authorization snapshot, retrying transient
// failures with capped exponential backoff until it succeeds or ctx is cancelled — the same boot
// discipline as the route snapshot (Postgres is a hard boot dependency).
func loadSenderIDSnapshotWithRetry(ctx context.Context, policies senderid.PolicyLister, ids senderid.ActiveSenderIDLister, logger *slog.Logger) (*senderid.Authorizer, error) {
	return loadWithRetry(ctx, logger, "sender-id snapshot", func(ctx context.Context) (*senderid.Authorizer, error) {
		return senderid.LoadSnapshot(ctx, policies, ids)
	})
}

// loadOptOutEnforcerWithRetry loads the opt-out Bloom snapshot and the inbound-number index and
// composes the enforcer, with the same boot-retry discipline.
func loadOptOutEnforcerWithRetry(ctx context.Context, suppressions optout.SuppressionLister, exact optout.ExactChecker, inbound optout.InboundNumberLister, logger *slog.Logger) (*optout.Enforcer, error) {
	return loadWithRetry(ctx, logger, "opt-out enforcer", func(ctx context.Context) (*optout.Enforcer, error) {
		snap, err := optout.LoadSnapshot(ctx, suppressions)
		if err != nil {
			return nil, err
		}
		index, err := optout.LoadInboundNumberIndex(ctx, inbound)
		if err != nil {
			return nil, err
		}
		return optout.NewEnforcer(optout.NewGuard(snap, exact), index), nil
	})
}

// loadAntispamWithRetry loads the anti-spam engine (compiled rule snapshot + Redis shared state),
// with the same boot-retry discipline as the other snapshots.
func loadAntispamWithRetry(ctx context.Context, lister antispam.RuleLister, state antispam.StateStore, metric antispam.Metric, logger *slog.Logger) (*antispam.Engine, error) {
	return loadWithRetry(ctx, logger, "anti-spam engine", func(ctx context.Context) (*antispam.Engine, error) {
		return antispam.New(ctx, lister, state, metric, logger)
	})
}

// failOpenMetric adapts a Prometheus counter to antispam.Metric.
type failOpenMetric struct{ c prometheus.Counter }

func (m failOpenMetric) FailOpen() { m.c.Inc() }

// loadWithRetry loads an immutable boot snapshot, retrying transient failures with capped exponential
// backoff until it succeeds or ctx is cancelled. Postgres is a hard boot dependency, so retrying
// (rather than exiting on the first error) keeps a (re)starting pod from being bricked by a transient
// Postgres outage.
func loadWithRetry[T any](ctx context.Context, logger *slog.Logger, what string, load func(context.Context) (T, error)) (T, error) {
	const (
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for attempt := 1; ; attempt++ {
		v, err := load(ctx)
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			var zero T
			return zero, ctx.Err()
		}
		logger.WarnContext(ctx, what+" load failed, retrying",
			"attempt", attempt, "backoff", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
