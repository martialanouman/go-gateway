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

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
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
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse)
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
	resolver, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load route snapshot: %w", err)
	}

	tracer := observability.Tracer(nil, serviceName)
	pl := pipeline.New(tracer, resolver)
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
	const (
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for attempt := 1; ; attempt++ {
		resolver, err := routing.LoadSnapshot(ctx, lister)
		if err == nil {
			return resolver, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		logger.WarnContext(ctx, "route snapshot load failed, retrying",
			"attempt", attempt, "backoff", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
