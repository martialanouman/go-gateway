// Command router-svc runs the MT pipeline: it consumes mt.inbound, normalises and routes each
// message, and publishes mt.routed (M2). A message rejected by the pipeline gets a rejected CDR row
// so get-message reflects it. The lifecycle — load and validate config, install telemetry, load the
// route snapshot, serve the ops port, run the consumer, drain on SIGTERM — follows the canonical
// skeleton (guide de codage §5).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
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
	//nolint:contextcheck // Detaching is the point: see drainTracing's comment.
	defer drainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

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
	resolver, err := routing.LoadSnapshot(ctx, postgres.NewRouteRepo(pool))
	if err != nil {
		return fmt.Errorf("load route snapshot: %w", err)
	}

	tracer := observability.Tracer(nil, serviceName)
	pl := pipeline.New(tracer, resolver, "")
	rtr := router.New(router.Deps{
		Consumer: consumer,
		Producer: producer,
		Pipeline: pl,
		CDR:      clickhouse.NewCDRWriter(chConn),
		Tracer:   tracer,
		Logger:   logger,
	})

	// Vital dependencies (plan §1.5): Kafka (no work without it) and ClickHouse (the rejection path
	// writes to it). Postgres is startup-only — the snapshot is immutable — so its loss does not make
	// the router unready.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ops.Run(runCtx, cfg.ShutdownTimeout); err != nil {
			select {
			case errCh <- fmt.Errorf("ops server: %w", err):
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rtr.Run(runCtx); err != nil {
			select {
			case errCh <- fmt.Errorf("router: %w", err):
			default:
			}
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}
	cancel()
	wg.Wait()

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
	}

	logger.Info("stopped")
	return nil
}

// drainTracing flushes buffered spans on the way out, on a context detached from the cancelled
// service context so the shutdown's own spans are not thrown away.
func drainTracing(shutdown observability.ShutdownFunc, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("flush traces on shutdown", "err", err)
	}
}
