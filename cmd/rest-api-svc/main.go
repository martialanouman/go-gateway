// Command rest-api-svc serves the public REST API (plan §1.4, business port 8080; ops 9090). It
// follows the canonical lifecycle of cmd/router-svc, adding a Postgres pool (API-key lookup), a
// Kafka producer (mt.inbound), a ClickHouse connection (CDR read/write) and the accepted-row worker
// pool, plus a client-facing HTTP listener — each a supervised component.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

const serviceName = "rest-api-svc"

// version is reported by the public health endpoint. A real build stamps it via -ldflags.
var version = "dev"

// Accepted-row worker pool sizing. The pool absorbs bursts off the request path; these are ample for
// M2 and become configurable when throughput tuning matters.
const (
	acceptedWorkers   = 4
	acceptedQueueSize = 1024
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres (API-key lookup), Kafka (produce mt.inbound), ClickHouse (CDR read/write), HTTP
	// (business listener). The deployment sets HTTP_PORT=8080 (plan §1.4); the shared default is 8081.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse, config.SectionHTTP)
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

	accepted := restapi.NewAcceptedWriter(clickhouse.NewCDRWriter(chConn), acceptedWorkers, acceptedQueueSize, logger)
	tracer := observability.Tracer(nil, serviceName)

	handler, _ := restapi.New(restapi.Deps{
		Principals: postgres.NewAPIKeyRepo(pool),
		Producer:   producer,
		CDRReader:  clickhouse.NewCDRReader(chConn),
		Accepted:   accepted,
		Tracer:     tracer,
		Logger:     logger,
		Version:    version,
	})

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.HTTP.Port),
		Handler:           handler,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
	}

	// Vital dependencies (plan §1.5): Kafka and Postgres gate accepting a message (POST), ClickHouse
	// gates reading its status (GET). All three remove the pod from the LB when unreachable.
	ops, err := observability.NewOpsServer(cfg, logger,
		producer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	supervise := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(runCtx); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}

	supervise("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	supervise("accepted writer", accepted.Run)
	supervise("rest http server", func(c context.Context) error { return runHTTP(c, srv, cfg.ShutdownTimeout, logger) })

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

// runHTTP serves the public API until ctx is cancelled, then drains within timeout.
func runHTTP(ctx context.Context, srv *http.Server, timeout time.Duration, logger *slog.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("rest api listening", "addr", srv.Addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("drain rest http server: %w", err)
		}
		return nil
	}
}

// drainTracing flushes buffered spans on the way out, on a context detached from the cancelled
// service context.
func drainTracing(shutdown observability.ShutdownFunc, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("flush traces on shutdown", "err", err)
	}
}
