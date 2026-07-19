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
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/buildinfo"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

const serviceName = "rest-api-svc"

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

	accepted := ingest.NewAcceptedWriter(clickhouse.NewCDRWriter(chConn), acceptedWorkers, acceptedQueueSize, logger)
	ingestor := ingest.NewIngestor(producer, accepted, logger)
	tracer := observability.Tracer(nil, serviceName)

	handler, _ := restapi.New(restapi.Deps{
		Principals: postgres.NewAPIKeyRepo(pool),
		Ingestor:   ingestor,
		CDRReader:  clickhouse.NewCDRReader(chConn),
		Tracer:     tracer,
		Logger:     logger,
		Version:    buildinfo.Version,
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

	// Ordered shutdown matters here: the HTTP listener must stop and fully drain BEFORE the
	// accepted-row writer, or a request that earns its 202 during the drain would call
	// Accepted.Enqueue after the writer's workers have already exited — silently dropping the
	// accepted CDR row and re-opening the 404 window get-message is meant to close. So each
	// component gets its own cancel, detached from the signal context, and shutdown drives them in
	// order: HTTP → writer → ops. (Cancelling one shared context would stop all three at once.)
	httpCtx, cancelHTTP := context.WithCancel(context.WithoutCancel(ctx))
	writerCtx, cancelWriter := context.WithCancel(context.WithoutCancel(ctx))
	opsCtx, cancelOps := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelOps()
	defer cancelWriter()
	defer cancelHTTP()

	errCh := make(chan error, 3)
	var httpWg, writerWg, opsWg sync.WaitGroup

	supervise := func(wg *sync.WaitGroup, name string, c context.Context, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(c); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}

	supervise(&opsWg, "ops server", opsCtx, func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	supervise(&writerWg, "accepted writer", writerCtx, accepted.Run)
	supervise(&httpWg, "rest http server", httpCtx, func(c context.Context) error { return runHTTP(c, srv, cfg.ShutdownTimeout, logger) })

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}

	// Drain in order: stop the HTTP server and let it finish in-flight requests (their Enqueue lands
	// in the writer's buffer), then stop the writer so it drains that buffer, then the ops server.
	cancelHTTP()
	httpWg.Wait()
	cancelWriter()
	writerWg.Wait()
	cancelOps()
	opsWg.Wait()

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
