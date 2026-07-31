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
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	billingpb "github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/content"
	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/buildinfo"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
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

	// Postgres (API-key lookup), Kafka (produce mt.inbound), ClickHouse (CDR read/write), Redis
	// (Idempotency-Key window), HTTP (business listener). The deployment sets HTTP_PORT=8080
	// (plan §1.4); the shared default is 8081.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse,
		config.SectionRedis, config.SectionHTTP, config.SectionBilling)
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

	// Redis backs the Idempotency-Key window on POST /messages. Like the other stores, it is required at
	// boot (NewClient pings and fails fast). At runtime, though, a Redis outage fails only idempotent
	// submits — each returns a per-request 503 — so Redis is deliberately NOT wired into /readyz: a blip
	// must not pull the pod out and take reads and non-idempotent submits down with it.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	// Content storage (§6.14, M10, step-162): the accepted-row writer seals the body into the CDR per the
	// customer's content_storage policy. The policy is a boot snapshot (rare changes; lock-free per message);
	// the data key comes from billing-svc (the sole KMS holder) through a TTL cache so a body is encrypted
	// without the body ever reaching billing-svc. The billing dial is lazy — billing-svc down at boot does not
	// block startup; a down billing-svc at runtime degrades an encrypted customer's CDR to no-content (counted).
	contentPolicy, err := content.LoadPolicySnapshot(ctx, postgres.NewCustomerRepo(pool))
	if err != nil {
		return fmt.Errorf("load content-storage policy: %w", err)
	}
	billingConn, err := grpc.NewClient(cfg.Billing.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial billing at %q: %w", cfg.Billing.Addr, err)
	}
	defer func() { _ = billingConn.Close() }()
	dekCache := content.NewDataKeyCache(content.NewGRPCDataKeyFetcher(billingpb.NewContentKeysClient(billingConn)))
	contentDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingest_content_dropped_total",
		Help: "Message bodies dropped from the CDR because the content data key was unavailable.",
	})
	sealer := ingest.NewContentSealer(contentPolicy, dekCache, contentDropped, logger)

	accepted := ingest.NewAcceptedWriter(clickhouse.NewCDRWriter(chConn), sealer, acceptedWorkers, acceptedQueueSize, logger)
	ingestor := ingest.NewIngestor(producer, accepted, logger)
	tracer := observability.Tracer(nil, serviceName)

	handler, _ := restapi.New(restapi.Deps{
		Principals:  postgres.NewAPIKeyRepo(pool),
		Ingestor:    ingestor,
		CDRReader:   clickhouse.NewCDRReader(chConn),
		Accounts:    postgres.NewAccountRepo(pool),
		SenderIDs:   postgres.NewSenderIDRepo(pool),
		RateLimits:  postgres.NewRateLimitRepo(pool),
		Idempotency: idempotency.New(rdb),
		Tracer:      tracer,
		Logger:      logger,
		Version:     buildinfo.Version,
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
	ops.Registry().MustRegister(contentDropped)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ordered shutdown matters here: the HTTP listener must stop and fully drain BEFORE the
	// accepted-row writer, or a request that earns its 202 during the drain would call Accepted.Enqueue
	// after the writer's workers have already exited — silently dropping the accepted CDR row and
	// re-opening the 404 window get-message is meant to close. supervisor.Ordered drains in reverse
	// registration order, so registering ops → writer → http yields the http → writer → ops drain.
	var g supervisor.Ordered
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("accepted writer", accepted.Run)
	g.Add("rest http server", func(c context.Context) error { return runHTTP(c, srv, cfg.ShutdownTimeout, logger) })
	if err := g.Run(ctx, logger); err != nil {
		return err
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
