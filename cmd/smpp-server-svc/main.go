// Command smpp-server-svc is the SMPP bind front door (plan §7, M3, SMPP :2775). It accepts ESME
// connections, authenticates each bind against the control plane (PostgreSQL), enforces
// allowed_bind_types, and reserves a session token against the SessionRegistry gRPC service so
// max_sessions is honoured (invariant d). A bound ESME's submit_sm travels the same MT pipeline as a
// REST submit: it is produced durably to mt.inbound (the boundary that earns the submit_sm_resp) and
// its accepted CDR row is written off the connection's path (step-025).
//
// It follows the canonical service lifecycle of cmd/rest-api-svc: PostgreSQL for auth, a gRPC client
// to session-manager for the quota, Kafka for the durable produce, ClickHouse for the accepted CDR
// row, and the SMPP listener supervised alongside the ops port.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "smpp-server-svc"

// Accepted-row worker pool sizing, mirroring rest-api-svc: the pool absorbs bursts off the
// connection's path and is ample for M3.
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

	// PostgreSQL (bind auth), Kafka (produce mt.inbound), ClickHouse (accepted CDR row) and gRPC to
	// session-manager (max_sessions). No HTTP business surface of its own — the SMPP listener is it.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse,
		config.SectionRedis, config.SectionSMPP)
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
		return fmt.Errorf("open postgres pool: %w", err)
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

	// The SessionRegistry client is a pod-to-pod internal call, so transport security is terminated at
	// the mesh, not here (insecure credentials). NewClient is lazy: it opens no connection until the
	// first bind, so a session-manager that is briefly down does not block startup — a bind during that
	// window simply fails with ESME_RSYSERR.
	registryConn, err := grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial session manager at %q: %w", cfg.SMPP.SessionManagerAddr, err)
	}
	defer func() { _ = registryConn.Close() }()

	// Redis backs the anti-brute-force throttle on the bind (step-026). It is deliberately NOT a
	// readiness dependency (see the ops server below): the throttle fails open, so a Redis outage must
	// degrade brute-force protection, not remove the pod from the load balancer.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	throttle := bindthrottle.New(rdb, bindthrottle.Config{
		MaxFailures: cfg.SMPP.BindMaxFailures,
		Window:      cfg.SMPP.BindFailureWindow,
		BackoffBase: cfg.SMPP.BindBackoffBase,
		BackoffMax:  cfg.SMPP.BindBackoffMax,
	})

	// The cancel_sm handler cancels a not-yet-dispatched message: it reads the message's current CDR
	// state (ClickHouse), flags the cancel intent in Redis for the connector pool to honour before
	// submit_sm, and writes the cancelled CDR row. Cancellation is SMPP-only — there is no REST surface
	// (ADR-0009).
	canceller := cancel.NewCanceller(
		clickhouse.NewCDRReader(chConn),
		clickhouse.NewCDRWriter(chConn),
		cancel.NewRedisFlags(rdb),
		logger,
	)
	throttleBlocked := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smpp_bind_throttle_blocked_total",
		Help: "SMPP binds refused by the anti-brute-force throttle before authentication.",
	}, []string{"subject"}) // bounded label: "system_id" | "ip"

	listener := smppserver.New(
		postgres.NewBindRepo(pool),
		registrypb.NewSessionRegistryClient(registryConn),
		ingestor,
		smppserver.Options{
			Addr:            fmt.Sprintf(":%d", cfg.SMPP.Port),
			PodID:           podID(cfg, logger),
			SystemID:        serviceName,
			IdleTimeout:     cfg.SMPP.IdleTimeout,
			Tracer:          observability.Tracer(nil, serviceName),
			Throttle:        throttle,
			ThrottleBlocked: throttleBlocked,
			MaxConns:        cfg.SMPP.MaxConns,
			Canceller:       canceller,
		},
		logger,
	)

	// Vital dependencies (plan §1.5): PostgreSQL gates authenticating a bind, and Kafka gates durably
	// accepting a submit_sm — both remove the pod from the LB when unreachable. ClickHouse is
	// deliberately NOT vital here: unlike rest-api-svc it backs no GET surface, only the best-effort
	// accepted CDR row (Enqueue drops on saturation, the connector's enroute row supersedes it), so a
	// ClickHouse outage must not refuse binds and submits while the durable path (Kafka) is healthy.
	// The SessionRegistry dependency is surfaced per-bind (ESME_RSYSERR) rather than gating readiness,
	// since a lazy gRPC client reports no meaningful state until traffic flows. Redis is likewise NOT a
	// readiness gate: the bind throttle fails open, so a Redis outage weakens brute-force protection but
	// must not pull the pod from the LB and cut SMPP capacity.
	ops, err := observability.NewOpsServer(cfg, logger,
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
		producer.ReadyCheck("kafka", cfg.Kafka.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	// The throttle's block counter is this service's first business metric; register it on the ops
	// registry so it surfaces on /metrics. The counter carries no high-cardinality label (never a
	// system_id or an IP), per the ops registry's cardinality rule.
	ops.Registry().MustRegister(throttleBlocked)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ordered shutdown matters here (as in rest-api-svc): the SMPP listener must stop and fully drain
	// its connections BEFORE the accepted-row writer, or a submit_sm that earns its submit_sm_resp
	// during the drain would Enqueue after the writer's workers have exited — silently dropping the
	// accepted CDR row and re-opening the get-message 404 window (§1.10). supervisor.Ordered drains in
	// reverse registration order, so registering ops → writer → listener yields the listener → writer →
	// ops drain.
	var g supervisor.Ordered
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("accepted writer", accepted.Run)
	g.Add("smpp listener", listener.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// podID resolves this pod's registry identity: the configured value, or the OS hostname as a fallback
// (what a Kubernetes pod's hostname already is). A last-resort empty id still lets binds succeed; it
// only makes a token harder to trace to its pod, which a warning flags.
func podID(cfg config.Config, logger *slog.Logger) string {
	if cfg.SMPP.PodID != "" {
		return cfg.SMPP.PodID
	}
	host, err := os.Hostname()
	if err != nil {
		logger.Warn("smpp: could not resolve hostname for pod id; using empty id", "err", err)
		return ""
	}
	return host
}
