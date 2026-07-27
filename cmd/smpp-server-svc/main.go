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
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
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

	// Dedicated query_sm rate limit (§6.22, step-087): a per-account token bucket, on a bucket separate
	// from the submit_sm budget, so an intensive querier cannot abuse the SMSC nor eat the send
	// allowance. It reuses the step-084 Redis limiter (shared across pods, fails closed on an outage).
	queryThrottled := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "smpp_query_throttled_total",
		Help: "query_sm operations refused by the dedicated per-account rate limit.",
	})
	queryLimiter := queryRateLimiter{
		limiter:  ratelimit.NewLimiter(rdb),
		rate:     cfg.SMPP.QuerySMRatePerSec,
		capacity: cfg.SMPP.QuerySMBurst,
	}

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
			QueryLimiter:    queryLimiter,
			QueryThrottled:  queryThrottled,
		},
		logger,
	)

	// The pod-local Deliver gRPC surface: step-048 dials this pod (after a Lookup) to push a deliver_sm
	// to a bind this pod owns. It shares cfg.GRPC.Port (reserved for the SMPP server's registry surface);
	// only Deliver is served here, the rest of SessionRegistry lives in session-manager.
	grpcServer := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(grpcServer, smppserver.NewDeliverServer(listener, logger))

	// Vital dependencies (plan §1.5): PostgreSQL alone — it gates authenticating a bind, so a pod that
	// cannot reach it can accept no session and must leave the LB. Kafka is deliberately NOT vital
	// (step-046): a receiver/transceiver bind delivers deliver_sm (MO/DLR) with no Kafka involvement, so
	// a Kafka outage must not remove the pod and cut delivery; a submit_sm that cannot be produced fails
	// per-PDU with ESME_RSYSERR instead. ClickHouse is likewise not vital (only the best-effort accepted
	// CDR row). The SessionRegistry client and Redis (bind throttle, fail-open) are surfaced per-bind,
	// not as readiness gates.
	ops, err := observability.NewOpsServer(cfg, logger,
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	// The throttle's block counter is this service's first business metric; register it on the ops
	// registry so it surfaces on /metrics. The counter carries no high-cardinality label (never a
	// system_id or an IP), per the ops registry's cardinality rule.
	ops.Registry().MustRegister(throttleBlocked, queryThrottled)

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
	// The disconnect subscriber force-closes this pod's sessions when a revocation or suspension is
	// fanned out by session-manager (step-032). It is fail-open (a Redis blip degrades disconnects, not
	// binds), so it is registered after the listener — it drains first, before connections are cut.
	g.Add("disconnect subscriber", func(c context.Context) error {
		return smppserver.RunDisconnectSubscriber(c, redisstore.Subscribe(c, rdb, disconnect.Channel), listener, logger)
	})
	// The Deliver gRPC server is registered last so it drains first (reverse order): it stops accepting
	// deliveries before the listener closes the binds they target, so no Deliver races a draining socket.
	g.Add("deliver grpc server", func(c context.Context) error {
		return runGRPC(c, grpcServer, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// podID resolves this pod's registry identity: the configured value, or the OS hostname as a fallback
// (what a Kubernetes pod's hostname already is). A last-resort empty id still lets binds succeed; it
// only makes a token harder to trace to its pod, which a warning flags.
// runGRPC serves the pod-local Deliver surface until ctx is cancelled, then drains within timeout. It
// mirrors session-manager's runGRPC: GracefulStop lets an in-flight Deliver finish, then a hard Stop
// caps the drain so the goroutine always has a stop condition.
func runGRPC(ctx context.Context, srv *grpc.Server, port int, timeout time.Duration, logger *slog.Logger) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("deliver grpc listening", "addr", lis.Addr().String())
		serveErr <- srv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-stopped:
			return nil
		case <-timer.C:
			srv.Stop()
			return nil
		}
	}
}

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

// queryRateLimiter adapts the step-084 token-bucket limiter to smppserver.QueryLimiter: it consumes
// one token from the account's DEDICATED query_sm bucket (window "query_sm"), separate from the
// submit_sm budget. It fails closed on a Redis outage (the limiter's local ceiling).
type queryRateLimiter struct {
	limiter        *ratelimit.Limiter
	rate, capacity int
}

func (q queryRateLimiter) Allow(ctx context.Context, accountID uuid.UUID) bool {
	return q.limiter.Allow(ctx, ratelimit.EntityAccount, accountID.String(), "query_sm", q.rate, q.capacity, 1).Allowed
}
