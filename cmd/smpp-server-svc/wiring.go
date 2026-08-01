package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// smppApp is smpp-server-svc fully wired and not yet running: every connection is open, every
// component built, but no socket is accepted, no goroutine started and no port bound. Separating
// "assemble the graph" from "run it" is what makes the wiring testable — a test can build the whole
// service against test dependencies and assert it holds together, without a single bind.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a
// value the caller (or a test) can inspect.
type smppApp struct {
	ops      *observability.OpsServer
	listener *smppserver.Listener
	grpc     *grpc.Server
	rdb      *goredis.Client

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred
	// Closes in run() used to provide.
	closers []func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *smppApp) onClose(f func()) { a.closers = append(a.closers, f) }

// close releases every connection the app holds. It is safe to call on a partially built app: only
// what was actually opened is registered.
func (a *smppApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// newSMPPApp builds the whole graph: stores, the SMPP listener with its throttles and cancel
// handler, the pod-local Deliver gRPC surface and the ops server.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds
// nothing.
func newSMPPApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *smppApp, err error) {
	a := &smppApp{}
	defer func() {
		if err != nil {
			a.close()
		}
	}()

	st, err := openStores(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.onClose(st.close)
	a.rdb = st.rdb

	stack, err := newListener(cfg, st, logger)
	if err != nil {
		return nil, err
	}
	a.onClose(stack.close)
	a.listener = stack.listener

	// The pod-local Deliver gRPC surface: step-048 dials this pod (after a Lookup) to push a deliver_sm
	// to a bind this pod owns. It shares cfg.GRPC.Port (reserved for the SMPP server's registry surface);
	// only Deliver is served here, the rest of SessionRegistry lives in session-manager.
	a.grpc = grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(a.grpc, smppserver.NewDeliverServer(stack.listener, logger))

	a.ops, err = newOpsServer(cfg, logger, st, stack)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores are the external connections smpp-server-svc opens at boot and holds for its whole
// lifetime: PostgreSQL (bind auth), Kafka (produce mt.inbound), ClickHouse (cancel_sm reads and
// writes the CDR), gRPC to session-manager (max_sessions) and Redis (bind throttle).
type stores struct {
	pg       *pgxpool.Pool
	ch       *clickhouse.Conn
	producer *kafka.Producer
	registry *grpc.ClientConn
	rdb      *goredis.Client
}

// openStores opens them in the order a degraded dependency must surface, releasing what it already
// holds if a later one fails.
func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	s.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	s.producer, err = kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	// The SessionRegistry client is a pod-to-pod internal call, so transport security is terminated at
	// the mesh, not here (insecure credentials). NewClient is lazy: it opens no connection until the
	// first bind, so a session-manager that is briefly down does not block startup — a bind during that
	// window simply fails with ESME_RSYSERR.
	s.registry, err = grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial session manager at %q: %w", cfg.SMPP.SessionManagerAddr, err)
	}

	// Redis backs the anti-brute-force throttle on the bind (step-026). It is deliberately NOT a
	// readiness dependency (see newOpsServer): the throttle fails open, so a Redis outage must degrade
	// brute-force protection, not remove the pod from the load balancer.
	s.rdb, err = redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return s, nil
}

// close releases the connections in reverse order of opening; a nil field is one that was never
// opened.
func (s *stores) close() {
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
	if s.registry != nil {
		_ = s.registry.Close()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
}

// listenerStack is the SMPP front door and the metrics it feeds. Building it opens no socket: the
// listener binds in Run.
type listenerStack struct {
	listener *smppserver.Listener

	// throttleBlocked and queryThrottled carry bounded labels only — never a system_id, an IP, a
	// message id or a MSISDN.
	throttleBlocked *prometheus.CounterVec
	queryThrottled  prometheus.Counter

	// stream carries realtime session events (step-184). Best-effort: a separate Kafka client that
	// drops rather than blocks, so a bind is never delayed by a dashboard.
	streamProducer *kafka.StreamProducer
	streamDropped  prometheus.Collector
}

func (l *listenerStack) close() {
	if l.streamProducer != nil {
		l.streamProducer.Close()
	}
}

func newListener(cfg config.Config, st *stores, logger *slog.Logger) (_ *listenerStack, err error) {
	l := &listenerStack{}
	defer func() {
		if err != nil {
			l.close()
		}
	}()

	ingestor := ingest.NewIngestor(st.producer, logger)

	throttle := bindthrottle.New(st.rdb, bindthrottle.Config{
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
		clickhouse.NewCDRReader(st.ch),
		clickhouse.NewCDRWriter(st.ch),
		cancel.NewRedisFlags(st.rdb),
		logger,
	)
	l.throttleBlocked = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smpp_bind_throttle_blocked_total",
		Help: "SMPP binds refused by the anti-brute-force throttle before authentication.",
	}, []string{"subject"}) // bounded label: "system_id" | "ip"

	// Dedicated query_sm rate limit (§6.22, step-087): a per-account token bucket, on a bucket separate
	// from the submit_sm budget, so an intensive querier cannot abuse the SMSC nor eat the send
	// allowance. It reuses the step-084 Redis limiter (shared across pods, fails closed on an outage).
	l.queryThrottled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "smpp_query_throttled_total",
		Help: "query_sm operations refused by the dedicated per-account rate limit.",
	})
	// A zero rate DISABLES the query_sm limit (nil limiter → onQuery answers without throttling), rather
	// than passing rate 0 to the bucket — which would deny every query_sm.
	var queryLimiter smppserver.QueryLimiter
	if cfg.SMPP.QuerySMRatePerSec > 0 {
		queryLimiter = queryRateLimiter{
			limiter:  ratelimit.NewLimiter(st.rdb),
			rate:     cfg.SMPP.QuerySMRatePerSec,
			capacity: cfg.SMPP.QuerySMBurst,
		}
	}

	l.streamProducer, err = kafka.NewStreamProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka stream producer: %w", err)
	}
	l.streamDropped = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "metrics_stream_dropped_total",
		Help: "Realtime records that never reached metrics.stream (full buffer, unreachable broker).",
	}, func() float64 { return float64(l.streamProducer.Dropped()) })
	sessionEvents := metricstream.NewEventPublisher(serviceName, l.streamProducer)

	l.listener = smppserver.New(
		postgres.NewBindRepo(st.pg),
		registrypb.NewSessionRegistryClient(st.registry),
		ingestor,
		smppserver.Options{
			Addr:            fmt.Sprintf(":%d", cfg.SMPP.Port),
			PodID:           podID(cfg, logger),
			SystemID:        serviceName,
			SessionEvents:   sessionEvents,
			IdleTimeout:     cfg.SMPP.IdleTimeout,
			Tracer:          observability.Tracer(nil, serviceName),
			Throttle:        throttle,
			ThrottleBlocked: l.throttleBlocked,
			MaxConns:        cfg.SMPP.MaxConns,
			Canceller:       canceller,
			QueryLimiter:    queryLimiter,
			QueryThrottled:  l.queryThrottled,
			InboundWindow:   cfg.SMPP.InboundWindow,
			// Empty behind an L7-terminated or direct deployment; the balancer's ranges behind an L4 one,
			// without which the per-IP bind throttle degenerates into a global one (step-191).
			TrustedProxyCIDRs: cfg.SMPP.TrustedProxyCIDRs,
		},
		logger,
	)
	return l, nil
}

// newOpsServer builds the ops listener (not yet bound) and registers the metrics this service feeds.
//
// Vital dependencies (plan §1.5): PostgreSQL alone — it gates authenticating a bind, so a pod that
// cannot reach it can accept no session and must leave the LB. Kafka is deliberately NOT vital
// (step-046): a receiver/transceiver bind delivers deliver_sm (MO/DLR) with no Kafka involvement, so
// a Kafka outage must not remove the pod and cut delivery; a submit_sm that cannot be produced fails
// per-PDU with ESME_RSYSERR instead. ClickHouse is likewise not vital (only the best-effort accepted
// CDR row). The SessionRegistry client and Redis (bind throttle, fail-open) are surfaced per-bind,
// not as readiness gates.
func newOpsServer(cfg config.Config, logger *slog.Logger, st *stores, stack *listenerStack) (*observability.OpsServer, error) {
	ops, err := observability.NewOpsServer(cfg, logger,
		postgres.PingCheck("postgres", st.pg, cfg.Postgres.Timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	// The throttle's block counter is this service's first business metric; register it on the ops
	// registry so it surfaces on /metrics. The counter carries no high-cardinality label (never a
	// system_id or an IP), per the ops registry's cardinality rule.
	ops.Registry().MustRegister(stack.throttleBlocked, stack.queryThrottled, stack.streamDropped)
	return ops, nil
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

// queryRateLimiter adapts the step-084 token-bucket limiter to smppserver.QueryLimiter: it consumes
// one token from the account's DEDICATED query_sm bucket (window "query_sm"), separate from the
// submit_sm budget. It fails closed on a Redis outage (the limiter's local ceiling).
type queryRateLimiter struct {
	limiter        *ratelimit.Limiter
	rate, capacity int
}

func (q queryRateLimiter) Allow(ctx context.Context, accountID uuid.UUID) bool {
	capacity := q.capacity
	if capacity <= 0 {
		capacity = q.rate // a burst of 0 would deny every query_sm; default to one second's worth
	}
	return q.limiter.Allow(ctx, ratelimit.EntityAccount, accountID.String(), "query_sm", q.rate, capacity, 1).Allowed
}
