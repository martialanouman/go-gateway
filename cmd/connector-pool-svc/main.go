// Command connector-pool-svc is the outbound SMSC leg (M2): a single SMPP bind that consumes
// mt.routed, submits each message, and records the outcome in the CDR. It follows the canonical
// service lifecycle, adding a Kafka consumer, a ClickHouse connection and the SMPP bind.
//
// The bind endpoint is read from the environment here rather than from the connectors control plane:
// the outbound password cannot be recovered from its stored hash, and M2 has no config-sync. This
// env block is the M2 stopgap; M3+ sources connectors from the control plane.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

const serviceName = "connector-pool-svc"

// connectorEnv is the M2 bind configuration. It mirrors the relevant smsc_connectors columns; the
// defaults point at the local fake SMSC.
type connectorEnv struct {
	Addr                 string        `env:"CONNECTOR_ADDR" envDefault:"localhost:2775"`
	SystemID             string        `env:"CONNECTOR_SYSTEM_ID" envDefault:"gateway"`
	Password             string        `env:"CONNECTOR_PASSWORD" envDefault:"gateway"`
	SystemType           string        `env:"CONNECTOR_SYSTEM_TYPE" envDefault:""`
	ID                   uuid.UUID     `env:"CONNECTOR_ID" envDefault:"00000000-0000-0000-0000-000000000001"`
	DialTimeout          time.Duration `env:"CONNECTOR_DIAL_TIMEOUT" envDefault:"5s"`
	ResponseTimeout      time.Duration `env:"CONNECTOR_RESPONSE_TIMEOUT" envDefault:"5s"`
	EnquireLinkInterval  time.Duration `env:"CONNECTOR_ENQUIRE_LINK_INTERVAL" envDefault:"30s"`
	EnquireLinkMaxMissed int           `env:"CONNECTOR_ENQUIRE_LINK_MAX_MISSED" envDefault:"3"`
	WindowSize           int           `env:"CONNECTOR_WINDOW_SIZE" envDefault:"10"`
	// BindPoolSize is the number of parallel SMPP binds (bind_pool_size, 1..32). Sourced from the
	// connectors control plane at M3+; the pool shards mt.routed across the binds (step-124).
	BindPoolSize int `env:"CONNECTOR_BIND_POOL_SIZE" envDefault:"1"`
	// MaxSendRate is the connector's throughput_limit_per_sec, the ceiling for the adaptive throttle
	// (step-086). Zero disables the AIMD pacing. Sourced from the connectors control plane at M3+.
	MaxSendRate float64 `env:"CONNECTOR_MAX_SEND_RATE" envDefault:"0"`
	// Auto-reconnection policy (step-127), mirroring the reconnect_* columns. Disabled by default (opt-in);
	// M3+ sources these from the control plane.
	AutoReconnect         bool          `env:"CONNECTOR_AUTO_RECONNECT" envDefault:"false"`
	ReconnectInitialDelay time.Duration `env:"CONNECTOR_RECONNECT_INITIAL_DELAY" envDefault:"1s"`
	ReconnectMultiplier   float64       `env:"CONNECTOR_RECONNECT_MULTIPLIER" envDefault:"2.0"`
	ReconnectMaxDelay     time.Duration `env:"CONNECTOR_RECONNECT_MAX_DELAY" envDefault:"60s"`
	ReconnectJitterPct    int           `env:"CONNECTOR_RECONNECT_JITTER_PCT" envDefault:"20"`
	ReconnectMaxAttempts  int           `env:"CONNECTOR_RECONNECT_MAX_ATTEMPTS" envDefault:"0"`
	// Dead-letter guards (step-129). RetryWindow dead-letters (retries_exhausted) a message with no
	// fallback chain that keeps hitting a connector-health failure past this window, so a poison record
	// cannot redeliver forever on a dead connector. MaxMessageAge dead-letters (delivery_expired) any
	// message older than this — the gateway's own validity SLA, covering messages that age out in
	// throttle backpressure. Both default off (the pre-step-129 redeliver-forever behaviour); M3+ may
	// source them from the control plane's validity policy.
	RetryWindow   time.Duration `env:"CONNECTOR_RETRY_WINDOW" envDefault:"0"`
	MaxMessageAge time.Duration `env:"CONNECTOR_MAX_MESSAGE_AGE" envDefault:"0"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionKafka, config.SectionClickHouse, config.SectionRedis, config.SectionPostgres)
	if err != nil {
		return err
	}

	var bindEnv connectorEnv
	if err := env.Parse(&bindEnv); err != nil {
		return fmt.Errorf("load connector config: %w", err)
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

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	// Postgres backs the rate-limit snapshot (each connector's throughput_limit_per_sec): the reroute
	// parking gate and the drainer pace against the fallback connectors' ceilings (step-126).
	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	// Per-connector consumer group (step-125, option B): every pool reads all of mt.routed and skips
	// records for other connectors, so a rerouted message reaches the next connector's pool. FromLatest
	// so a brand-new per-connector group does not replay the whole retained topic (mass re-send); a
	// restart still resumes from its committed offset.
	//
	// MIGRATION HAZARD: renaming the group from "connector-pool-svc" to per-connector, with FromLatest,
	// silently DROPS any mt.routed produced-but-not-yet-consumed at cutover (offset old-commit → head)
	// — neither the old nor the new group reads it. Safe on a greenfield/zero-traffic cutover only; a
	// cutover with live traffic MUST first drain mt.routed or seed the new group's offsets from the old
	// group's committed offsets. See docs runbook before the first production deploy.
	consumerGroup := serviceName + "-" + bindEnv.ID.String()
	consumer, err := kafka.NewConsumerFromLatest(cfg.Kafka, consumerGroup, kafka.TopicMTRouted)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// The producer publishes the return path (mo.inbound, dlr.events) durably (acks=all): a deliver_sm
	// from the SMSC is acknowledged only once its MO/DLR is on Kafka (step-043).
	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	// Redis backs the cancel-flag check before each submit_sm: a cancel_sm on smpp-server-svc flags a
	// message here so it is not dispatched. NewClient pings eagerly, so Redis must be reachable at boot
	// (a startup outage crash-loops the pod, as everywhere else). At RUNTIME it is deliberately NOT a
	// readiness dependency and the flag check fails OPEN (connectorpool.handler): a Redis outage lets
	// delivery continue rather than halting all outbound traffic — cancellation is best-effort.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	// Adaptive-throttle metrics (step-086): the current AIMD send rate (gauge) and the count of
	// ESME_RTHROTTLED events (counter). No labels — a pod binds one connector, so these are per-pod
	// (cardinality-bounded, never a message id or MSISDN). Registered with the ops registry below.
	sendRateGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "connector_send_rate",
		Help: "Current adaptive (AIMD) send rate for this connector, in submits per second.",
	})
	throttledTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "connector_throttled_total",
		Help: "ESME_RTHROTTLED responses received from the SMSC for this connector.",
	})
	// connector_dead_letter_total{reason}: messages parked on mt.dead-letter, by gateway reason
	// (fallback_exhausted, retries_exhausted, delivery_expired) — so a dead-lettered message is always
	// counted, never silently lost (step-129). The reason label is a bounded gateway code, never a
	// message id or MSISDN.
	deadLetterTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "connector_dead_letter_total",
		Help: "Messages parked on mt.dead-letter for this connector, labelled by reason.",
	}, []string{"reason"})
	if bindEnv.MaxSendRate > 0 {
		sendRateGauge.Set(bindEnv.MaxSendRate) // start at the ceiling; the AIMD lowers it on throttle
	} else {
		sendRateGauge.Set(math.NaN()) // AIMD disabled: report no value rather than a misleading 0
	}

	// Circuit-breaker aggregate (step-121/122): each bind runs a local breaker fed by its submit
	// outcomes; the pool's heartbeat publishes their state into breaker:state:{connector_id}, which the
	// router reads (step-123) to fence an open connector. podID identifies this pod within the connector's
	// quorum — the pod hostname, unique per replica.
	podID, err := os.Hostname()
	if err != nil || podID == "" {
		podID = serviceName + "-" + uuid.NewString() // unique per replica so quorum fields never collide
	}
	breakerAgg := breaker.NewAggregator(rdb, redisstore.NewPubSubPublisher(rdb), podID)

	// Reroute parking gate (step-126): the per-connector token bucket (shared with the router's
	// rate-limit, so rerouted/drained/fresh traffic compete for one budget) decides whether a reroute
	// can go straight to mt.routed or must be parked and drained at the fallback connector's ceiling.
	rateSnap, err := ratelimit.LoadSnapshot(ctx, postgres.NewRateLimitRepo(pool), postgres.NewConnectorRepo(pool))
	if err != nil {
		return fmt.Errorf("load rate-limit snapshot: %w", err)
	}
	rerouteLimiter := ratelimit.NewEnforcer(rateSnap, ratelimit.NewLimiter(rdb))

	tracer := observability.Tracer(nil, serviceName)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:       consumer,
		CDR:            clickhouse.NewCDRWriter(chConn),
		CancelFlags:    cancel.NewRedisFlags(rdb),
		DLRMap:         dlrmap.NewRedisMap(rdb),
		Producer:       producer,
		Breaker:        breakerAgg,
		BreakerState:   breakerStateReader{rdb: rdb},
		RerouteLimiter: rerouteLimiter,
		Reconnect: reconnect.Config{
			Enabled:      bindEnv.AutoReconnect,
			InitialDelay: bindEnv.ReconnectInitialDelay,
			Multiplier:   bindEnv.ReconnectMultiplier,
			MaxDelay:     bindEnv.ReconnectMaxDelay,
			JitterPct:    bindEnv.ReconnectJitterPct,
			MaxAttempts:  bindEnv.ReconnectMaxAttempts,
		},
		// Hot reconfigure (step-128b): re-read bind_pool_size + reconnect policy from the control plane on
		// each re-dial, publish per-bind status, and poll the reconfigure generation the Admin API bumps.
		ConfigSource:  connectorConfigSource{repo: postgres.NewConnectorRepo(pool)},
		StatusControl: status.NewReader(rdb),
		PodID:         podID,
		ConnectorID:   bindEnv.ID,
		Bind: connectorpool.BindConfig{
			Addr:                 bindEnv.Addr,
			SystemID:             bindEnv.SystemID,
			Password:             bindEnv.Password,
			SystemType:           bindEnv.SystemType,
			DialTimeout:          bindEnv.DialTimeout,
			ResponseTimeout:      bindEnv.ResponseTimeout,
			EnquireLinkInterval:  bindEnv.EnquireLinkInterval,
			EnquireLinkMaxMissed: bindEnv.EnquireLinkMaxMissed,
			WindowSize:           bindEnv.WindowSize,
			BindPoolSize:         bindEnv.BindPoolSize,
		},
		MaxSendRate:   bindEnv.MaxSendRate,
		Throttle:      throttleMetric{rate: sendRateGauge, throttled: throttledTotal},
		DeadLetter:    deadLetterMetric{counter: deadLetterTotal},
		RetryWindow:   bindEnv.RetryWindow,
		MaxMessageAge: bindEnv.MaxMessageAge,
		Tracer:        tracer,
		Logger:        logger,
	})

	// Vital dependencies (plan §1.5): Kafka (no work without it), ClickHouse (the outcome is recorded
	// there) and the SMSC bind itself — the pool cannot deliver a single message without a live bind,
	// and an idle-time bind drop would otherwise leave the pod Ready with nothing behind it.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		producer.ReadyCheck("kafka-producer", cfg.Kafka.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		observability.ReadinessCheck{Name: "smsc-bind", Probe: svc.BindReady},
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	// connector_link_up: the runtime SMSC link_status (1 = up, 0 = down), reported live and kept strictly
	// distinct from the breaker_state (application health). No labels — one connector per pod.
	linkUp := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "connector_link_up",
		Help: "SMSC link status: 1 when the bind is established, 0 when down (distinct from breaker_state).",
	}, func() float64 {
		if svc.LinkStatus() == "up" {
			return 1
		}
		return 0
	})
	ops.Registry().MustRegister(sendRateGauge, throttledTotal, deadLetterTotal, linkUp)

	// Bounded reroute drainer (step-126): consumes mt.reroute-park (AtStart — parked messages are durable
	// and must all be drained) and replays each to mt.routed at the target connector's ceiling. Its own
	// per-connector group; it skips-and-commits records for other connectors.
	drainConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-drain-"+bindEnv.ID.String(), kafka.TopicMTReroutePark)
	if err != nil {
		return fmt.Errorf("kafka drain consumer: %w", err)
	}
	defer drainConsumer.Close()
	drainer := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    drainConsumer,
		Producer:    producer,
		Limiter:     rerouteLimiter,
		ConnectorID: bindEnv.ID,
		Logger:      logger,
	})

	logger.InfoContext(ctx, "starting", "config", cfg, "connector_addr", bindEnv.Addr)

	// Ops and the connector pool tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("connector pool", svc.Run)
	g.Add("reroute-park drainer", drainer.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// connectorConfigSource re-reads a connector's live bind_pool_size + reconnect policy from Postgres so an
// Admin resize / policy change takes effect on the next re-dial (step-128b). The bind endpoint
// (addr/password) still comes from env — the outbound password cannot be recovered from its hash.
type connectorConfigSource struct{ repo *postgres.ConnectorRepo }

func (c connectorConfigSource) Load(ctx context.Context, connectorID uuid.UUID) (int, reconnect.Config, error) {
	conn, err := c.repo.Get(ctx, connectorID)
	if err != nil {
		return 0, reconnect.Config{}, err
	}
	rc := reconnect.Config{
		Enabled:      conn.AutoReconnectEnabled,
		InitialDelay: time.Duration(conn.ReconnectInitialDelayMs) * time.Millisecond,
		Multiplier:   conn.ReconnectMultiplier,
		MaxDelay:     time.Duration(conn.ReconnectMaxDelayMs) * time.Millisecond,
		JitterPct:    conn.ReconnectJitterPct,
		MaxAttempts:  conn.ReconnectMaxAttempts,
	}
	return conn.BindPoolSize, rc, nil
}

// throttleMetric adapts the Prometheus gauge/counter to connectorpool.ThrottleMetric.
type throttleMetric struct {
	rate      prometheus.Gauge
	throttled prometheus.Counter
}

func (m throttleMetric) SetRate(rate float64) { m.rate.Set(rate) }
func (m throttleMetric) IncThrottled()        { m.throttled.Inc() }

// deadLetterMetric adapts the Prometheus counter vector to connectorpool.DeadLetterMetric.
type deadLetterMetric struct{ counter *prometheus.CounterVec }

func (m deadLetterMetric) Inc(reason string) { m.counter.WithLabelValues(reason).Inc() }

// breakerStateReader reads a connector's breaker aggregate (breaker:state:{id}) so a reroute can skip a
// candidate that is itself open (step-125). A missing key or unparsable value reads "not open" (the
// breaker's own default is closed); it is consulted only on a reroute, never on the hot path.
type breakerStateReader struct{ rdb *goredis.Client }

func (b breakerStateReader) IsOpen(ctx context.Context, connectorID uuid.UUID) (bool, error) {
	token, err := b.rdb.Get(ctx, "breaker:state:{"+connectorID.String()+"}").Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return false, nil // no aggregate yet → treat as available
		}
		return false, err
	}
	st, ok := breaker.ParseState(token)
	return ok && st == breaker.Open, nil
}
