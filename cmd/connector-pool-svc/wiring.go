package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/connectorpool/settle"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// poolApp is connector-pool-svc fully wired and not yet running: every connection is open, every
// component built, but no SMPP bind is dialled, no goroutine started and no port bound. Separating
// "assemble the graph" from "run it" is what makes the wiring testable — a test can build the whole
// service against test dependencies and assert it holds together, without a single submit_sm.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a
// value the caller (or a test) can inspect.
type poolApp struct {
	ops      *observability.OpsServer
	pool     *connectorpool.Service
	drainer  *connectorpool.Drainer
	emitter  *metricstream.Emitter
	consumer *kafka.Consumer
	catalog  *metrics.Catalog

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred
	// Closes in run() used to provide. They are named because that order is the property worth
	// guarding, and an anonymous stack cannot be asserted against.
	closers []closer
}

// closer is a release step and the name it answers to. The name carries no behaviour: it exists so
// that the release ORDER — a property of newPoolApp, and one a wrong edit breaks silently — can be
// asserted on the graph the service actually builds.
type closer struct {
	name string
	fn   func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *poolApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name: name, fn: f})
}

// close releases every connection the app holds. It is safe to call on a partially built app: only
// what was actually opened is registered.
func (a *poolApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}

// newPoolApp builds the whole connector-pool graph: stores, throttle metrics, the breaker aggregate,
// the reroute gate, billing settle, the realtime feed, the pool itself, the ops server and the
// reroute drainer — in that order, which is the order in which a degraded dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds
// nothing.
func newPoolApp(ctx context.Context, cfg config.Config, bindEnv connectorEnv, logger *slog.Logger) (_ *poolApp, err error) {
	a := &poolApp{}
	defer func() {
		if err != nil {
			a.close()
		}
	}()

	st, err := openStores(ctx, cfg, bindEnv.ID)
	if err != nil {
		return nil, err
	}
	a.onClose("stores", st.close)
	a.consumer = st.consumer

	throttle := newThrottleMetrics(bindEnv)

	// Circuit-breaker aggregate (step-121/122): each bind runs a local breaker fed by its submit
	// outcomes; the pool's heartbeat publishes their state into breaker:state:{connector_id}, which the
	// router reads (step-123) to fence an open connector. podID identifies this pod within the connector's
	// quorum — the pod hostname, unique per replica.
	podID, err := os.Hostname()
	if err != nil || podID == "" {
		podID = serviceName + "-" + uuid.NewString() // unique per replica so quorum fields never collide
	}
	breakerAgg := breaker.NewAggregator(st.rdb, redisstore.NewPubSubPublisher(st.rdb), podID)

	// Reroute parking gate (step-126): the per-connector token bucket (shared with the router's
	// rate-limit, so rerouted/drained/fresh traffic compete for one budget) decides whether a reroute
	// can go straight to mt.routed or must be parked and drained at the fallback connector's ceiling.
	rateSnap, err := ratelimit.LoadSnapshot(ctx, postgres.NewRateLimitRepo(st.pg), postgres.NewConnectorRepo(st.pg))
	if err != nil {
		return nil, fmt.Errorf("load rate-limit snapshot: %w", err)
	}
	rerouteLimiter := ratelimit.NewEnforcer(rateSnap, ratelimit.NewLimiter(st.rdb))

	billing, err := newSettler(cfg, logger)
	if err != nil {
		return nil, err
	}
	a.onClose("billing", billing.close)

	tracer := observability.Tracer(nil, serviceName)
	stream, err := newMetricStream(cfg)
	if err != nil {
		return nil, err
	}
	a.onClose("stream", stream.close)
	a.emitter = stream.emitter
	a.catalog = metrics.NewCatalog()

	a.pool = connectorpool.New(connectorpool.Deps{
		Consumer:       st.consumer,
		Stream:         stream.emitter,
		BreakerGauge:   a.catalog,
		Metrics:        a.catalog,
		CDR:            clickhouse.NewCDRWriter(st.ch),
		CancelFlags:    cancel.NewRedisFlags(st.rdb),
		DLRMap:         dlrmap.NewRedisMap(st.rdb),
		Producer:       st.producer,
		Breaker:        breakerAgg,
		BreakerState:   breakerStateReader{rdb: st.rdb},
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
		ConfigSource:  connectorConfigSource{repo: postgres.NewConnectorRepo(st.pg)},
		StatusControl: status.NewReader(st.rdb),
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
		Throttle:      throttleMetric{rate: throttle.sendRate, throttled: throttle.throttledTotal},
		DeadLetter:    deadLetterMetric{counter: throttle.deadLetterTotal},
		Billing:       billing.settler,
		RetryWindow:   bindEnv.RetryWindow,
		MaxMessageAge: bindEnv.MaxMessageAge,
		Tracer:        tracer,
		Logger:        logger,
	})

	a.ops, err = newOpsServer(cfg, logger, st, a.pool, a.catalog, throttle, billing, stream)
	if err != nil {
		return nil, err
	}

	drainer, err := newDrainer(cfg, st, rerouteLimiter, bindEnv.ID, logger)
	if err != nil {
		return nil, err
	}
	a.onClose("drainer", drainer.close)
	a.drainer = drainer.drainer
	return a, nil
}

// stores are the external connections connector-pool-svc opens at boot and holds for its whole
// lifetime.
type stores struct {
	ch       *clickhouse.Conn
	pg       *pgxpool.Pool
	consumer *kafka.Consumer
	producer *kafka.Producer
	rdb      *goredis.Client
}

// openStores opens them in the order a degraded dependency must surface, releasing what it already
// holds if a later one fails.
func openStores(ctx context.Context, cfg config.Config, connectorID uuid.UUID) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	// Postgres backs the rate-limit snapshot (each connector's throughput_limit_per_sec): the reroute
	// parking gate and the drainer pace against the fallback connectors' ceilings (step-126).
	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

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
	s.consumer, err = kafka.NewConsumerFromLatest(cfg.Kafka, serviceName+"-"+connectorID.String(), kafka.TopicMTRouted)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	// The producer publishes the return path (mo.inbound, dlr.events) durably (acks=all): a deliver_sm
	// from the SMSC is acknowledged only once its MO/DLR is on Kafka (step-043).
	s.producer, err = kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	// Redis backs the cancel token, consulted on two paths: CLAIMED before each submit_sm so a cancel_sm
	// on smpp-server-svc cannot lose a race it should win, and merely READ in the max-age expiry branch,
	// which has already decided not to send and only wants to record a cancellation as one rather than as
	// an expiry (step-245). NewClient pings eagerly, so Redis must be reachable at boot (a startup outage
	// crash-loops the pod, as everywhere else). At RUNTIME it is deliberately NOT a readiness dependency
	// and BOTH checks fail OPEN: a Redis outage lets delivery continue rather than halting all outbound
	// traffic — cancellation is best-effort.
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
	if s.producer != nil {
		s.producer.Close()
	}
	if s.consumer != nil {
		s.consumer.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
}

// throttleMetrics are the send-path counters. None carries a label that could grow unbounded: a pod
// binds one connector, so these are per-pod, never per message id or MSISDN.
type throttleMetrics struct {
	// sendRate is the current AIMD send rate and throttledTotal counts ESME_RTHROTTLED events
	// (step-086).
	sendRate       prometheus.Gauge
	throttledTotal prometheus.Counter

	// deadLetterTotal counts messages parked on mt.dead-letter by gateway reason (fallback_exhausted,
	// retries_exhausted, delivery_expired), so a dead-lettered message is always counted, never
	// silently lost (step-129). The reason label is a bounded gateway code.
	deadLetterTotal *prometheus.CounterVec
}

func newThrottleMetrics(bindEnv connectorEnv) *throttleMetrics {
	m := &throttleMetrics{
		sendRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "connector_send_rate",
			Help: "Current adaptive (AIMD) send rate for this connector, in submits per second.",
		}),
		throttledTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "connector_throttled_total",
			Help: "ESME_RTHROTTLED responses received from the SMSC for this connector.",
		}),
		deadLetterTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connector_dead_letter_total",
			Help: "Messages parked on mt.dead-letter for this connector, labelled by reason.",
		}, []string{"reason"}),
	}
	if bindEnv.MaxSendRate > 0 {
		m.sendRate.Set(bindEnv.MaxSendRate) // start at the ceiling; the AIMD lowers it on throttle
	} else {
		m.sendRate.Set(math.NaN()) // AIMD disabled: report no value rather than a misleading 0
	}
	return m
}

// settler captures the reserved credit on a sent message and releases it on a terminal failure, via
// billing-svc (step-146). billing-svc is reached lazily (grpc.NewClient opens no connection until the
// first RPC) and the settle FAILS OPEN — a billing fault never redelivers a sent message — so a down
// billing-svc neither blocks startup nor duplicates SMS; a message with no reservation (billing
// disabled) makes zero calls.
type settler struct {
	settler *settle.Settler

	// captureFailed / releaseFailed count the fail-open events so an alert can fire (no labels — one
	// connector per pod, never a message id or MSISDN).
	captureFailed prometheus.Counter
	releaseFailed prometheus.Counter

	conn *grpc.ClientConn
}

func (s *settler) close() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func newSettler(cfg config.Config, logger *slog.Logger) (_ *settler, err error) {
	s := &settler{
		captureFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "billing_capture_failed_total",
			Help: "MT credit captures that failed and were left for reconciliation (fail-open).",
		}),
		releaseFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "billing_release_failed_total",
			Help: "MT credit releases that failed and were left for reconciliation (fail-open).",
		}),
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.conn, err = grpc.NewClient(cfg.Billing.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial billing at %q: %w", cfg.Billing.Addr, err)
	}
	s.settler = settle.NewSettler(pb.NewBillingClient(s.conn),
		settle.WithTimeout(cfg.Billing.SettleTimeout),
		settle.WithMetric(settleMetric{captureFailed: s.captureFailed, releaseFailed: s.releaseFailed}),
		settle.WithLogger(logger))
	return s, nil
}

// metricStream is the realtime dashboard feed (§1.6, step-182): a separate best-effort Kafka client,
// so a burst of snapshots can never fill the durable producer's buffer and stall a send.
type metricStream struct {
	producer *kafka.StreamProducer
	emitter  *metricstream.Emitter

	// dropped is the ONLY signal that the feed is degraded: nothing else fails when Kafka refuses a
	// snapshot.
	dropped []prometheus.Collector
}

func (m *metricStream) close() {
	if m.producer != nil {
		m.producer.Close()
	}
}

func newMetricStream(cfg config.Config) (_ *metricStream, err error) {
	m := &metricStream{}
	defer func() {
		if err != nil {
			m.close()
		}
	}()

	m.producer, err = kafka.NewStreamProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka stream producer: %w", err)
	}
	m.emitter, err = metricstream.New(serviceName, m.producer)
	if err != nil {
		return nil, fmt.Errorf("metric stream emitter: %w", err)
	}
	// The emitter also reports its refusals in-band (dropped_since_start), but that path is self-concealing:
	// a snapshot that fails to serialise can never carry the count saying so, and a stream nobody consumes
	// reports nothing at all. Both ends on /metrics (step-210).
	m.dropped = []prometheus.Collector{
		metrics.StreamDropCollector("buffer", m.producer),
		metrics.StreamDropCollector("refused", m.emitter),
	}
	return m, nil
}

// newOpsServer builds the ops listener (not yet bound) and registers exactly the metrics this service
// feeds.
//
// Vital dependencies (plan §1.5): Kafka (no work without it), ClickHouse (the outcome is recorded
// there) and the SMSC bind itself — the pool cannot deliver a single message without a live bind, and
// an idle-time bind drop would otherwise leave the pod Ready with nothing behind it.
func newOpsServer(
	cfg config.Config,
	logger *slog.Logger,
	st *stores,
	svc *connectorpool.Service,
	catalog *metrics.Catalog,
	throttle *throttleMetrics,
	billing *settler,
	stream *metricStream,
) (*observability.OpsServer, error) {
	ops, err := observability.NewOpsServer(cfg, logger,
		st.consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		st.producer.ReadyCheck("kafka-producer", cfg.Kafka.Timeout),
		st.ch.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		observability.ReadinessCheck{Name: "smsc-bind", Probe: svc.BindReady},
	)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
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
	ops.Registry().MustRegister(throttle.sendRate, throttle.throttledTotal, throttle.deadLetterTotal, linkUp,
		billing.captureFailed, billing.releaseFailed)
	ops.Registry().MustRegister(poolCatalogueCollectors(catalog)...)
	ops.Registry().MustRegister(stream.dropped...)
	return ops, nil
}

// poolCatalogueCollectors is the subset of the catalogue this service actually feeds (step-180).
// Registering Collectors() wholesale would expose always-zero series, which read as "measured, and
// nothing happened".
//
// The converse mistake is worse and is what this list is a named function for: a metric OBSERVED by
// connector-pool and missing here is scraped by nobody, so a dashboard shows a healthy silence while
// the code diligently records into a collector no registry owns. Feeding a catalogue metric from this
// service means adding it here, and TestOpsExposesTheMetricsThisServiceFeeds is what enforces it.
func poolCatalogueCollectors(catalog *metrics.Catalog) []prometheus.Collector {
	return []prometheus.Collector{
		catalog.ConnectorBreakerState,
		catalog.QueueDepth,
		catalog.SubmitsTotal,
		catalog.SubmitRejectedTotal,
		catalog.MessageE2EDuration,
	}
}

// reroutePark is the bounded reroute drainer (step-126): it consumes mt.reroute-park (AtStart —
// parked messages are durable and must all be drained) and replays each to mt.routed at the target
// connector's ceiling. Its own per-connector group; it skips-and-commits records for other
// connectors.
type reroutePark struct {
	drainer  *connectorpool.Drainer
	consumer *kafka.Consumer
}

func (d *reroutePark) close() {
	if d.consumer != nil {
		d.consumer.Close()
	}
}

func newDrainer(cfg config.Config, st *stores, limiter *ratelimit.Enforcer, connectorID uuid.UUID, logger *slog.Logger) (_ *reroutePark, err error) {
	d := &reroutePark{}
	defer func() {
		if err != nil {
			d.close()
		}
	}()

	d.consumer, err = kafka.NewConsumer(cfg.Kafka, serviceName+"-drain-"+connectorID.String(), kafka.TopicMTReroutePark)
	if err != nil {
		return nil, fmt.Errorf("kafka drain consumer: %w", err)
	}
	d.drainer = connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    d.consumer,
		Producer:    st.producer,
		Limiter:     limiter,
		ConnectorID: connectorID,
		Logger:      logger,
	})
	return d, nil
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

// settleMetric adapts the fail-open capture/release counters to settle.Metric (bounded, no labels).
type settleMetric struct{ captureFailed, releaseFailed prometheus.Counter }

func (m settleMetric) CaptureFailed() { m.captureFailed.Inc() }
func (m settleMetric) ReleaseFailed() { m.releaseFailed.Inc() }

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
