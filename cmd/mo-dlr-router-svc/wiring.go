package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// Bounds on webhook delivery. The timeout keeps one failing endpoint from stalling the delivery
// consumer's serial loop (head-of-line blocking): a single attempt runs there, and a transient failure is
// deferred to webhook.retry rather than retried in band. See the sender wiring in newDeliveryLeg.
const (
	webhookHotPathTimeout = 5 * time.Second
	// webhookMaxAttempts is the TOTAL attempt budget across the first try and every deferred retry — not
	// a per-pass cap. It is larger than the 3 the old inline loop allowed precisely because attempts no
	// longer block anything: spread over a growing backoff they span roughly an hour, so an endpoint down
	// for a short outage is recovered instead of dead-lettered within seconds. The sender's maximum retry
	// age is the other bound, and stops a slow-failing endpoint sooner.
	webhookMaxAttempts = 8
	// moReassemblyTTL bounds how long the segments of a concatenated MO are buffered while awaiting the
	// rest (step-083). A group that does not complete within it evicts, so an orphaned half-message
	// cannot accumulate. Handsets and SMSCs reassemble on a similar order of minutes.
	moReassemblyTTL = 10 * time.Minute
)

// returnPathApp is mo-dlr-router-svc fully wired and not yet running: every connection is open,
// every component built, but no consumer is started and no port bound. Separating "assemble the
// graph" from "run it" is what makes the wiring testable — a test can build the whole service
// against test dependencies and assert it holds together, without a single receipt flowing.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a
// value the caller (or a test) can inspect.
type returnPathApp struct {
	ops         *observability.OpsServer
	dlr         *modlrrouter.Service
	mo          *modlrrouter.MORouter
	moDelivery  *modlrrouter.MODeliveryRouter
	dlrDelivery *modlrrouter.DLRDeliveryRouter

	// retry is the webhook retry drain: its own consumer group and goroutine (step-192).
	retryConsumer *kafka.Consumer
	retryRunner   *modlrrouter.WebhookRetryRunner

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred
	// Closes in run() used to provide. They are named because that order is the property worth
	// guarding, and an anonymous stack cannot be asserted against.
	closers []closer
}

// closer is a release step and the name it answers to. The name carries no behaviour: it exists so
// that the release ORDER — a property of newReturnPathApp, and one a wrong edit breaks silently — can be
// asserted on the graph the service actually builds.
type closer struct {
	name string
	fn   func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *returnPathApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name: name, fn: f})
}

// close releases every connection the app holds. It is safe to call on a partially built app: only
// what was actually opened is registered.
func (a *returnPathApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}

// newReturnPathApp builds the whole graph: stores, the DLR leg, the MO leg, the delivery leg, the
// webhook retry drain and the ops server — in that order, which is the order in which a degraded
// dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds
// nothing.
func newReturnPathApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *returnPathApp, err error) {
	a := &returnPathApp{}
	defer func() {
		if err != nil {
			a.close()
		}
	}()

	st, err := openStores(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.onClose("stores", st.close)

	dlr := newDLRLeg(st, logger)
	a.dlr = dlr.router

	mo, err := newMOLeg(ctx, cfg, st, logger)
	if err != nil {
		return nil, err
	}
	a.onClose("mo", mo.close)
	a.mo = mo.router

	delivery, err := newDeliveryLeg(cfg, st, mo, logger)
	if err != nil {
		return nil, err
	}
	a.onClose("delivery", delivery.close)
	a.moDelivery = delivery.mo
	a.dlrDelivery = delivery.dlr

	retry, err := newWebhookRetry(cfg, mo.pg, delivery.sender, logger)
	if err != nil {
		return nil, err
	}
	a.onClose("retry", retry.close)
	a.retryConsumer = retry.consumer
	a.retryRunner = retry.runner

	a.ops, err = newOpsServer(cfg, logger, st, mo, dlr, delivery, retry)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores are the connections the DLR leg needs; the MO and delivery legs open their own further
// down, in the order run() used to.
type stores struct {
	ch       *clickhouse.Conn
	consumer *kafka.Consumer
	rdb      *goredis.Client
}

func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
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

	s.consumer, err = kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicDLREvents)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	// Redis backs the DLR correlation lookup. NewClient pings eagerly, so Redis must be reachable at
	// boot. At RUNTIME it is deliberately NOT a readiness dependency: a receipt whose mapping cannot be
	// read is retried (an infra error) or counted (a genuine miss), so a Redis blip self-heals rather
	// than taking the pod out of rotation.
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
	if s.consumer != nil {
		s.consumer.Close()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
}

// dlrLeg correlates each delivery receipt back to its message through the dlrmap (§1.11) and records
// the final delivery outcome as a versioned CDR row.
type dlrLeg struct {
	router *modlrrouter.Service

	// unmapped counts receipts with no mapping (expired or unknown smsc_msg_id) — the receipt is
	// logged and counted, never dropped silently. No labels: the cardinality-bounded rule forbids a
	// message id / MSISDN / connector id here.
	unmapped prometheus.Counter
}

func newDLRLeg(st *stores, logger *slog.Logger) *dlrLeg {
	unmapped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlr_unmapped_total",
		Help: "Delivery receipts received with no dlrmap entry (expired or unknown smsc_msg_id).",
	})
	return &dlrLeg{
		unmapped: unmapped,
		router: modlrrouter.New(modlrrouter.Deps{
			Consumer: st.consumer,
			Resolver: dlrmap.NewRedisMap(st.rdb),
			CDR:      clickhouse.NewCDRWriter(st.ch),
			Unmapped: unmapped,
			Tracer:   observability.Tracer(nil, serviceName),
			Logger:   logger,
		}),
	}
}

// moLeg resolves mo.inbound to an account and publishes mo.routed (step-045). It owns the Postgres
// pool and the producer the delivery leg and the retry drain also use.
type moLeg struct {
	router   *modlrrouter.MORouter
	pg       *pgxpool.Pool
	producer *kafka.Producer
	consumer *kafka.Consumer

	// unrouted carries bounded labels (few connectors, three reasons) — never a MSISDN or body.
	unrouted *prometheus.CounterVec
}

func (m *moLeg) close() {
	if m.consumer != nil {
		m.consumer.Close()
	}
	if m.producer != nil {
		m.producer.Close()
	}
	if m.pg != nil {
		m.pg.Close()
	}
}

func newMOLeg(ctx context.Context, cfg config.Config, st *stores, logger *slog.Logger) (_ *moLeg, err error) {
	m := &moLeg{}
	defer func() {
		if err != nil {
			m.close()
		}
	}()

	m.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	// The routing table is cold-loaded once at boot; a hot reload is a later milestone.
	snapshot, err := modlrrouter.LoadSnapshot(ctx, logger,
		postgres.NewInboundNumberRepo(m.pg), postgres.NewInboundKeywordRepo(m.pg), postgres.NewAccountRepo(m.pg))
	if err != nil {
		return nil, fmt.Errorf("load mo routing snapshot: %w", err)
	}

	m.producer, err = kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	m.consumer, err = kafka.NewConsumer(cfg.Kafka, serviceName+"-mo", kafka.TopicMOInbound)
	if err != nil {
		return nil, fmt.Errorf("kafka mo consumer: %w", err)
	}

	m.unrouted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mo_unrouted_total",
		Help: "Mobile-originated messages that resolved to no account, by connector and reason.",
	}, []string{"connector_id", "reason"})

	// STOP detection (step-063): opt-out keywords are cold-loaded once at boot (hot reload is a later
	// milestone). A matched STOP writes a suppression scoped to the inbound number and emits a
	// never-billed auto-reply straight to mt.routed (the same producer — the record names the topic).
	optOutKeywords, err := modlrrouter.LoadOptOutKeywords(ctx, postgres.NewOptOutKeywordRepo(m.pg))
	if err != nil {
		return nil, fmt.Errorf("load opt-out keywords: %w", err)
	}
	stopDetector := modlrrouter.NewStopDetector(modlrrouter.StopDeps{
		Keywords: optOutKeywords,
		Suppress: postgres.NewSuppressionRepo(m.pg),
		Producer: m.producer,
		Tracer:   observability.Tracer(nil, serviceName),
		Logger:   logger,
	})

	m.router = modlrrouter.NewMORouter(modlrrouter.MODeps{
		Consumer:    m.consumer,
		Snapshot:    snapshot,
		Producer:    m.producer,
		Unrouted:    postgres.NewUnroutedMORepo(m.pg),
		Metric:      unroutedMetric{vec: m.unrouted},
		Stop:        stopDetector,
		Velocity:    moVelocity{state: antispam.NewRedisState(st.rdb)},
		Reassembler: encoding.NewReassembler(st.rdb, moReassemblyTTL),
		Tracer:      observability.Tracer(nil, serviceName),
		Logger:      logger,
	})
	return m, nil
}

// deliveryLeg hands each resolved MO/DLR to a live bind, else the webhook, else a durable
// dead-letter (step-048). The SessionRegistry client resolves an account's binds; PodClients dials
// the owning pod's SessionRegistry.Deliver (address templated from pod_id).
type deliveryLeg struct {
	mo  *modlrrouter.MODeliveryRouter
	dlr *modlrrouter.DLRDeliveryRouter

	// sender is shared with the retry drain: both re-resolve a webhook from the control plane and send
	// through the same attempt budget.
	sender *webhook.Sender

	// undelivered and mappingMiss carry bounded labels only.
	undelivered *prometheus.CounterVec
	mappingMiss prometheus.Counter

	registry    *grpc.ClientConn
	pods        *modlrrouter.PodClients
	moConsumer  *kafka.Consumer
	dlrConsumer *kafka.Consumer
}

func (d *deliveryLeg) close() {
	if d.dlrConsumer != nil {
		d.dlrConsumer.Close()
	}
	if d.moConsumer != nil {
		d.moConsumer.Close()
	}
	if d.pods != nil {
		d.pods.Close()
	}
	if d.registry != nil {
		_ = d.registry.Close()
	}
}

func newDeliveryLeg(cfg config.Config, st *stores, mo *moLeg, logger *slog.Logger) (_ *deliveryLeg, err error) {
	d := &deliveryLeg{}
	defer func() {
		if err != nil {
			d.close()
		}
	}()

	d.registry, err = grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial session registry: %w", err)
	}
	d.pods = modlrrouter.NewPodClients(modlrrouter.NewTemplateResolver(cfg.SMPP.PodAddrTemplate))

	// The webhook sender owns its retries and parks an exhausted event on webhook.dead-letter (step-047
	// interface, wired here). The deliverer never parks a webhook event itself.
	//
	// Send runs on the delivery consumer goroutine, which processes records serially, so it must never
	// wait: an unresponsive endpoint would block the whole partition's return traffic (head-of-line).
	// It therefore spends exactly ONE attempt — bounded by a short per-request timeout — and defers a
	// transient failure to webhook.retry, drained by its own paced consumer (step-192). Only a
	// permanent rejection, an exhausted attempt budget or an over-age event reaches the dead-letter.
	webhookClient := &http.Client{
		Timeout:       webhookHotPathTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	d.sender = webhook.NewSender(webhookClient, modlrrouter.NewWebhookDeadLetterSink(mo.producer), logger,
		webhook.WithRetrySink(modlrrouter.NewWebhookRetrySink(mo.producer)),
		webhook.WithMaxAttempts(webhookMaxAttempts))

	// mo_dlr_undelivered_total{event_type, reason}: bounded labels (two event types, two reasons) — the
	// count of events that reached neither a bind nor a webhook and were dead-lettered.
	d.undelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mo_dlr_undelivered_total",
		Help: "Return-path events dead-lettered because no bind and no webhook could take them.",
	}, []string{"event_type", "reason"})
	// dlr_delivery_mapping_miss_total: a DLR whose dlrmap TTL elapsed before delivery — distinct from
	// step-044's CDR-side dlr_unmapped_total (the two dlr.events consumers miss independently).
	d.mappingMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlr_delivery_mapping_miss_total",
		Help: "Delivery receipts dropped at the delivery stage because their dlrmap entry had expired.",
	})

	deliverer := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   modlrrouter.NewRegistryLookup(registrypb.NewSessionRegistryClient(d.registry)),
		Pods:     d.pods,
		Webhooks: postgres.NewWebhookRepo(mo.pg),
		Sender:   d.sender,
		Producer: mo.producer,
		Metric:   deliveryMetric{vec: d.undelivered},
		Logger:   logger,
	})

	d.moConsumer, err = kafka.NewConsumer(cfg.Kafka, serviceName+"-mo-delivery", kafka.TopicMORouted)
	if err != nil {
		return nil, fmt.Errorf("kafka mo delivery consumer: %w", err)
	}
	d.mo = modlrrouter.NewMODeliveryRouter(modlrrouter.MODeliveryDeps{
		Consumer:  d.moConsumer,
		Deliverer: deliverer,
		Tracer:    observability.Tracer(nil, serviceName),
		Logger:    logger,
	})

	d.dlrConsumer, err = kafka.NewConsumer(cfg.Kafka, serviceName+"-dlr-delivery", kafka.TopicDLREvents)
	if err != nil {
		return nil, fmt.Errorf("kafka dlr delivery consumer: %w", err)
	}
	d.dlr = modlrrouter.NewDLRDeliveryRouter(modlrrouter.DLRDeliveryDeps{
		Consumer:    d.dlrConsumer,
		Resolver:    dlrmap.NewRedisMap(st.rdb),
		Deliverer:   deliverer,
		MappingMiss: d.mappingMiss,
		Tracer:      observability.Tracer(nil, serviceName),
		Logger:      logger,
	})
	return d, nil
}

// webhookRetry is the retry drain (step-192): its OWN consumer group and goroutine, so a slow
// endpoint's backoff is served there and never on the delivery consumers. It re-resolves each
// event's webhook from the control plane — the queued record deliberately carries no signing secret,
// and a rotated secret or an edited URL therefore takes effect on the next pass.
type webhookRetry struct {
	consumer *kafka.Consumer
	runner   *modlrrouter.WebhookRetryRunner

	// handled carries a bounded label (retried|dropped|skipped). The age histogram is the operational
	// signal that matters: an account whose endpoint is durably unreachable shows up as a rising retry
	// age WELL BEFORE its events start landing in the dead-letter, which is the only other symptom and
	// arrives hours later.
	handled *prometheus.CounterVec
	age     prometheus.Histogram
}

func (w *webhookRetry) close() {
	if w.consumer != nil {
		w.consumer.Close()
	}
}

func newWebhookRetry(cfg config.Config, pool *pgxpool.Pool, sender *webhook.Sender, logger *slog.Logger) (_ *webhookRetry, err error) {
	w := &webhookRetry{}
	defer func() {
		if err != nil {
			w.close()
		}
	}()

	w.consumer, err = kafka.NewConsumer(cfg.Kafka, serviceName+"-webhook-retry", kafka.TopicWebhookRetry)
	if err != nil {
		return nil, fmt.Errorf("kafka webhook-retry consumer: %w", err)
	}
	w.handled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_retry_handled_total",
		Help: "Webhook events processed off the retry topic, by outcome.",
	}, []string{"outcome"})
	w.age = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "webhook_retry_age_seconds",
		Help: "Time an event has been in retry when re-attempted, counted from its first attempt.",
		// Buckets spanning the retry lifetime: the first backoff through the 6h maximum age.
		Buckets: []float64{30, 60, 300, 900, 1800, 3600, 10800, 21600},
	})
	w.runner = modlrrouter.NewWebhookRetryRunner(postgres.NewWebhookRepo(pool), sender,
		modlrrouter.WithRetryMetric(webhookRetryMetric{handled: w.handled, age: w.age}),
		modlrrouter.WithRetryRunnerLogger(logger))
	return w, nil
}

// newOpsServer builds the ops listener (not yet bound) and registers the metrics this service feeds.
//
// Vital dependencies (plan §1.5): Kafka (no work without it) and ClickHouse (the delivery outcome is
// recorded there). Redis is intentionally absent — see openStores.
func newOpsServer(
	cfg config.Config,
	logger *slog.Logger,
	st *stores,
	mo *moLeg,
	dlr *dlrLeg,
	delivery *deliveryLeg,
	retry *webhookRetry,
) (*observability.OpsServer, error) {
	ops, err := observability.NewOpsServer(cfg, logger,
		st.consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		mo.producer.ReadyCheck("kafka-producer", cfg.Kafka.Timeout),
		st.ch.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		postgres.PingCheck("postgres", mo.pg, cfg.Postgres.Timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(dlr.unmapped, mo.unrouted, delivery.undelivered, delivery.mappingMiss,
		retry.handled, retry.age)
	return ops, nil
}

// webhookRetryMetric adapts the drain counters to modlrrouter.RetryMetric.
type webhookRetryMetric struct {
	handled *prometheus.CounterVec
	age     prometheus.Histogram
}

func (m webhookRetryMetric) Handled(outcome string) { m.handled.WithLabelValues(outcome).Inc() }
func (m webhookRetryMetric) Age(d time.Duration)    { m.age.Observe(d.Seconds()) }

// moVelocity adapts antispam.RedisState to modlrrouter.MOVelocityRecorder: it records an inbound MO
// into its source's global velocity counter (step-066), the same key a "by source" MT velocity rule
// reads, so a sender's MT and MO traffic count together.
type moVelocity struct{ state *antispam.RedisState }

func (m moVelocity) RecordSource(ctx context.Context, from string) error {
	return m.state.Record(ctx, antispam.MOSourceVelocityKey(from))
}

// unroutedMetric adapts a Prometheus CounterVec to modlrrouter.UnroutedMetric.
type unroutedMetric struct{ vec *prometheus.CounterVec }

func (m unroutedMetric) Inc(connectorID, reason string) {
	m.vec.WithLabelValues(connectorID, reason).Inc()
}

// deliveryMetric adapts a Prometheus CounterVec to modlrrouter.DeliveryMetric.
type deliveryMetric struct{ vec *prometheus.CounterVec }

func (m deliveryMetric) UndeliveredInc(eventType, reason string) {
	m.vec.WithLabelValues(eventType, reason).Inc()
}
