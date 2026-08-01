// Command mo-dlr-router-svc is the return-path router (M4). This step runs the DLR leg: it consumes
// dlr.events, correlates each delivery receipt back to its message through the dlrmap (§1.11), and
// records the final delivery outcome as a versioned CDR row. It follows the canonical service
// lifecycle with a Kafka consumer, a ClickHouse connection and a Redis client. The MO leg (mo.inbound)
// lands in step-045.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

const serviceName = "mo-dlr-router-svc"

// Bounds on webhook delivery. The timeout keeps one failing endpoint from stalling the delivery
// consumer's serial loop (head-of-line blocking): a single attempt runs there, and a transient failure is
// deferred to webhook.retry rather than retried in band. See the sender wiring in run().
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

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionKafka, config.SectionClickHouse, config.SectionRedis,
		config.SectionPostgres, config.SectionSMPP)
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

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicDLREvents)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// Redis backs the DLR correlation lookup. NewClient pings eagerly, so Redis must be reachable at
	// boot. At RUNTIME it is deliberately NOT a readiness dependency: a receipt whose mapping cannot be
	// read is retried (an infra error) or counted (a genuine miss), so a Redis blip self-heals rather
	// than taking the pod out of rotation.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	// dlr_unmapped_total counts receipts with no mapping (expired or unknown smsc_msg_id) — the receipt
	// is logged and counted, never dropped silently. No labels: the cardinality-bounded rule forbids a
	// message id / MSISDN / connector id here.
	unmapped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlr_unmapped_total",
		Help: "Delivery receipts received with no dlrmap entry (expired or unknown smsc_msg_id).",
	})

	svc := modlrrouter.New(modlrrouter.Deps{
		Consumer: consumer,
		Resolver: dlrmap.NewRedisMap(rdb),
		CDR:      clickhouse.NewCDRWriter(chConn),
		Unmapped: unmapped,
		Tracer:   observability.Tracer(nil, serviceName),
		Logger:   logger,
	})

	// --- MO leg (step-045): resolve mo.inbound to an account and publish mo.routed ---
	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	// The routing table is cold-loaded once at boot; a hot reload is a later milestone.
	snapshot, err := modlrrouter.LoadSnapshot(ctx, logger,
		postgres.NewInboundNumberRepo(pool), postgres.NewInboundKeywordRepo(pool), postgres.NewAccountRepo(pool))
	if err != nil {
		return fmt.Errorf("load mo routing snapshot: %w", err)
	}

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	moConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-mo", kafka.TopicMOInbound)
	if err != nil {
		return fmt.Errorf("kafka mo consumer: %w", err)
	}
	defer moConsumer.Close()

	// mo_unrouted_total{connector_id, reason}: bounded labels (few connectors, three reasons) — never a
	// MSISDN or body, per the cardinality rule.
	unroutedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mo_unrouted_total",
		Help: "Mobile-originated messages that resolved to no account, by connector and reason.",
	}, []string{"connector_id", "reason"})

	// STOP detection (step-063): opt-out keywords are cold-loaded once at boot (hot reload is a later
	// milestone). A matched STOP writes a suppression scoped to the inbound number and emits a
	// never-billed auto-reply straight to mt.routed (the same producer — the record names the topic).
	optOutKeywords, err := modlrrouter.LoadOptOutKeywords(ctx, postgres.NewOptOutKeywordRepo(pool))
	if err != nil {
		return fmt.Errorf("load opt-out keywords: %w", err)
	}
	stopDetector := modlrrouter.NewStopDetector(modlrrouter.StopDeps{
		Keywords: optOutKeywords,
		Suppress: postgres.NewSuppressionRepo(pool),
		Producer: producer,
		Tracer:   observability.Tracer(nil, serviceName),
		Logger:   logger,
	})

	moRouter := modlrrouter.NewMORouter(modlrrouter.MODeps{
		Consumer:    moConsumer,
		Snapshot:    snapshot,
		Producer:    producer,
		Unrouted:    postgres.NewUnroutedMORepo(pool),
		Metric:      unroutedMetric{vec: unroutedTotal},
		Stop:        stopDetector,
		Velocity:    moVelocity{state: antispam.NewRedisState(rdb)},
		Reassembler: encoding.NewReassembler(rdb, moReassemblyTTL),
		Tracer:      observability.Tracer(nil, serviceName),
		Logger:      logger,
	})

	// --- Delivery leg (step-048): hand each resolved MO/DLR to a live bind, else the webhook, else a
	// durable dead-letter. The SessionRegistry client resolves an account's binds; PodClients dials the
	// owning pod's SessionRegistry.Deliver (address templated from pod_id). ---
	registryConn, err := grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial session registry: %w", err)
	}
	defer func() { _ = registryConn.Close() }()

	pods := modlrrouter.NewPodClients(modlrrouter.NewTemplateResolver(cfg.SMPP.PodAddrTemplate))
	defer pods.Close()

	// The webhook sender owns its retries and parks an exhausted event on webhook.dead-letter (step-047
	// interface, wired here). The deliverer never parks a webhook event itself.
	//
	// Send runs on the delivery consumer goroutine, which processes records serially, so it must never
	// wait: an unresponsive endpoint would block the whole partition's return traffic (head-of-line).
	// It therefore spends exactly ONE attempt — bounded by a short per-request timeout — and defers a
	// transient failure to webhook.retry, drained below by its own paced consumer (step-192). Only a
	// permanent rejection, an exhausted attempt budget or an over-age event reaches the dead-letter.
	webhookClient := &http.Client{
		Timeout:       webhookHotPathTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	sender := webhook.NewSender(webhookClient, modlrrouter.NewWebhookDeadLetterSink(producer), logger,
		webhook.WithRetrySink(modlrrouter.NewWebhookRetrySink(producer)),
		webhook.WithMaxAttempts(webhookMaxAttempts))

	// mo_dlr_undelivered_total{event_type, reason}: bounded labels (two event types, two reasons) — the
	// count of events that reached neither a bind nor a webhook and were dead-lettered.
	undeliveredTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mo_dlr_undelivered_total",
		Help: "Return-path events dead-lettered because no bind and no webhook could take them.",
	}, []string{"event_type", "reason"})
	// dlr_delivery_mapping_miss_total: a DLR whose dlrmap TTL elapsed before delivery — distinct from
	// step-044's CDR-side dlr_unmapped_total (the two dlr.events consumers miss independently).
	dlrMappingMiss := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlr_delivery_mapping_miss_total",
		Help: "Delivery receipts dropped at the delivery stage because their dlrmap entry had expired.",
	})

	deliverer := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   modlrrouter.NewRegistryLookup(registrypb.NewSessionRegistryClient(registryConn)),
		Pods:     pods,
		Webhooks: postgres.NewWebhookRepo(pool),
		Sender:   sender,
		Producer: producer,
		Metric:   deliveryMetric{vec: undeliveredTotal},
		Logger:   logger,
	})

	moDeliveryConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-mo-delivery", kafka.TopicMORouted)
	if err != nil {
		return fmt.Errorf("kafka mo delivery consumer: %w", err)
	}
	defer moDeliveryConsumer.Close()
	moDelivery := modlrrouter.NewMODeliveryRouter(modlrrouter.MODeliveryDeps{
		Consumer:  moDeliveryConsumer,
		Deliverer: deliverer,
		Tracer:    observability.Tracer(nil, serviceName),
		Logger:    logger,
	})

	dlrDeliveryConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-dlr-delivery", kafka.TopicDLREvents)
	if err != nil {
		return fmt.Errorf("kafka dlr delivery consumer: %w", err)
	}
	defer dlrDeliveryConsumer.Close()
	dlrDelivery := modlrrouter.NewDLRDeliveryRouter(modlrrouter.DLRDeliveryDeps{
		Consumer:    dlrDeliveryConsumer,
		Resolver:    dlrmap.NewRedisMap(rdb),
		Deliverer:   deliverer,
		MappingMiss: dlrMappingMiss,
		Tracer:      observability.Tracer(nil, serviceName),
		Logger:      logger,
	})

	// The retry drain (step-192): its OWN consumer group and goroutine, so a slow endpoint's backoff is
	// served here and never on the delivery consumers above. It re-resolves each event's webhook from the
	// control plane — the queued record deliberately carries no signing secret, and a rotated secret or an
	// edited URL therefore takes effect on the next pass.
	retryConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-webhook-retry", kafka.TopicWebhookRetry)
	if err != nil {
		return fmt.Errorf("kafka webhook-retry consumer: %w", err)
	}
	defer retryConsumer.Close()
	retryRunner := modlrrouter.NewWebhookRetryRunner(postgres.NewWebhookRepo(pool), sender,
		modlrrouter.WithRetryRunnerLogger(logger))

	// Vital dependencies (plan §1.5): Kafka (no work without it) and ClickHouse (the delivery outcome is
	// recorded there). Redis is intentionally absent — see its comment above.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		producer.ReadyCheck("kafka-producer", cfg.Kafka.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(unmapped, unroutedTotal, undeliveredTotal, dlrMappingMiss)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("dlr router", svc.Run)
	g.Add("mo router", moRouter.Run)
	g.Add("mo delivery", moDelivery.Run)
	g.Add("dlr delivery", dlrDelivery.Run)
	g.Add("webhook retry", func(c context.Context) error { return retryConsumer.Run(c, retryRunner.Handle) })
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

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
