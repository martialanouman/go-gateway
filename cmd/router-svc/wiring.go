package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/content"
	contentkeypb "github.com/martialanouman/go-gateway/internal/contentkeys/pb"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/outcome"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/credit"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// routerApp is router-svc fully wired and not yet running: every connection is open, every component
// built, but no goroutine is started and no port is bound. Separating "assemble the graph" from "run
// it" is what makes the wiring testable — a test can build the whole service against test
// dependencies and assert it holds together, without a single message flowing.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a
// value the caller (or a test) can inspect.
type routerApp struct {
	ops      *observability.OpsServer
	router   *router.Router
	accepted *ingest.AcceptedConsumer
	// Kept as the wrapper, not just the projector: it owns the consumer whose start offset is a
	// durability property a test must be able to assert (step-201c D9).
	outcome  *outcomeProjector
	watcher  *config.Watcher
	emitter  *metricstream.Emitter
	consumer *kafka.Consumer
	catalog  *metrics.Catalog

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred
	// Closes in run() used to provide.
	closers []func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *routerApp) onClose(f func()) { a.closers = append(a.closers, f) }

// close releases every connection the app holds. It is safe to call on a partially built app: only
// what was actually opened is registered.
func (a *routerApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// newRouterApp builds the whole router graph: stores, boot snapshots, MT pipeline, the two CDR
// projectors (accepted, outcome), realtime feed, ops server and hot-reload watcher — in that order,
// which is the order in which a degraded dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds
// nothing.
func newRouterApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *routerApp, err error) {
	a := &routerApp{}
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
	a.consumer = st.consumer

	boot, err := loadBootSnapshots(ctx, st.pg, logger)
	if err != nil {
		return nil, err
	}

	// The anti-spam engine, the token bucket and the exact-route short-cut all read Redis, which is a
	// boot dependency (NewClient pings eagerly). It retries with the same discipline as the snapshots —
	// a transient outage must not crashloop a (re)starting pod.
	rdb, err := loadWithRetry(ctx, logger, "redis", func(ctx context.Context) (*goredis.Client, error) {
		return redisstore.NewClient(ctx, cfg.Redis)
	})
	if err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	a.onClose(func() { _ = rdb.Close() })

	// The metric catalogue (step-180) declares the bounded label sets this service feeds.
	a.catalog = metrics.NewCatalog()
	tracer := observability.Tracer(nil, serviceName)

	stack, err := newPipelineStack(ctx, cfg, st.pg, rdb, boot, a.catalog, tracer, logger)
	if err != nil {
		return nil, err
	}
	a.onClose(stack.close)

	proj, err := newAcceptedProjector(ctx, cfg, st.pg, st.ch, logger)
	if err != nil {
		return nil, err
	}
	a.onClose(proj.close)
	a.accepted = proj.consumer

	outc, err := newOutcomeProjector(cfg, st.ch, logger)
	if err != nil {
		return nil, err
	}
	a.onClose(outc.close)
	a.outcome = outc

	stream, err := newMetricStream(cfg)
	if err != nil {
		return nil, err
	}
	a.onClose(stream.close)
	a.emitter = stream.emitter

	a.router = router.New(router.Deps{
		Consumer: st.consumer,
		Producer: st.producer,
		Pipeline: stack.pipeline,
		CDR:      clickhouse.NewCDRWriter(st.ch),
		Sealer:   proj.sealer,
		Tracer:   tracer,
		Logger:   logger,
		Stream:   stream.emitter,
		Metrics:  a.catalog,
	})

	ops, blooms, err := newOpsServer(cfg, logger, st.consumer, a.catalog, stack, proj, stream, boot)
	if err != nil {
		return nil, err
	}
	a.ops = ops

	a.watcher = newSnapshotWatcher(st.pg, rdb, boot, stack, proj, blooms, logger)
	return a, nil
}

// stores are the external connections router-svc opens at boot and holds for its whole lifetime:
// Postgres for the startup snapshots, Kafka for the data plane, ClickHouse for rejected CDR rows.
// No HTTP: the router has no client-facing listener.
type stores struct {
	pg       *pgxpool.Pool
	ch       *clickhouse.Conn
	producer *kafka.Producer
	consumer *kafka.Consumer
}

// openStores opens them in dependency-free order, releasing what it already holds if a later one
// fails.
func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	s.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	s.producer, err = kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	s.consumer, err = kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTInbound)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return s, nil
}

// close releases the connections in reverse order of opening; a nil field is one that was never
// opened.
func (s *stores) close() {
	if s.consumer != nil {
		s.consumer.Close()
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

// bootSnapshots are the immutable control-plane snapshots loaded once at boot from Postgres. Each is
// a hard boot dependency — the router cannot route without routes, nor stay compliant without the
// sender-ID and opt-out state — so each is loaded with the same retry discipline rather than exiting
// on the first failure.
type bootSnapshots struct {
	routes    *routing.SnapshotResolver
	senderIDs *senderid.Authorizer
	optOut    *optout.Enforcer

	// suppressions is kept because the opt-out enforcer is rebuilt from it on every invalidation.
	suppressions *postgres.SuppressionRepo
}

// loadBootSnapshots loads the three snapshots in order, retrying transient Postgres failures so a
// blip cannot brick a (re)starting pod.
func loadBootSnapshots(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) (*bootSnapshots, error) {
	// The route snapshot is loaded once at startup; config-sync's hot reload swaps it later.
	routes, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), logger)
	if err != nil {
		return nil, fmt.Errorf("load route snapshot: %w", err)
	}

	// The sender-ID authorization snapshot (§6.19): the account policies and the customers' active
	// sender IDs, indexed once for lock-free per-message checks.
	senderIDs, err := loadSenderIDSnapshotWithRetry(ctx, postgres.NewAccountRepo(pool), postgres.NewSenderIDRepo(pool), logger)
	if err != nil {
		return nil, fmt.Errorf("load sender-id snapshot: %w", err)
	}

	// The opt-out enforcer (§6.20): a per-scope Bloom over the suppressions (exact confirmation behind
	// it) and an inbound-number index to resolve the sending number's scope.
	suppressions := postgres.NewSuppressionRepo(pool)
	optOut, err := loadOptOutEnforcerWithRetry(ctx, suppressions, suppressions, postgres.NewInboundNumberRepo(pool), logger)
	if err != nil {
		return nil, fmt.Errorf("load opt-out enforcer: %w", err)
	}

	return &bootSnapshots{routes: routes, senderIDs: senderIDs, optOut: optOut, suppressions: suppressions}, nil
}

// pipelineStack is the MT pipeline plus the handles a config-sync invalidation rebuilds. It holds no
// listener: everything here is either an in-memory snapshot or a lazily-dialled client.
type pipelineStack struct {
	pipeline *pipeline.Pipeline

	// Reload handles: the state the snapshot watcher swaps atomically under the readers.
	exactBloom     *exact.Bloom
	exactRepo      *postgres.ExactRouteRepo
	scriptRepo     *postgres.RoutingScriptRepo
	scriptResolver *routing.ScriptResolver
	billingRepo    *postgres.BillingRepo
	creditHolder   *credit.Holder
	breakerAvail   breakerAvailability

	failOpenTotal prometheus.Counter
	billingConn   *grpc.ClientConn
}

// close releases the billing connection; every other field is in-memory state.
func (p *pipelineStack) close() {
	if p.billingConn != nil {
		_ = p.billingConn.Close()
	}
}

// newPipelineStack assembles the compliance and routing stages in pipeline order (§6.1) on top of
// the boot snapshots: the Redis overlays, anti-spam, rate limiting, the L0/L1/L2 resolver chain and
// the credit reserver.
func newPipelineStack(
	ctx context.Context,
	cfg config.Config,
	pool *pgxpool.Pool,
	rdb *goredis.Client,
	boot *bootSnapshots,
	catalog *metrics.Catalog,
	tracer trace.Tracer,
	logger *slog.Logger,
) (_ *pipelineStack, err error) {
	p := &pipelineStack{}
	defer func() {
		if err != nil {
			p.close()
		}
	}()

	// least_loaded reads each connector's published in-flight gauge (connectorload:{id}) from Redis —
	// the mutable overlay beside the immutable route snapshot (step-104/114). A missing gauge reads 0.
	boot.routes.UseLoadReader(connectorLoad{rdb: rdb})
	// Breaker awareness (step-123): read each connector's aggregate breaker:state once at boot to seed the
	// availability overlay, then refresh it on every snapshot rebuild. Reading here (not per message)
	// keeps the hot path off Redis. A transient read failure just leaves every connector available.
	p.breakerAvail = breakerAvailability{rdb: rdb}
	if err := boot.routes.RefreshAvailability(ctx, p.breakerAvail); err != nil {
		logger.WarnContext(ctx, "seed breaker availability failed; starting with all connectors available", "err", err)
	}

	// anti_spam_fail_open_total: bounded (no labels) — counts messages let through because a
	// Redis-backed anti-spam check could not run (§1.5 fail-open). Never a MSISDN or body.
	p.failOpenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "anti_spam_fail_open_total",
		Help: "Messages passed (flagged) because a Redis-backed anti-spam check failed open.",
	})
	// The anti-spam engine (§6.20): content rules compiled once at boot, duplicates checked in Redis.
	spam, err := loadAntispamWithRetry(ctx, postgres.NewAntispamRuleRepo(pool), antispam.NewRedisState(rdb), failOpenMetric{c: p.failOpenTotal}, logger)
	if err != nil {
		return nil, fmt.Errorf("load anti-spam engine: %w", err)
	}

	// The rate-limit snapshot (§6.4): the operational rate_limits plus each connector's
	// throughput_limit_per_sec hard ceiling, indexed once at boot. The token bucket that enforces it
	// lives in Redis (shared across pods) and fails closed on an outage (step-084).
	rateSnap, err := loadWithRetry(ctx, logger, "rate-limit snapshot", func(ctx context.Context) (*ratelimit.Snapshot, error) {
		return ratelimit.LoadSnapshot(ctx, postgres.NewRateLimitRepo(pool), postgres.NewConnectorRepo(pool))
	})
	if err != nil {
		return nil, fmt.Errorf("load rate-limit snapshot: %w", err)
	}
	rateLimiter := ratelimit.NewEnforcer(rateSnap, ratelimit.NewLimiter(rdb))

	// The L0 exact-number short-cut (§6.1): an in-memory Bloom over every exact_routes MSISDN, loaded
	// once at boot, in front of the shared Redis map. A ported number routes straight to its target
	// (skipping route resolution only, never compliance); every other number falls through to the
	// declarative resolver.
	p.exactRepo = postgres.NewExactRouteRepo(pool)
	p.exactBloom, err = loadWithRetry(ctx, logger, "exact-route bloom", func(ctx context.Context) (*exact.Bloom, error) {
		return exact.LoadBloom(ctx, p.exactRepo)
	})
	if err != nil {
		return nil, fmt.Errorf("load exact-route bloom: %w", err)
	}

	// The L1 routing-script stage (§6.1): active scripts compiled once into an immutable snapshot,
	// scope-resolved per message (account → customer → platform). A null result or ANY script fault
	// falls back to declarative resolution and is metered; the stage sits between exact (L0) and
	// declarative (L2), never before compliance.
	p.scriptRepo = postgres.NewRoutingScriptRepo(pool)
	scriptSnap, err := loadWithRetry(ctx, logger, "routing-script snapshot", func(ctx context.Context) (*routing.ScriptSnapshot, error) {
		return routing.BuildScriptSnapshot(ctx, p.scriptRepo, logger)
	})
	if err != nil {
		return nil, fmt.Errorf("load routing scripts: %w", err)
	}
	// routing_script_failures_total carries bounded labels only (runtime, reason) — never a body or
	// script text.
	p.scriptResolver = routing.NewScriptResolver(scriptSnap, logger, scriptMeter{c: catalog.RoutingScriptFailures})
	resolver := routing.NewL0Resolver(exact.NewResolver(p.exactBloom, rdb), p.scriptResolver, boot.routes)

	// The credit stage (§6.9, step-145): a per-customer billing-scope snapshot gates the reserve, so a
	// billing-disabled customer makes ZERO billing round-trip, and the billing gRPC client reserves credit
	// for the rest. billing-svc is reached lazily (grpc.NewClient opens no connection until the first RPC),
	// so a router that bills nobody never touches it and a down content-key-svc does not block startup. The
	// scope snapshot is rebuilt on every config invalidation, so enabling billing for a customer takes
	// effect without a restart.
	p.billingRepo = postgres.NewBillingRepo(pool)
	creditSnap, err := loadWithRetry(ctx, logger, "billing scope snapshot", func(ctx context.Context) (*credit.Snapshot, error) {
		return credit.LoadSnapshot(ctx, p.billingRepo)
	})
	if err != nil {
		return nil, fmt.Errorf("load billing scope snapshot: %w", err)
	}
	p.creditHolder = &credit.Holder{}
	p.creditHolder.Store(creditSnap)
	p.billingConn, err = grpc.NewClient(cfg.Billing.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial billing at %q: %w", cfg.Billing.Addr, err)
	}
	reserver := credit.NewReserver(p.creditHolder, pb.NewBillingClient(p.billingConn), credit.WithTimeout(cfg.Billing.ReserveTimeout))

	p.pipeline = pipeline.New(tracer, resolver, boot.senderIDs, boot.optOut, spam, rateLimiter, reserver)
	return p, nil
}

// acceptedProjector is the durable accepted-CDR projection (step-101): the accepted row is projected
// off mt.inbound by a DEDICATED consumer group, separate from routing, committing at-least-once after
// the ClickHouse write so no accepted row is ever lost (a ClickHouse fault reprocesses the batch; a
// slow ClickHouse only lengthens the get-message 404 window, never the routing path). It also seals
// the body into the CDR per content_storage (step-162): the DEK comes from content-key-svc via a TTL
// cache, so the body never reaches it; an unavailable DEK degrades to no-content (counted), never a
// stall.
type acceptedProjector struct {
	consumer *ingest.AcceptedConsumer
	sealer   *ingest.ContentSealer

	// policy is swapped on every config invalidation so a content_storage change — critically an
	// opt-out (stored_*→off) — takes effect without a restart (step-102).
	policy  *content.PolicyHolder
	dropped prometheus.Counter

	kafka *kafka.Consumer
	conn  *grpc.ClientConn
}

func (p *acceptedProjector) close() {
	if p.kafka != nil {
		p.kafka.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

// newAcceptedProjector wires the projection. A fresh group starts at the LATEST offset — replaying
// the whole retained topic would re-insert every historical accepted row and storm billing for DEKs —
// so the deploy pins the start offset (runbook).
func newAcceptedProjector(ctx context.Context, cfg config.Config, pool *pgxpool.Pool, ch *clickhouse.Conn, logger *slog.Logger) (_ *acceptedProjector, err error) {
	p := &acceptedProjector{}
	defer func() {
		if err != nil {
			p.close()
		}
	}()

	policy, err := content.LoadPolicySnapshot(ctx, postgres.NewCustomerRepo(pool))
	if err != nil {
		return nil, fmt.Errorf("load content-storage policy: %w", err)
	}
	p.policy = &content.PolicyHolder{}
	p.policy.Store(policy)

	// The data key comes from content-key-svc (the sole KMS holder), on its own connection: the body is
	// sealed here and never reaches that service (step-162/167). Lazy dial, like the billing one.
	p.conn, err = grpc.NewClient(cfg.ContentKey.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial content key service at %q: %w", cfg.ContentKey.Addr, err)
	}
	dekCache := content.NewDataKeyCache(content.NewGRPCDataKeyFetcher(contentkeypb.NewContentKeysClient(p.conn)))
	p.dropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "accepted_content_dropped_total",
		Help: "Message bodies dropped from the accepted CDR row because the content data key was unavailable.",
	})
	p.sealer = ingest.NewContentSealer(p.policy, dekCache, p.dropped, logger)

	p.kafka, err = kafka.NewConsumerFromLatest(cfg.Kafka, serviceName+"-accepted-cdr", kafka.TopicMTInbound)
	if err != nil {
		return nil, fmt.Errorf("kafka accepted-cdr consumer: %w", err)
	}
	p.consumer = ingest.NewAcceptedConsumer(p.kafka, clickhouse.NewCDRWriter(ch), p.sealer, logger)
	return p, nil
}

// outcomeProjector is the durable enroute/failed CDR projection (step-201c, D1): connector-pool-svc
// publishes each send outcome on mt.outcome and commits, and this consumer batches them into ClickHouse.
//
// It runs HERE, in router-svc, and not in the pool that produces it, for three reasons. It is the same
// projection the service already hosts for the accepted row — same shape, same store, same runbook — so
// the whole MT CDR projection is one thing to reason about and to scale. A pool pod is bound to ONE
// connector (its mt.routed group carries the connector id), whereas mt.outcome is fleet-wide: hosting it
// there would either give every connector's pool its own group — each writing every other connector's
// rows — or one group shared across pods of different connectors, making a connector's CDR depend on how
// many pods some other connector happens to run. And re-entering ClickHouse into the pool's process is
// what this step exists to undo: the pool's readiness already gates on ClickHouse, so a saturated store
// would drain the very pods that must keep submitting.
//
// Its own consumer group, separate from the accepted one, so neither projection can stall the other.
type outcomeProjector struct {
	projector *outcome.Projector
	kafka     *kafka.Consumer
}

func (p *outcomeProjector) close() {
	if p.kafka != nil {
		p.kafka.Close()
	}
}

// newOutcomeProjector wires the projection. Unlike the accepted one, a fresh group starts AT THE
// START, and the difference is not cosmetic.
//
// NewConsumerFromLatest exists for a group id that is per-instance, where a fresh group must not
// replay a topic that has been accumulating for months. Neither applies here: this group id is fixed
// and fleet-wide, and mt.outcome is a NEW topic — on the deploy that introduces it there is no history
// to replay, so the write-storm argument has no object.
//
// The error costs are wildly asymmetric. Starting at the start, on a topic that already holds
// outcomes, costs a burst of rewrites that the ReplacingMergeTree collapses. Starting at the end costs
// the outcomes produced before this consumer first joined — and connector-pool-svc may well have been
// rolled out first, since nothing orders the two. Those messages stay "accepted" for ever, and
// billing.Reaper settles orphan reservations against the recorded CDR outcome, so their reservations
// are held for good. Silently: no log, no metric, no error.
//
// The same applies on OffsetOutOfRange — a projector stopped longer than the topic's retention would
// otherwise skip straight to the end.
func newOutcomeProjector(cfg config.Config, ch *clickhouse.Conn, logger *slog.Logger) (*outcomeProjector, error) {
	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-outcome-cdr", kafka.TopicMTOutcome)
	if err != nil {
		return nil, fmt.Errorf("kafka outcome-cdr consumer: %w", err)
	}
	return &outcomeProjector{
		projector: outcome.NewProjector(consumer, clickhouse.NewCDRWriter(ch), logger),
		kafka:     consumer,
	}, nil
}

// metricStream is the realtime dashboard feed (§1.6, step-182). It is BEST-EFFORT throughout: a
// separate Kafka client (so a burst of snapshots can never fill the durable producer's buffer and
// stall a message being accepted), non-blocking publishes, and a nil emitter would simply disable it.
// Nothing here may fail a message — the CDR remains the authority for what happened.
type metricStream struct {
	producer *kafka.StreamProducer
	emitter  *metricstream.Emitter

	// dropped is the ONLY signal that the feed is degraded: nothing else fails when Kafka refuses a
	// snapshot.
	dropped prometheus.Collector
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
	m.dropped = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "metrics_stream_dropped_total",
		Help: "Realtime snapshots that never reached metrics.stream (full buffer, unreachable broker).",
	}, func() float64 { return float64(m.producer.Dropped()) })

	m.emitter, err = metricstream.New(serviceName, m.producer)
	if err != nil {
		return nil, fmt.Errorf("metric stream emitter: %w", err)
	}
	return m, nil
}

// bloomGauges track each in-memory Bloom filter's freshness and size. They are labelled by filter
// (exact | optout), with no unbounded label: the timestamp lets an alert fire on a stale filter, the
// capacity tracks growth after a reload.
type bloomGauges struct {
	reload   *prometheus.GaugeVec
	capacity *prometheus.GaugeVec
}

// set records a filter's reload instant and capacity.
func (g bloomGauges) set(filter string, capacityBits uint) {
	g.reload.WithLabelValues(filter).Set(float64(time.Now().Unix()))
	g.capacity.WithLabelValues(filter).Set(float64(capacityBits))
}

// newOpsServer builds the ops listener (not yet bound) and registers exactly the metrics this
// service feeds.
//
// Vital dependency (plan §1.5): Kafka alone. The router's core job — consume mt.inbound, normalise,
// route, publish mt.routed — needs only Kafka and the in-memory snapshot. ClickHouse is touched only
// to write a rejected CDR row, and Postgres is startup-only (immutable snapshot); gating readiness on
// either would drain healthy pods over a store the happy path does not use.
func newOpsServer(
	cfg config.Config,
	logger *slog.Logger,
	consumer *kafka.Consumer,
	catalog *metrics.Catalog,
	stack *pipelineStack,
	proj *acceptedProjector,
	stream *metricStream,
	boot *bootSnapshots,
) (*observability.OpsServer, bloomGauges, error) {
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
	)
	if err != nil {
		return nil, bloomGauges{}, fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(stack.failOpenTotal, proj.dropped)
	// Only the catalogue metrics this service actually feeds. Registering Collectors() wholesale would both
	// panic on the duplicate and expose always-zero series, which read as "measured, and nothing happened"
	// rather than "not measured here".
	ops.Registry().MustRegister(catalog.RoutingScriptFailures, catalog.QueueDepth,
		catalog.MessagesTotal, catalog.RejectedTotal, catalog.PipelineDuration, stream.dropped)

	blooms := bloomGauges{
		reload: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bloom_last_reload_timestamp_seconds",
			Help: "Unix time of the last successful in-memory Bloom filter reload.",
		}, []string{"filter"}),
		capacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bloom_capacity_bits",
			Help: "Bit-array size (m) of the in-memory Bloom filter after its last reload.",
		}, []string{"filter"}),
	}
	ops.Registry().MustRegister(blooms.reload, blooms.capacity)
	// Seed both series at boot so the freshness gauge reads "last load", not "last hot reload": a pod
	// that has not yet received an invalidation still reports current, non-absent filters.
	blooms.set("exact", stack.exactBloom.CapacityBits())
	blooms.set("optout", boot.optOut.CapacityBits())

	return ops, blooms, nil
}

// newSnapshotWatcher builds the hot-reload watcher (step-105/106): on a config-sync invalidation it
// rebuilds the immutable route snapshot and the two Bloom filters (exact-number routes, opt-out
// suppressions) and swaps each atomically — the readers pick up the new state lock-free, with no
// downtime and no routing hole. Each component keeps its current state on its own build failure; the
// returned error just makes the Watcher log and retry on the next notification (rebuilds are
// idempotent).
func newSnapshotWatcher(
	pool *pgxpool.Pool,
	rdb *goredis.Client,
	boot *bootSnapshots,
	stack *pipelineStack,
	proj *acceptedProjector,
	blooms bloomGauges,
	logger *slog.Logger,
) *config.Watcher {
	return config.NewWatcher(
		func(ctx context.Context) (config.Stream, error) {
			return redisstore.Subscribe(ctx, rdb, config.ChannelSnapshotInvalidation), nil
		},
		func(ctx context.Context) error {
			snap, err := routing.BuildSnapshot(ctx, postgres.NewRouteRepo(pool))
			if err != nil {
				return err
			}
			boot.routes.Swap(snap)
			// Refresh the breaker-availability overlay for the new snapshot's connectors. This is what makes
			// a breaker:events invalidation exclude a newly-open connector (and readmit a recovered one). A
			// read failure is not fatal to the rebuild: keep the freshly-swapped routes serving with the last
			// known availability and let the next invalidation retry.
			if aerr := boot.routes.RefreshAvailability(ctx, stack.breakerAvail); aerr != nil {
				logger.WarnContext(ctx, "refresh breaker availability failed; serving with stale availability", "err", aerr)
			}

			if err := stack.exactBloom.Reload(ctx, stack.exactRepo); err != nil {
				return err
			}
			blooms.set("exact", stack.exactBloom.CapacityBits())

			if err := boot.optOut.Reload(ctx, boot.suppressions, postgres.NewInboundNumberRepo(pool)); err != nil {
				return err
			}
			blooms.set("optout", boot.optOut.CapacityBits())

			// Rebuild the routing-script snapshot (recompiles the active scripts) and swap it in.
			scriptSnap, err := routing.BuildScriptSnapshot(ctx, stack.scriptRepo, logger)
			if err != nil {
				return err
			}
			stack.scriptResolver.Swap(scriptSnap)

			// Rebuild the billing-scope snapshot so a customers-config change (billing enabled/disabled, a
			// balance-scope flip) takes effect without a restart. Skipping this would strand the credit gate on
			// the boot snapshot: a customer newly enabled for billing would flow free until the pod restarts.
			bsnap, err := credit.LoadSnapshot(ctx, stack.billingRepo)
			if err != nil {
				return err
			}
			stack.creditHolder.Store(bsnap)

			// Rebuild the content-storage policy so a content_storage change takes effect without a restart —
			// most importantly an opt-out (stored_*→off, a consent withdrawal / GDPR request), which must stop
			// the data plane sealing bodies promptly rather than only at the next restart (step-102).
			csnap, err := content.LoadPolicySnapshot(ctx, postgres.NewCustomerRepo(pool))
			if err != nil {
				return err
			}
			proj.policy.Store(csnap)
			return nil
		},
		config.WithLogger(logger),
	)
}

// loadSnapshotWithRetry loads the route snapshot, retrying transient failures with capped
// exponential backoff until it succeeds or ctx is cancelled. Postgres is a hard boot dependency —
// the router cannot route without routes — so retrying (rather than exiting on the first error)
// keeps a (re)starting pod from being bricked by a transient Postgres outage.
func loadSnapshotWithRetry(ctx context.Context, lister routing.RouteLister, logger *slog.Logger) (*routing.SnapshotResolver, error) {
	return loadWithRetry(ctx, logger, "route snapshot", func(ctx context.Context) (*routing.SnapshotResolver, error) {
		return routing.LoadSnapshot(ctx, lister)
	})
}

// loadSenderIDSnapshotWithRetry loads the sender-ID authorization snapshot, retrying transient
// failures with capped exponential backoff until it succeeds or ctx is cancelled — the same boot
// discipline as the route snapshot (Postgres is a hard boot dependency).
func loadSenderIDSnapshotWithRetry(ctx context.Context, policies senderid.PolicyLister, ids senderid.ActiveSenderIDLister, logger *slog.Logger) (*senderid.Authorizer, error) {
	return loadWithRetry(ctx, logger, "sender-id snapshot", func(ctx context.Context) (*senderid.Authorizer, error) {
		return senderid.LoadSnapshot(ctx, policies, ids)
	})
}

// loadOptOutEnforcerWithRetry loads the opt-out Bloom snapshot and the inbound-number index and
// composes the enforcer, with the same boot-retry discipline.
func loadOptOutEnforcerWithRetry(ctx context.Context, suppressions optout.SuppressionLister, exact optout.ExactChecker, inbound optout.InboundNumberLister, logger *slog.Logger) (*optout.Enforcer, error) {
	return loadWithRetry(ctx, logger, "opt-out enforcer", func(ctx context.Context) (*optout.Enforcer, error) {
		snap, err := optout.LoadSnapshot(ctx, suppressions)
		if err != nil {
			return nil, err
		}
		index, err := optout.LoadInboundNumberIndex(ctx, inbound)
		if err != nil {
			return nil, err
		}
		return optout.NewEnforcer(optout.NewGuard(snap, exact), index), nil
	})
}

// loadAntispamWithRetry loads the anti-spam engine (compiled rule snapshot + Redis shared state),
// with the same boot-retry discipline as the other snapshots.
func loadAntispamWithRetry(ctx context.Context, lister antispam.RuleLister, state antispam.StateStore, metric antispam.Metric, logger *slog.Logger) (*antispam.Engine, error) {
	return loadWithRetry(ctx, logger, "anti-spam engine", func(ctx context.Context) (*antispam.Engine, error) {
		return antispam.New(ctx, lister, state, metric, logger)
	})
}

// loadWithRetry loads an immutable boot snapshot, retrying transient failures with capped exponential
// backoff until it succeeds or ctx is cancelled. Postgres is a hard boot dependency, so retrying
// (rather than exiting on the first error) keeps a (re)starting pod from being bricked by a transient
// Postgres outage.
func loadWithRetry[T any](ctx context.Context, logger *slog.Logger, what string, load func(context.Context) (T, error)) (T, error) {
	const (
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for attempt := 1; ; attempt++ {
		v, err := load(ctx)
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			var zero T
			return zero, ctx.Err()
		}
		logger.WarnContext(ctx, what+" load failed, retrying",
			"attempt", attempt, "backoff", backoff.String(), "err", err)
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// failOpenMetric adapts a Prometheus counter to antispam.Metric.
type failOpenMetric struct{ c prometheus.Counter }

func (m failOpenMetric) FailOpen() { m.c.Inc() }

// scriptMeter adapts the routing-script failure counter to routing.FailureMeter (bounded labels only).
type scriptMeter struct{ c *prometheus.CounterVec }

func (m scriptMeter) Inc(language, reason string) { m.c.WithLabelValues(language, reason).Inc() }

// connectorLoad reads the connectorload:{id} in-flight gauge from Redis for the least_loaded strategy.
// A missing key or a parse/read error reads 0 — least_loaded then degrades to a deterministic pick.
type connectorLoad struct{ rdb *goredis.Client }

func (c connectorLoad) InFlight(ctx context.Context, connectorID uuid.UUID) int {
	n, err := c.rdb.Get(ctx, "connectorload:{"+connectorID.String()+"}").Int()
	if err != nil {
		return 0
	}
	return n
}

// breakerAvailability reads breaker:state:{id} for a set of connectors (one MGET) and returns those that
// are open — the routing availability overlay (step-123). It is consulted only at snapshot rebuild, so
// the per-message resolve path never touches Redis. A missing key (nil) or an unparsable value counts as
// available (the breaker's own default is closed); an MGET error propagates so the caller keeps the last
// known availability rather than fencing the whole fleet.
type breakerAvailability struct{ rdb *goredis.Client }

func (b breakerAvailability) Unavailable(ctx context.Context, connectorIDs []uuid.UUID) (map[uuid.UUID]bool, error) {
	if len(connectorIDs) == 0 {
		return nil, nil
	}
	keys := make([]string, len(connectorIDs))
	for i, id := range connectorIDs {
		keys[i] = "breaker:state:{" + id.String() + "}"
	}
	// One MGET for every connector in the snapshot (cold path, once per rebuild). breaker:state keys use
	// a per-connector hash tag, so on a true Redis Cluster this would span slots (CROSSSLOT); the store
	// here is single-instance Redis/Dragonfly, where a multi-key read is served directly.
	vals, err := b.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	open := make(map[uuid.UUID]bool)
	for i, v := range vals {
		token, ok := v.(string)
		if !ok {
			continue // nil (key absent) or unexpected type → available
		}
		if st, ok := breaker.ParseState(token); ok && st == breaker.Open {
			open[connectorIDs[i]] = true
		}
	}
	return open, nil
}
