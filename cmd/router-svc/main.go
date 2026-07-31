// Command router-svc runs the MT pipeline: it consumes mt.inbound, normalises and routes each
// message, and publishes mt.routed (M2). A message rejected by the pipeline gets a rejected CDR row
// so get-message reflects it. The lifecycle — load and validate config, install telemetry, load the
// route snapshot, serve the ops port, run the consumer, drain on SIGTERM — follows the canonical
// skeleton (guide de codage §5).
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/content"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/credit"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// serviceName identifies this binary in logs, traces and metrics, and is the consumer group id.
const serviceName = "router-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres for the startup route snapshot, Kafka for the data plane, ClickHouse for rejected
	// CDR rows. No HTTP: the router has no client-facing listener.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse, config.SectionRedis, config.SectionBilling)
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

	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTInbound)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// The route snapshot is loaded once at startup (config-sync's hot reload is a later milestone).
	// Postgres is a hard boot dependency — the router cannot route without routes — so a transient
	// outage must not permanently brick a (re)starting pod: retry with backoff until Postgres answers
	// or the context is cancelled, rather than exiting on the first failure.
	snapshot, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load route snapshot: %w", err)
	}

	// The sender-ID authorization snapshot (§6.19) is a second immutable boot dependency, loaded with
	// the same retry discipline as the routes: the account policies and the customers' active sender
	// IDs, indexed once for lock-free per-message checks.
	senderIDs, err := loadSenderIDSnapshotWithRetry(ctx, postgres.NewAccountRepo(pool), postgres.NewSenderIDRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load sender-id snapshot: %w", err)
	}

	// The opt-out enforcer (§6.20) is a third immutable boot dependency: a per-scope Bloom over the
	// suppressions (exact confirmation behind it) and an inbound-number index to resolve the sending
	// number's scope. Same boot-retry discipline as the routes.
	suppressions := postgres.NewSuppressionRepo(pool)
	optOut, err := loadOptOutEnforcerWithRetry(ctx, suppressions, suppressions, postgres.NewInboundNumberRepo(pool), logger)
	if err != nil {
		return fmt.Errorf("load opt-out enforcer: %w", err)
	}

	// The anti-spam engine (§6.20): content rules compiled once at boot, duplicates checked in Redis.
	// Redis is a boot dependency (NewClient pings eagerly), so it retries with the same discipline as
	// the other snapshots — a transient outage must not crashloop a (re)starting pod.
	rdb, err := loadWithRetry(ctx, logger, "redis", func(ctx context.Context) (*goredis.Client, error) {
		return redisstore.NewClient(ctx, cfg.Redis)
	})
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	// least_loaded reads each connector's published in-flight gauge (connectorload:{id}) from Redis —
	// the mutable overlay beside the immutable route snapshot (step-104/114). A missing gauge reads 0.
	snapshot.UseLoadReader(connectorLoad{rdb: rdb})
	// Breaker awareness (step-123): read each connector's aggregate breaker:state once at boot to seed the
	// availability overlay, then refresh it on every snapshot rebuild below. Reading here (not per message)
	// keeps the hot path off Redis. A transient read failure just leaves every connector available.
	breakerAvail := breakerAvailability{rdb: rdb}
	if err := snapshot.RefreshAvailability(ctx, breakerAvail); err != nil {
		logger.WarnContext(ctx, "seed breaker availability failed; starting with all connectors available", "err", err)
	}
	// anti_spam_fail_open_total: bounded (no labels) — counts messages let through because a
	// Redis-backed anti-spam check could not run (§1.5 fail-open). Never a MSISDN or body.
	failOpenTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "anti_spam_fail_open_total",
		Help: "Messages passed (flagged) because a Redis-backed anti-spam check failed open.",
	})
	spam, err := loadAntispamWithRetry(ctx, postgres.NewAntispamRuleRepo(pool), antispam.NewRedisState(rdb), failOpenMetric{c: failOpenTotal}, logger)
	if err != nil {
		return fmt.Errorf("load anti-spam engine: %w", err)
	}

	// The rate-limit snapshot (§6.4): the operational rate_limits plus each connector's
	// throughput_limit_per_sec hard ceiling, indexed once at boot. The token bucket that enforces it
	// lives in Redis (shared across pods) and fails closed on an outage (step-084). Same boot-retry
	// discipline as the other snapshots.
	rateSnap, err := loadWithRetry(ctx, logger, "rate-limit snapshot", func(ctx context.Context) (*ratelimit.Snapshot, error) {
		return ratelimit.LoadSnapshot(ctx, postgres.NewRateLimitRepo(pool), postgres.NewConnectorRepo(pool))
	})
	if err != nil {
		return fmt.Errorf("load rate-limit snapshot: %w", err)
	}
	rateLimiter := ratelimit.NewEnforcer(rateSnap, ratelimit.NewLimiter(rdb))

	// The L0 exact-number short-cut (§6.1): an in-memory Bloom over every exact_routes MSISDN, loaded
	// once at boot with the same retry discipline, in front of the shared Redis map. A ported number
	// routes straight to its target (skipping route resolution only, never compliance); every other
	// number falls through to the declarative resolver. Config-sync's hot reload is a later milestone.
	exactRepo := postgres.NewExactRouteRepo(pool)
	exactBloom, err := loadWithRetry(ctx, logger, "exact-route bloom", func(ctx context.Context) (*exact.Bloom, error) {
		return exact.LoadBloom(ctx, exactRepo)
	})
	if err != nil {
		return fmt.Errorf("load exact-route bloom: %w", err)
	}
	// The L1 routing-script stage (§6.1): active scripts compiled once into an immutable snapshot,
	// scope-resolved per message (account → customer → platform). A null result or ANY script fault
	// falls back to declarative resolution and is metered; the stage sits between exact (L0) and
	// declarative (L2), never before compliance.
	scriptRepo := postgres.NewRoutingScriptRepo(pool)
	scriptSnap, err := loadWithRetry(ctx, logger, "routing-script snapshot", func(ctx context.Context) (*routing.ScriptSnapshot, error) {
		return routing.BuildScriptSnapshot(ctx, scriptRepo, logger)
	})
	if err != nil {
		return fmt.Errorf("load routing scripts: %w", err)
	}
	// routing_script_failures_total: bounded labels (runtime, reason) — never a body or script text.
	scriptFailures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "routing_script_failures_total",
		Help: "Routing scripts that fell back to declarative resolution, by runtime and reason.",
	}, []string{"runtime", "reason"})
	scriptResolver := routing.NewScriptResolver(scriptSnap, logger, scriptMeter{c: scriptFailures})
	resolver := routing.NewL0Resolver(exact.NewResolver(exactBloom, rdb), scriptResolver, snapshot)

	// The credit stage (§6.9, step-145): a per-customer billing-scope snapshot gates the reserve, so a
	// billing-disabled customer makes ZERO billing round-trip, and the billing gRPC client reserves credit
	// for the rest. billing-svc is reached lazily (grpc.NewClient opens no connection until the first RPC),
	// so a router that bills nobody never touches it and a down billing-svc does not block startup. The
	// scope snapshot is a boot dependency with the same retry discipline as the others, and — crucially — is
	// rebuilt on every config invalidation below, so enabling billing for a customer takes effect without a
	// restart.
	billingRepo := postgres.NewBillingRepo(pool)
	creditSnap, err := loadWithRetry(ctx, logger, "billing scope snapshot", func(ctx context.Context) (*credit.Snapshot, error) {
		return credit.LoadSnapshot(ctx, billingRepo)
	})
	if err != nil {
		return fmt.Errorf("load billing scope snapshot: %w", err)
	}
	var creditHolder credit.Holder
	creditHolder.Store(creditSnap)
	billingConn, err := grpc.NewClient(cfg.Billing.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial billing at %q: %w", cfg.Billing.Addr, err)
	}
	defer func() { _ = billingConn.Close() }()
	reserver := credit.NewReserver(&creditHolder, pb.NewBillingClient(billingConn), credit.WithTimeout(cfg.Billing.ReserveTimeout))

	// Accepted CDR projection (step-101): the accepted row is projected durably off mt.inbound by a DEDICATED
	// consumer group, separate from routing, committing at-least-once after the ClickHouse write so no accepted
	// row is ever lost (a ClickHouse fault reprocesses the batch; a slow ClickHouse only lengthens the
	// get-message 404 window, never the routing path). It also seals the body into the CDR per content_storage
	// (step-162): the DEK comes from billing-svc via a TTL cache, so the body never reaches billing-svc; an
	// unavailable DEK degrades to no-content (counted), never a stall. A fresh group starts at the LATEST
	// offset — replaying the whole retained topic would re-insert every historical accepted row and storm
	// billing for DEKs — so the deploy pins the start offset (runbook).
	contentPolicy, err := content.LoadPolicySnapshot(ctx, postgres.NewCustomerRepo(pool))
	if err != nil {
		return fmt.Errorf("load content-storage policy: %w", err)
	}
	dekCache := content.NewDataKeyCache(content.NewGRPCDataKeyFetcher(pb.NewContentKeysClient(billingConn)))
	contentDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "accepted_content_dropped_total",
		Help: "Message bodies dropped from the accepted CDR row because the content data key was unavailable.",
	})
	sealer := ingest.NewContentSealer(contentPolicy, dekCache, contentDropped, logger)
	acceptedConsumer, err := kafka.NewConsumerFromLatest(cfg.Kafka, serviceName+"-accepted-cdr", kafka.TopicMTInbound)
	if err != nil {
		return fmt.Errorf("kafka accepted-cdr consumer: %w", err)
	}
	defer acceptedConsumer.Close()
	acceptedProjector := ingest.NewAcceptedConsumer(acceptedConsumer, clickhouse.NewCDRWriter(chConn), sealer, logger)

	tracer := observability.Tracer(nil, serviceName)
	pl := pipeline.New(tracer, resolver, senderIDs, optOut, spam, rateLimiter, reserver)
	rtr := router.New(router.Deps{
		Consumer: consumer,
		Producer: producer,
		Pipeline: pl,
		CDR:      clickhouse.NewCDRWriter(chConn),
		Tracer:   tracer,
		Logger:   logger,
	})

	// Vital dependency (plan §1.5): Kafka alone. The router's core job — consume mt.inbound,
	// normalise, route, publish mt.routed — needs only Kafka and the in-memory snapshot. ClickHouse
	// is touched only to write a rejected CDR row, and Postgres is startup-only (immutable snapshot);
	// gating readiness on either would drain healthy pods over a store the happy path does not use.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(failOpenTotal, scriptFailures, contentDropped)

	// bloom_last_reload / bloom_capacity_bits: labelled by filter (exact | optout), no unbounded labels.
	// The timestamp lets an alert fire on a stale filter; the capacity tracks growth after a reload.
	bloomReloadTimestamp := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloom_last_reload_timestamp_seconds",
		Help: "Unix time of the last successful in-memory Bloom filter reload.",
	}, []string{"filter"})
	bloomCapacityBits := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bloom_capacity_bits",
		Help: "Bit-array size (m) of the in-memory Bloom filter after its last reload.",
	}, []string{"filter"})
	ops.Registry().MustRegister(bloomReloadTimestamp, bloomCapacityBits)
	// Seed both series at boot so the freshness gauge reads "last load", not "last hot reload": a pod
	// that has not yet received an invalidation still reports current, non-absent filters.
	bootNow := float64(time.Now().Unix())
	bloomReloadTimestamp.WithLabelValues("exact").Set(bootNow)
	bloomCapacityBits.WithLabelValues("exact").Set(float64(exactBloom.CapacityBits()))
	bloomReloadTimestamp.WithLabelValues("optout").Set(bootNow)
	bloomCapacityBits.WithLabelValues("optout").Set(float64(optOut.CapacityBits()))

	// Hot reload (step-105/106): on a config-sync invalidation, rebuild the immutable route snapshot and
	// the two Bloom filters (exact-number routes, opt-out suppressions) and swap each atomically — the
	// readers pick up the new state lock-free, with no downtime and no routing hole. Each component
	// keeps its current state on its own build failure; the returned error just makes the Watcher log
	// and retry on the next notification (rebuilds are idempotent).
	snapshotWatcher := config.NewWatcher(
		func(ctx context.Context) (config.Stream, error) {
			return redisstore.Subscribe(ctx, rdb, config.ChannelSnapshotInvalidation), nil
		},
		func(ctx context.Context) error {
			snap, berr := routing.BuildSnapshot(ctx, postgres.NewRouteRepo(pool))
			if berr != nil {
				return berr
			}
			snapshot.Swap(snap)
			// Refresh the breaker-availability overlay for the new snapshot's connectors. This is what makes
			// a breaker:events invalidation exclude a newly-open connector (and readmit a recovered one). A
			// read failure is not fatal to the rebuild: keep the freshly-swapped routes serving with the last
			// known availability and let the next invalidation retry.
			if aerr := snapshot.RefreshAvailability(ctx, breakerAvail); aerr != nil {
				logger.WarnContext(ctx, "refresh breaker availability failed; serving with stale availability", "err", aerr)
			}

			if berr := exactBloom.Reload(ctx, exactRepo); berr != nil {
				return berr
			}
			bloomReloadTimestamp.WithLabelValues("exact").Set(float64(time.Now().Unix()))
			bloomCapacityBits.WithLabelValues("exact").Set(float64(exactBloom.CapacityBits()))

			if berr := optOut.Reload(ctx, suppressions, postgres.NewInboundNumberRepo(pool)); berr != nil {
				return berr
			}
			bloomReloadTimestamp.WithLabelValues("optout").Set(float64(time.Now().Unix()))
			bloomCapacityBits.WithLabelValues("optout").Set(float64(optOut.CapacityBits()))

			// Rebuild the routing-script snapshot (recompiles the active scripts) and swap it in.
			scriptSnap, berr := routing.BuildScriptSnapshot(ctx, scriptRepo, logger)
			if berr != nil {
				return berr
			}
			scriptResolver.Swap(scriptSnap)

			// Rebuild the billing-scope snapshot so a customers-config change (billing enabled/disabled, a
			// balance-scope flip) takes effect without a restart. Skipping this would strand the credit gate on
			// the boot snapshot: a customer newly enabled for billing would flow free until the pod restarts.
			bsnap, berr := credit.LoadSnapshot(ctx, billingRepo)
			if berr != nil {
				return berr
			}
			creditHolder.Store(bsnap)
			return nil
		},
		config.WithLogger(logger),
	)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router pipeline tear down together — neither must outlive the other — so the
	// unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("router", rtr.Run)
	// The accepted-CDR projector is SELF-RESTARTING rather than a plain g.Add: it writes ClickHouse on every
	// message, so a transient ClickHouse fault surfaces as a RunBatch error (uncommitted offset → reprocess).
	// If that error propagated to the unordered supervisor it would tear down the whole process — routing
	// included — over a store the routing happy path never touches (see the readiness rationale above). Instead
	// we absorb it: on a fault we back off and resume the consumer (reprocessing from the last commit, so no
	// accepted row is lost), and only ctx cancellation stops it. ClickHouse thus stays non-vital for routing.
	g.Add("accepted cdr", func(c context.Context) error {
		return runResilient(c, "accepted-cdr projector", acceptedProjector.Run, logger)
	})
	g.Add("snapshot watcher", snapshotWatcher.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
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

// acceptedProjectorBackoff is the pause between restarts of the accepted-CDR projector after a transient
// fault (a ClickHouse blip), long enough to let the store recover without a tight crash loop, short enough
// to keep the get-message 404 window from lengthening unduly.
const acceptedProjectorBackoff = 2 * time.Second

// runResilient runs a supervised loop that survives transient faults: it restarts run after a bounded backoff
// on any non-nil error, and returns only when ctx is cancelled (a clean stop). It lets one component tolerate
// a dependency blip without failing the whole process — used for the accepted-CDR projector so a ClickHouse
// fault reprocesses (at-least-once) instead of crashing router-svc's routing path.
func runResilient(ctx context.Context, name string, run func(context.Context) error, logger *slog.Logger) error {
	for {
		err := run(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		logger.ErrorContext(ctx, "component faulted; restarting after backoff", "component", name, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(acceptedProjectorBackoff):
		}
	}
}
