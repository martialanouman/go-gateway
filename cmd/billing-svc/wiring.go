package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// billingApp is billing-svc fully wired and not yet running: every connection is open, every component
// built, but no goroutine is started and no port is bound. Separating "assemble the graph" from "run it"
// is what makes the wiring testable — a test can build the whole service against test dependencies and
// assert it holds together, without a single credit moving.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a value
// the caller (or a test) can inspect.
type billingApp struct {
	ops  *observability.OpsServer
	grpc *grpc.Server
	// repo and configProvider are held for the supervised config refresh, which rebuilds the snapshot
	// from Postgres on a ticker.
	repo           *postgres.BillingRepo
	configProvider *billing.ConfigProvider
	reconciler     *billing.Reconciler
	reaper         *billing.Reaper

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred Closes
	// in run() used to provide.
	closers []func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *billingApp) onClose(f func()) { a.closers = append(a.closers, f) }

// close releases every connection the app holds. It is safe to call on a partially built app: only what
// was actually opened is registered.
func (a *billingApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// newBillingApp builds the whole billing graph: stores, accountant, external billing, the reaper stack,
// the gRPC server, the realtime alert feed and the ops server — in that order, which is the order in
// which a degraded dependency must surface.
//
// The order is load-bearing beyond taste: ClickHouse and Kafka open AFTER the initial billing-config
// load, so a boot that cannot read the config never opens them and reports the config failure rather
// than a store failure.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds nothing.
func newBillingApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *billingApp, err error) {
	a := &billingApp{}
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

	acct, err := newAccountant(ctx, st.pg, st.rdb, logger)
	if err != nil {
		return nil, err
	}
	a.repo = acct.repo
	a.configProvider = acct.configProvider

	ext := newExternalBilling(acct.acc, acct.repo, acct.configProvider, logger)
	a.reconciler = ext.reconciler

	reap, err := newReaperStack(cfg, acct.repo, acct.acc, logger)
	if err != nil {
		return nil, err
	}
	a.onClose(reap.close)
	a.reaper = reap.reaper

	a.grpc = grpc.NewServer()

	feed, err := newAlertFeed(cfg)
	if err != nil {
		return nil, err
	}
	a.onClose(feed.close)

	pb.RegisterBillingServer(a.grpc, billing.NewServer(ext.biller, acct.repo, feed.alerts))

	collectors := make([]prometheus.Collector, 0, len(ext.collectors)+len(reap.collectors)+len(feed.collectors))
	collectors = append(collectors, ext.collectors...)
	collectors = append(collectors, reap.collectors...)
	collectors = append(collectors, feed.collectors...)

	a.ops, err = newOpsServer(cfg, logger, st.rdb, st.pg, collectors)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores are the external connections billing-svc must hold before it can account for anything: Redis
// (the operational balance cache) and Postgres (the durable ledger authority).
//
// ClickHouse and Kafka are NOT here on purpose — see newBillingApp's comment on ordering.
type stores struct {
	rdb *goredis.Client
	pg  *pgxpool.Pool
}

// openStores opens them in dependency-free order, releasing what it already holds if a later one fails.
func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.rdb, err = redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("open redis client: %w", err)
	}

	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	return s, nil
}

// close releases the connections in reverse order of opening; a nil field is one that was never opened.
func (s *stores) close() {
	if s.pg != nil {
		s.pg.Close()
	}
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
}

// accountant is the credit core: the ledger repository, the config snapshot it serves from, and the
// accountant itself.
type accountant struct {
	acc            *billing.Accountant
	repo           *postgres.BillingRepo
	configProvider *billing.ConfigProvider
}

// newAccountant builds the core and loads its first config snapshot.
//
// The initial load is deliberately fatal: without it every customer's overdraft and MO floor would
// silently fall back to strict prepaid until the first refresh tick. A LATER refresh failure is not
// fatal — the previous snapshot keeps serving (see runConfigRefresh).
func newAccountant(ctx context.Context, pool *pgxpool.Pool, rdb *goredis.Client, logger *slog.Logger) (*accountant, error) {
	a := &accountant{
		repo:           postgres.NewBillingRepo(pool),
		configProvider: &billing.ConfigProvider{},
	}
	a.acc = billing.New(rdb, a.repo, billing.WithConfigSource(a.configProvider), billing.WithLogger(logger))
	if err := a.acc.EnsureNonClustered(ctx); err != nil {
		return nil, err
	}
	if err := refreshConfig(ctx, a.repo, a.configProvider); err != nil {
		return nil, fmt.Errorf("initial billing config load: %w", err)
	}
	return a, nil
}

// externalBilling decorates the accountant with external authorization (§6.10, step-147) and its
// report-only reconciliation pass.
type externalBilling struct {
	biller     *billing.ExternalBiller
	reconciler *billing.Reconciler
	collectors []prometheus.Collector
}

// newExternalBilling wires the stub provider: a real HTTP provider is deferred, so a local allow-all
// stub stands in — the mode/timeout/policy still ride the config snapshot, so external-billing config is
// exercised end to end. Metrics are labelled by provider (bounded): fail-open passes (a dead provider
// silently authorizing) and reconciliation discrepancies both drive alerts.
func newExternalBilling(acc *billing.Accountant, repo *postgres.BillingRepo, configSource billing.ConfigSource, logger *slog.Logger) *externalBilling {
	failOpenTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "billing_external_authz_fail_open_total",
		Help: "External billing authorizations that failed open (proceeded unconfirmed), by provider.",
	}, []string{"provider"})
	discrepancyTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "billing_external_discrepancy_total",
		Help: "External billing reconciliation discrepancies, by provider.",
	}, []string{"provider"})

	provider := billing.NewStubProvider()
	return &externalBilling{
		biller: billing.NewExternalBiller(acc, configSource, provider,
			billing.WithExternalMetric(extFailOpenMetric{c: failOpenTotal}), billing.WithExternalLogger(logger)),
		reconciler: billing.NewReconciler(repo, provider,
			billing.WithDiscrepancyMetric(extDiscrepancyMetric{c: discrepancyTotal}),
			billing.WithTolerance(reconcileTolerance), billing.WithReconcileLogger(logger)),
		collectors: []prometheus.Collector{failOpenTotal, discrepancyTotal},
	}
}

// reaperStack is the reaper and the read-only ClickHouse connection it settles against.
type reaperStack struct {
	reaper     *billing.Reaper
	ch         *clickhouse.Conn
	collectors []prometheus.Collector
}

// newReaperStack opens the CDR reader and builds the reaper (step-190): the net under the connector
// pool's fail-open settle. A billing fault there is swallowed (propagating it would redeliver the record
// and re-send the SMS), so an outage leaves reserve debits standing with nothing to close them. The
// reaper sweeps them from the ledger and settles each against the CDR outcome.
//
// ClickHouse is read-only here and NOT a readiness dependency: the reaper is a periodic background job,
// so a ClickHouse outage must not take billing-svc out of the load balancer — the reservations simply
// wait for a later pass. See newOpsServer, where that choice is made, and the test that pins it.
func newReaperStack(cfg config.Config, repo *postgres.BillingRepo, settler billing.ReaperSettler, logger *slog.Logger) (_ *reaperStack, err error) {
	r := &reaperStack{}
	defer func() {
		if err != nil {
			r.close()
		}
	}()

	r.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	// reaper_reaped_total{action}: bounded label (capture|release). reaper_unresolvable_total counts
	// reservations left INTACT because their outcome could not be established — never released on a guess,
	// since refunding a really-sent message is a free delivery. A rising unresolvable count is an audit gap
	// and MUST alert; reaped is the ordinary rate of self-healing.
	reapedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "reaper_reaped_total",
		Help: "Orphaned MT reservations reconciled by the reaper, by settlement action.",
	}, []string{"action"})
	unresolvableTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reaper_unresolvable_total",
		Help: "Orphaned MT reservations left intact because their outcome could not be established (MUST alert).",
	})

	r.reaper = billing.NewReaper(repo, clickhouse.NewCDRReader(r.ch), settler,
		billing.WithMinAge(cfg.Billing.ReaperMinAge),
		billing.WithReaperMetric(reaperMetric{reaped: reapedTotal, unresolvable: unresolvableTotal}),
		billing.WithReaperLogger(logger))
	r.collectors = []prometheus.Collector{reapedTotal, unresolvableTotal}
	return r, nil
}

func (r *reaperStack) close() {
	if r.ch != nil {
		_ = r.ch.Close()
	}
}

// alertFeed is the realtime billing alert publisher (step-184) and the Kafka client behind it.
type alertFeed struct {
	producer   *kafka.StreamProducer
	alerts     *metricstream.EventPublisher
	collectors []prometheus.Collector
}

// newAlertFeed opens the feed. Best-effort by construction: a separate Kafka client that drops rather
// than blocks, so an alert can never delay or fail a billing call. Kafka is correspondingly out of
// readiness.
func newAlertFeed(cfg config.Config) (*alertFeed, error) {
	producer, err := kafka.NewStreamProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka stream producer: %w", err)
	}

	alerts := metricstream.NewEventPublisher(serviceName, producer)
	return &alertFeed{
		producer:   producer,
		alerts:     alerts,
		collectors: streamDropCollectors(producer, alerts),
	}, nil
}

func (f *alertFeed) close() {
	if f.producer != nil {
		f.producer.Close()
	}
}

// newOpsServer builds the ops port and registers this service's collectors on its guarded registry.
//
// Both Redis and Postgres are vital: a balance can be neither served nor rehydrated without them, so a
// pod that loses either must leave the load balancer (plan §1.5). Nothing else belongs here — ClickHouse
// and Kafka back background work only, and registering a non-vital dependency would remove healthy pods
// over a degradation they absorb.
func newOpsServer(cfg config.Config, logger *slog.Logger, rdb *goredis.Client, pool *pgxpool.Pool, collectors []prometheus.Collector) (*observability.OpsServer, error) {
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)

	ops, err := observability.NewOpsServer(cfg, logger, redisCheck, pgCheck)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(collectors...)
	return ops, nil
}

// extFailOpenMetric adapts the fail-open counter to billing.ExternalMetric (bounded provider label).
type extFailOpenMetric struct{ c *prometheus.CounterVec }

func (m extFailOpenMetric) AuthzFailOpen(providerID uuid.UUID) {
	m.c.WithLabelValues(providerID.String()).Inc()
}

// extDiscrepancyMetric adapts the discrepancy counter to billing.DiscrepancyMetric.
type extDiscrepancyMetric struct{ c *prometheus.CounterVec }

func (m extDiscrepancyMetric) Discrepancy(providerID uuid.UUID) {
	m.c.WithLabelValues(providerID.String()).Inc()
}

// reaperMetric adapts the reaper counters to billing.ReaperMetric.
type reaperMetric struct {
	reaped       *prometheus.CounterVec
	unresolvable prometheus.Counter
}

func (m reaperMetric) Reaped(action string) { m.reaped.WithLabelValues(action).Inc() }
func (m reaperMetric) Unresolvable()        { m.unresolvable.Inc() }

// streamDropCollectors meters both ends of this service's alert feed: what the transport refused, and what
// the publisher could not serialise. Metering the transport alone is what let realtime records vanish in
// silence (step-210), and a named function is what lets the wiring be scraped in a test.
//
// No rate_cap series here, deliberately: the publisher's cap guards session events only, and Alerted is
// exempt because a floor alert fires once per owner per period. Exposing a counter that can only ever read
// zero would tell an operator these alerts are throttled when nothing throttles them.
func streamDropCollectors(transport metrics.DropCounter, alerts *metricstream.EventPublisher) []prometheus.Collector {
	return []prometheus.Collector{
		metrics.StreamDropCollector("buffer", transport),
		metrics.StreamDropCollector("encode", metrics.DropCounterFunc(alerts.DroppedUnserializable)),
	}
}
