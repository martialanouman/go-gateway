// Command billing-svc serves the Billing gRPC API: the opt-in MT/MO credit accounting core (plan §7, M9,
// gRPC :7001). It follows the canonical service lifecycle of cmd/session-manager-svc, backing the
// accountant with Redis (the operational balance cache) and Postgres (the durable ledger authority), and
// exposing a gRPC listener plus a periodic config-snapshot refresh as supervised components alongside the
// ops port. The plan assigns billing-svc port 7001; the deploy sets GRPC_PORT=7001 (the shared config
// default 7000 is session-manager-svc's, §1.4).
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

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

const serviceName = "billing-svc"

// configRefreshInterval is how often billing-svc rebuilds its per-customer billing-config snapshot from
// Postgres. Floor config changes are rare and non-urgent, so a periodic reload keeps the hot-path read
// lock-free without a change-stream dependency (config-sync push can replace it later).
const configRefreshInterval = 30 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Billing needs Redis (the balance cache), Postgres (the durable ledger) and gRPC; Kafka only for the best-effort realtime alert feed (deliberately out of readiness), no HTTP.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionRedis, config.SectionPostgres,
		config.SectionGRPC, config.SectionKafka, config.SectionClickHouse)
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

	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("open redis client: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	repo := postgres.NewBillingRepo(pool)
	configProvider := &billing.ConfigProvider{}
	acc := billing.New(rdb, repo, billing.WithConfigSource(configProvider), billing.WithLogger(logger))
	if err := acc.EnsureNonClustered(ctx); err != nil {
		return err
	}
	// Load the initial billing-config snapshot before serving, so a customer's overdraft/MO floor is in
	// force from the first request. A load failure at boot is fatal (we would otherwise serve strict
	// prepaid to everyone silently); a later refresh failure only keeps the previous snapshot.
	if err := refreshConfig(ctx, repo, configProvider); err != nil {
		return fmt.Errorf("initial billing config load: %w", err)
	}

	// External billing provider (§6.10, step-147): a pluggable provider decorates the accountant with
	// external authorization. A real HTTP provider is deferred, so a local stub (allow-all, no network) is
	// wired — the mode/timeout/policy still ride the config snapshot, so external-billing config is exercised
	// end to end. Metrics are labelled by provider (bounded): fail-open passes (a dead provider silently
	// authorizing) and reconciliation discrepancies both drive alerts.
	externalFailOpenTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "billing_external_authz_fail_open_total",
		Help: "External billing authorizations that failed open (proceeded unconfirmed), by provider.",
	}, []string{"provider"})
	externalDiscrepancyTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "billing_external_discrepancy_total",
		Help: "External billing reconciliation discrepancies, by provider.",
	}, []string{"provider"})
	provider := billing.NewStubProvider()
	biller := billing.NewExternalBiller(acc, configProvider, provider,
		billing.WithExternalMetric(extFailOpenMetric{c: externalFailOpenTotal}), billing.WithExternalLogger(logger))
	reconciler := billing.NewReconciler(repo, provider,
		billing.WithDiscrepancyMetric(extDiscrepancyMetric{c: externalDiscrepancyTotal}),
		billing.WithTolerance(reconcileTolerance), billing.WithReconcileLogger(logger))

	// The reaper (step-190) is the net under the connector pool's fail-open settle: a billing fault there is
	// swallowed (propagating it would redeliver the record and re-send the SMS), so an outage leaves reserve
	// debits standing with nothing to close them. It sweeps them from the ledger and settles each against the
	// CDR outcome. ClickHouse is read-only here and NOT a readiness dependency: the reaper is a periodic
	// background job, so a ClickHouse outage must not take billing-svc out of the load balancer — the
	// reservations simply wait for a later pass.
	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	// reaper_reaped_total{action}: bounded label (capture|release). reaper_unresolvable_total counts
	// reservations left INTACT because their outcome could not be established — never released on a guess,
	// since refunding a really-sent message is a free delivery. A rising unresolvable count is an audit gap
	// and MUST alert; reaped is the ordinary rate of self-healing.
	reaperReapedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "reaper_reaped_total",
		Help: "Orphaned MT reservations reconciled by the reaper, by settlement action.",
	}, []string{"action"})
	reaperUnresolvableTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reaper_unresolvable_total",
		Help: "Orphaned MT reservations left intact because their outcome could not be established (MUST alert).",
	})
	reaper := billing.NewReaper(repo, clickhouse.NewCDRReader(chConn), acc,
		billing.WithMinAge(cfg.Billing.ReaperMinAge),
		billing.WithReaperMetric(reaperMetric{reaped: reaperReapedTotal, unresolvable: reaperUnresolvableTotal}),
		billing.WithReaperLogger(logger))

	grpcServer := grpc.NewServer()
	// Realtime billing alerts (step-184). Best-effort: a separate Kafka client that drops rather than blocks,
	// so an alert can never delay or fail a billing call.
	streamProducer, err := kafka.NewStreamProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka stream producer: %w", err)
	}
	defer streamProducer.Close()
	alerts := metricstream.NewEventPublisher(serviceName, streamProducer)
	streamDropped := streamDropCollectors(streamProducer, alerts)

	pb.RegisterBillingServer(grpcServer, billing.NewServer(biller, repo, alerts))

	// Both Redis and Postgres are vital: a balance can be neither served nor rehydrated without them, so a
	// pod that loses either must leave the load balancer (plan §1.5).
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, redisCheck, pgCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(externalFailOpenTotal, externalDiscrepancyTotal,
		reaperReapedTotal, reaperUnresolvableTotal)
	ops.Registry().MustRegister(streamDropped...)

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("grpc server", func(c context.Context) error {
		return runGRPC(c, grpcServer, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	g.Add("config refresh", func(c context.Context) error {
		return runConfigRefresh(c, repo, configProvider, logger)
	})
	g.Add("external reconcile", func(c context.Context) error {
		return runReconcile(c, reconciler, logger)
	})
	g.Add("reservation reaper", func(c context.Context) error {
		return runReap(c, reaper, cfg.Billing.ReaperInterval, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// refreshConfig rebuilds the billing-config snapshot from Postgres and stores it in the provider.
func refreshConfig(ctx context.Context, lister billing.CustomerLister, p *billing.ConfigProvider) error {
	snap, err := billing.LoadConfigSnapshot(ctx, lister)
	if err != nil {
		return err
	}
	p.Store(snap)
	return nil
}

// runConfigRefresh periodically rebuilds the billing-config snapshot until ctx is cancelled. A rebuild
// error is logged, not fatal: the previous snapshot keeps serving (staleness over a mass strict-prepaid
// downgrade). The ticker gives the goroutine its stop condition.
func runConfigRefresh(ctx context.Context, lister billing.CustomerLister, p *billing.ConfigProvider, logger *slog.Logger) error {
	ticker := time.NewTicker(configRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := refreshConfig(ctx, lister, p); err != nil {
				logger.WarnContext(ctx, "billing: config refresh failed — serving previous snapshot", "err", err)
			}
		}
	}
}

// reconcileInterval is how often billing-svc reconciles local settled consumption against external providers
// (§6.10). Reconciliation is non-urgent and report-only, so a slow cadence keeps its per-customer reads well
// off the hot path.
const reconcileInterval = 5 * time.Minute

// reconcileTolerance is the credit difference below which a local/external mismatch is ignored, not alerted.
// ConsumedCredits excludes in-flight holds while a provider's Usage may include them, so a busy customer
// almost always differs by the in-flight amount at tick time; a zero tolerance would alert chronically. This
// is a conservative starting point — real per-deployment tuning (or a relative threshold) is a follow-up.
const reconcileTolerance = 100

// runReconcile periodically runs one external-billing reconciliation pass until ctx is cancelled. A pass
// error is logged, not fatal: the next tick retries. Being a supervised ticker that threads ctx into the
// pass, it drains cleanly on shutdown (the in-flight pass's reads honour the cancelled context).
func runReconcile(ctx context.Context, r *billing.Reconciler, logger *slog.Logger) error {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				logger.WarnContext(ctx, "billing: external reconciliation pass failed — retrying next tick", "err", err)
			}
		}
	}
}

// extFailOpenMetric adapts the fail-open counter to billing.ExternalMetric (bounded provider label).
type extFailOpenMetric struct{ c *prometheus.CounterVec }

func (m extFailOpenMetric) AuthzFailOpen(providerID uuid.UUID) {
	m.c.WithLabelValues(providerID.String()).Inc()
}

// runReap periodically runs one reaper pass until ctx is cancelled. A pass error is logged, not fatal: the
// next tick retries, and a reservation left open one more cycle costs nothing (the money is already
// recorded — only its settlement is late). Being a supervised ticker threading ctx into the pass, it
// drains cleanly on shutdown.
func runReap(ctx context.Context, r *billing.Reaper, every time.Duration, logger *slog.Logger) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.ReapOnce(ctx); err != nil {
				logger.WarnContext(ctx, "billing: reaper pass failed — retrying next tick", "err", err)
			}
		}
	}
}

// reaperMetric adapts the reaper counters to billing.ReaperMetric.
type reaperMetric struct {
	reaped       *prometheus.CounterVec
	unresolvable prometheus.Counter
}

func (m reaperMetric) Reaped(action string) { m.reaped.WithLabelValues(action).Inc() }
func (m reaperMetric) Unresolvable()        { m.unresolvable.Inc() }

// extDiscrepancyMetric adapts the discrepancy counter to billing.DiscrepancyMetric.
type extDiscrepancyMetric struct{ c *prometheus.CounterVec }

func (m extDiscrepancyMetric) Discrepancy(providerID uuid.UUID) {
	m.c.WithLabelValues(providerID.String()).Inc()
}

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

// runGRPC serves the Billing API until ctx is cancelled, then drains within timeout. GracefulStop lets
// in-flight RPCs finish; if they overrun the window a hard Stop cuts them, so the goroutine always has a
// stop condition (mirrors cmd/session-manager-svc).
func runGRPC(ctx context.Context, srv *grpc.Server, port int, timeout time.Duration, logger *slog.Logger) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("billing listening", "addr", lis.Addr().String())
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
