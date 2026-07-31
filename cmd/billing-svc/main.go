// Command billing-svc serves the Billing gRPC API: the opt-in MT/MO credit accounting core (plan §7, M9,
// gRPC :7001). It follows the canonical service lifecycle of cmd/session-manager-svc, backing the
// accountant with Redis (the operational balance cache) and Postgres (the durable ledger authority), and
// exposing a gRPC listener plus a periodic config-snapshot refresh as supervised components alongside the
// ops port. The plan assigns billing-svc port 7001; the deploy sets GRPC_PORT=7001 (the shared config
// default 7000 is session-manager-svc's, §1.4).
package main

import (
	"context"
	"encoding/base64"
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
	"github.com/martialanouman/go-gateway/internal/content"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
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

	// Billing needs Redis (the balance cache), Postgres (the durable ledger) and gRPC; no Kafka or HTTP.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionRedis, config.SectionPostgres, config.SectionGRPC)
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

	// Content keys (§6.14, M10, step-161): billing-svc is the sole holder of the KMS, so it hosts the
	// per-customer content-key lifecycle. This is independent of the billing opt-in — every customer that
	// stores content needs a key. The real KMS provider is an infra decision (§14); until then a local KMS
	// is built from CONTENT_KMS_MASTER_KEY, or an ephemeral dev key if unset (dev/test only — keys wrapped
	// under an ephemeral master do not survive a restart).
	kms, err := loadContentKMS(logger)
	if err != nil {
		return err
	}
	contentKeys := billing.NewContentKeyServer(kms, postgres.NewContentKeyRepo(pool))

	grpcServer := grpc.NewServer()
	pb.RegisterBillingServer(grpcServer, billing.NewServer(biller, repo))
	pb.RegisterContentKeysServer(grpcServer, contentKeys)

	// Both Redis and Postgres are vital: a balance can be neither served nor rehydrated without them, so a
	// pod that loses either must leave the load balancer (plan §1.5).
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, redisCheck, pgCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(externalFailOpenTotal, externalDiscrepancyTotal)

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

// extDiscrepancyMetric adapts the discrepancy counter to billing.DiscrepancyMetric.
type extDiscrepancyMetric struct{ c *prometheus.CounterVec }

func (m extDiscrepancyMetric) Discrepancy(providerID uuid.UUID) {
	m.c.WithLabelValues(providerID.String()).Inc()
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

// contentKMSMasterKeyEnv holds a base64-std-encoded 32-byte AES-256 master key (KEK) for the local content
// KMS. It is a dev/staging convenience until the real KMS provider (AWS/GCP/Vault) is wired (§14).
const contentKMSMasterKeyEnv = "CONTENT_KMS_MASTER_KEY"

// loadContentKMS builds the content KMS. With CONTENT_KMS_MASTER_KEY set (base64 of 32 bytes) it uses a
// stable local master key, so wrapped content keys survive restarts; without it, it falls back to an
// ephemeral in-memory dev key and warns — usable for tests and a single-process laptop, but any key wrapped
// under it becomes unreadable after a restart. The real provider replaces this behind the content.KMS
// interface with no call-site change.
func loadContentKMS(logger *slog.Logger) (content.KMS, error) {
	raw := os.Getenv(contentKMSMasterKeyEnv)
	if raw == "" {
		logger.Warn("no " + contentKMSMasterKeyEnv + " set: using an ephemeral dev content KMS master key (content keys will not survive a restart)")
		return content.NewDevKMS(), nil
	}
	master, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", contentKMSMasterKeyEnv, err)
	}
	kms, err := content.NewLocalKMS(master, "local/v1")
	if err != nil {
		return nil, fmt.Errorf("build content KMS from %s: %w", contentKMSMasterKeyEnv, err)
	}
	return kms, nil
}
