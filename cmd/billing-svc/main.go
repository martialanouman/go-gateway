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

	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
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

	app, err := newBillingApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("grpc server", func(c context.Context) error {
		return runGRPC(c, app.grpc, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	g.Add("config refresh", func(c context.Context) error {
		return runConfigRefresh(c, app.repo, app.configProvider, logger)
	})
	g.Add("external reconcile", func(c context.Context) error {
		return runReconcile(c, app.reconciler, logger)
	})
	g.Add("reservation reaper", func(c context.Context) error {
		return runReap(c, app.reaper, cfg.Billing.ReaperInterval, logger)
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
