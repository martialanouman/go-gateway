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
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/config"
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

	grpcServer := grpc.NewServer()
	pb.RegisterBillingServer(grpcServer, billing.NewServer(acc, repo))

	// Both Redis and Postgres are vital: a balance can be neither served nor rehydrated without them, so a
	// pod that loses either must leave the load balancer (plan §1.5).
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, redisCheck, pgCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("grpc server", func(c context.Context) error {
		return runGRPC(c, grpcServer, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	g.Add("config refresh", func(c context.Context) error {
		return runConfigRefresh(c, repo, configProvider, logger)
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
