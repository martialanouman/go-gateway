// Command config-sync bridges the control plane to the data plane (plan §11, M7). The Admin API
// announces a coarse "config changed" event on the config:changed Redis channel after every mutation;
// config-sync coalesces a burst of those into a single invalidation on the snapshot-invalidation
// channel, which every data-plane pod subscribes to and answers by rebuilding its routing snapshot.
// Collapsing the fan-out here (fleet level) spares the data plane a thundering herd when a bulk admin
// operation produces many mutations. It follows the canonical service lifecycle of cmd/session-manager
// -svc: Redis-only, an ops port with health/readiness, supervised components.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "config-sync"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// config-sync talks only to Redis (pub/sub) and serves the ops port; it declares just those.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionRedis)
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

	// The relay: subscribe to config:changed, coalesce, republish one invalidation on breaker:events.
	pub := redisstore.NewPubSubPublisher(rdb)
	relay := config.NewWatcher(
		func(ctx context.Context) (config.Stream, error) {
			return redisstore.Subscribe(ctx, rdb, config.ChannelConfigChanged), nil
		},
		func(ctx context.Context) error {
			return pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`))
		},
		config.WithLogger(logger),
	)

	// Redis is vital: without it config-sync can neither hear a change nor announce one, so a pod that
	// cannot reach it must leave the load balancer (plan §1.5). The probe pings the client.
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, redisCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("invalidation relay", relay.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}
