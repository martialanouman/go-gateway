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

	app, err := newConfigSyncApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("invalidation relay", app.relay.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}
