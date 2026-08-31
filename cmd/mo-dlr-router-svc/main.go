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
	"os"
	"os/signal"
	"syscall"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

const serviceName = "mo-dlr-router-svc"

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

	app, err := newReturnPathApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("dlr router", app.dlr.Run)
	g.Add("mo router", app.mo.Run)
	g.Add("mo delivery", app.moDelivery.Run)
	g.Add("dlr delivery", app.dlrDelivery.Run)
	g.Add("webhook retry", func(c context.Context) error { return app.retryConsumer.Run(c, app.retryRunner.Handle) })
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}
