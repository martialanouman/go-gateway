// Command rest-api-svc serves the public REST API (plan §1.4, business port 8080; ops 9090). It
// follows the canonical lifecycle of cmd/router-svc, adding a Postgres pool (API-key lookup), a
// Kafka producer (mt.inbound), a ClickHouse connection (CDR reads for get-message) and a client-facing HTTP listener — each a supervised component.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

const serviceName = "rest-api-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres (API-key lookup), Kafka (produce mt.inbound), ClickHouse (CDR read/write), Redis
	// (Idempotency-Key window), HTTP (business listener). The deployment sets HTTP_PORT=8080
	// (plan §1.4); the shared default is 8081.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse,
		config.SectionRedis, config.SectionHTTP, config.SectionBilling)
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

	app, err := newRestAPIApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The HTTP listener drains before the ops server (supervisor.Ordered drains in reverse registration
	// order). The accepted CDR row is no longer written here — it is projected durably off mt.inbound by
	// router-svc (step-101) — so there is no off-path writer to drain.
	var g supervisor.Ordered
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("rest http server", func(c context.Context) error { return runHTTP(c, app.http, cfg.ShutdownTimeout, logger) })
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// runHTTP serves the public API until ctx is cancelled, then drains within timeout.
func runHTTP(ctx context.Context, srv *http.Server, timeout time.Duration, logger *slog.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("rest api listening", "addr", srv.Addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("drain rest http server: %w", err)
		}
		return nil
	}
}
