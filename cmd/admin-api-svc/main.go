// Command admin-api-svc serves the internal Admin API: the HTTP surface an operator uses to
// provision the control plane (plan §1.4, port 8081). It follows the canonical service lifecycle of
// cmd/router-svc, adding a Postgres pool and a business HTTP listener as supervised components.
package main

import (
	"context"
	"encoding/json"
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
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/realtime"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "admin-api-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// The Admin API talks to Postgres and serves HTTP; it has no Kafka client, so it declares only
	// the sections it uses, exactly as cmd/migrate does. It also declares SectionSMPP — not to serve
	// SMPP, but for the one field SESSION_MANAGER_ADDR: a control-plane mutation (revoke, suspend) must
	// force-disconnect the affected live binds via session-manager's SessionRegistry (step-032), and the
	// address of that service is the same env var every session-manager client already uses.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionHTTP, config.SectionSMPP, config.SectionRedis, config.SectionClickHouse, config.SectionContentKey, config.SectionKafka)
	if err != nil {
		return err
	}
	if err := validateAdminConfig(cfg); err != nil {
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

	app, err := newAdminApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The ops server and the business HTTP server are supervised together: one failing brings the
	// service down predictably rather than leaving a half-dead pod (guide de codage §5). Neither has a
	// teardown-ordering constraint, so the unordered supervisor fits.
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("admin http server", func(c context.Context) error { return runHTTP(c, app.http, cfg.ShutdownTimeout, logger) })
	g.Add("cdr retention", func(c context.Context) error {
		return app.retainer.Run(c, cfg.ClickHouse.RetentionInterval)
	})
	// Self-restarting, never fatal: the dashboard feed is best-effort, and a Kafka hiccup must not tear down
	// the control plane — customers, credentials, billing, GDPR — along with it.
	g.Add("metrics stream", func(c context.Context) error {
		return runResilient(c, "metrics stream", func(rc context.Context) error {
			return app.stream.Run(rc, func(_ context.Context, rec kafka.Record) error {
				publishRecord(app.hub, rec.Value)
				return nil
			})
		}, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// runHTTP serves the Admin API until ctx is cancelled, then drains within timeout. It mirrors
// OpsServer.Run: same lifecycle, different port, so the business API and the ops port can fail
// independently.
func runHTTP(ctx context.Context, srv *http.Server, timeout time.Duration, logger *slog.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("admin api listening", "addr", srv.Addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// Drain on a context detached from the cancelled one, so in-flight requests get the full
		// window rather than being aborted immediately.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("drain admin http server: %w", err)
		}
		return nil
	}
}

// streamBackoff paces a restart of the metrics feed after a Kafka fault.
const streamBackoff = 5 * time.Second

// runResilient restarts a non-vital component until ctx is cancelled, so its faults never reach the
// supervisor and take the whole service down.
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
		case <-time.After(streamBackoff):
		}
	}
}

// publishRecord routes a metrics.stream record to its feed, dropping anything this build does not understand.
// The hub is a blind byte pipe, so this is the one place a malformed or future-versioned frame is kept off
// every dashboard.
func publishRecord(hub *realtime.Hub, value []byte) {
	var head struct {
		V    int               `json:"v"`
		Feed metricstream.Feed `json:"feed"`
	}
	if err := json.Unmarshal(value, &head); err != nil || head.V != metricstream.SchemaVersion {
		return
	}
	switch head.Feed {
	case metricstream.FeedSessions:
		hub.Publish(realtime.StreamSessions, value)
	case metricstream.FeedBillingAlerts:
		hub.Publish(realtime.StreamBillingAlerts, value)
	case metricstream.FeedMetrics, "":
		// Empty means a snapshot produced before step-184 added the discriminator.
		hub.Publish(realtime.StreamMetrics, value)
	}
}
