// Command session-manager-svc serves the SessionRegistry gRPC API: the cross-pod registry of live
// SMPP ESME binds (plan §7, M3, gRPC :7000). It follows the canonical service lifecycle of
// cmd/admin-api-svc, backing the registry with Redis and exposing a gRPC listener as a supervised
// component alongside the ops port.
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

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "session-manager-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// The session manager talks only to Redis and serves gRPC; it has no Postgres, Kafka or HTTP
	// surface, so it declares just the sections it uses.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionRedis, config.SectionGRPC)
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

	app, err := newSessionManagerApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The ops server and the gRPC server are supervised together: one failing brings the service down
	// predictably rather than leaving a half-dead pod (guide de codage §5). Neither has a
	// teardown-ordering constraint, so the unordered supervisor fits.
	var g supervisor.Group
	g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("grpc server", func(c context.Context) error {
		return runGRPC(c, app.grpc, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// runGRPC serves the SessionRegistry until ctx is cancelled, then drains within timeout. It mirrors
// OpsServer.Run: same lifecycle, different port. GracefulStop lets in-flight RPCs finish; if they
// overrun the window a hard Stop cuts them, so the goroutine always has a stop condition.
func runGRPC(ctx context.Context, srv *grpc.Server, port int, timeout time.Duration, logger *slog.Logger) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("session registry listening", "addr", lis.Addr().String())
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
			// In-flight RPCs overran the drain window; cut them so the pod can exit before the kubelet
			// SIGKILLs it mid-teardown.
			srv.Stop()
			return nil
		}
	}
}
