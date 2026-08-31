// Command smpp-server-svc is the SMPP bind front door (plan §7, M3, SMPP :2775). It accepts ESME
// connections, authenticates each bind against the control plane (PostgreSQL), enforces
// allowed_bind_types, and reserves a session token against the SessionRegistry gRPC service so
// max_sessions is honoured (invariant d). A bound ESME's submit_sm travels the same MT pipeline as a
// REST submit: it is produced durably to mt.inbound (the boundary that earns the submit_sm_resp) and
// its accepted CDR row is written off the connection's path (step-025).
//
// It follows the canonical service lifecycle of cmd/rest-api-svc: PostgreSQL for auth, a gRPC client
// to session-manager for the quota, Kafka for the durable produce, ClickHouse for the accepted CDR
// row, and the SMPP listener supervised alongside the ops port.
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
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "smpp-server-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// PostgreSQL (bind auth), Kafka (produce mt.inbound), ClickHouse (CDR) and Redis. Two gRPC surfaces
	// meet here, which is how SectionGRPC came to be missing: the CLIENT dial to session-manager for
	// max_sessions travels in SectionSMPP (SMPP_SESSION_MANAGER_ADDR), while this pod's OWN Deliver
	// server listens on cfg.GRPC.Port (step-048) — declaring SectionGRPC is what gets that port checked
	// against the ops port at boot. No HTTP business surface of its own: the SMPP listener is it.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse,
		config.SectionRedis, config.SectionSMPP, config.SectionGRPC)
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

	app, err := newSMPPApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The SMPP listener drains before the ops server (supervisor.Ordered drains in reverse registration
	// order). The accepted CDR row is no longer written here — it is projected durably off mt.inbound by
	// router-svc (step-101) — so there is no off-path writer to drain.
	var g supervisor.Ordered
	g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("smpp listener", app.listener.Run)
	// The disconnect subscriber force-closes this pod's sessions when a revocation or suspension is
	// fanned out by session-manager (step-032). It is fail-open (a Redis blip degrades disconnects, not
	// binds), so it is registered after the listener — it drains first, before connections are cut.
	g.Add("disconnect subscriber", func(c context.Context) error {
		return smppserver.RunDisconnectSubscriber(c, redisstore.Subscribe(c, app.rdb, disconnect.Channel), app.listener, logger)
	})
	// The Deliver gRPC server is registered last so it drains first (reverse order): it stops accepting
	// deliveries before the listener closes the binds they target, so no Deliver races a draining socket.
	g.Add("deliver grpc server", func(c context.Context) error {
		return runGRPC(c, app.grpc, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// runGRPC serves the pod-local Deliver surface until ctx is cancelled, then drains within timeout. It
// mirrors session-manager's runGRPC: GracefulStop lets an in-flight Deliver finish, then a hard Stop
// caps the drain so the goroutine always has a stop condition.
func runGRPC(ctx context.Context, srv *grpc.Server, port int, timeout time.Duration, logger *slog.Logger) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("deliver grpc listening", "addr", lis.Addr().String())
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
