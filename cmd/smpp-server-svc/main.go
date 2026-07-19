// Command smpp-server-svc is the SMPP bind front door (plan §7, M3, SMPP :2775). It accepts ESME
// connections, authenticates each bind against the control plane (PostgreSQL), enforces
// allowed_bind_types, and reserves a session token against the SessionRegistry gRPC service so
// max_sessions is honoured (invariant d). submit_sm is rejected until the MT pipeline lands (step-025).
//
// It follows the canonical service lifecycle of cmd/session-manager-svc: PostgreSQL for auth, a gRPC
// client to session-manager for the quota, and the SMPP listener supervised alongside the ops port.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
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

	// The SMPP server authenticates against PostgreSQL and calls session-manager over gRPC; it opens no
	// Redis, Kafka, ClickHouse or HTTP surface of its own, so it declares just the sections it uses.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionSMPP)
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

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	// The SessionRegistry client is a pod-to-pod internal call, so transport security is terminated at
	// the mesh, not here (insecure credentials). NewClient is lazy: it opens no connection until the
	// first bind, so a session-manager that is briefly down does not block startup — a bind during that
	// window simply fails with ESME_RSYSERR.
	registryConn, err := grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial session manager at %q: %w", cfg.SMPP.SessionManagerAddr, err)
	}
	defer func() { _ = registryConn.Close() }()

	listener := smppserver.New(
		postgres.NewBindRepo(pool),
		registrypb.NewSessionRegistryClient(registryConn),
		smppserver.Options{
			Addr:        fmt.Sprintf(":%d", cfg.SMPP.Port),
			PodID:       podID(cfg, logger),
			SystemID:    serviceName,
			IdleTimeout: cfg.SMPP.IdleTimeout,
		},
		logger,
	)

	// PostgreSQL is vital: without it no bind can be authenticated, so a pod that cannot reach it must
	// leave the load balancer (plan §1.5). The SessionRegistry dependency is surfaced per-bind
	// (ESME_RSYSERR) rather than gating readiness, since a lazy gRPC client reports no meaningful state
	// until traffic flows.
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, pgCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The ops server and the SMPP listener are supervised together: one failing brings the service down
	// predictably rather than leaving a half-dead pod (guide de codage §5). Neither has a
	// teardown-ordering constraint, so the unordered supervisor fits.
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("smpp listener", listener.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// podID resolves this pod's registry identity: the configured value, or the OS hostname as a fallback
// (what a Kubernetes pod's hostname already is). A last-resort empty id still lets binds succeed; it
// only makes a token harder to trace to its pod, which a warning flags.
func podID(cfg config.Config, logger *slog.Logger) string {
	if cfg.SMPP.PodID != "" {
		return cfg.SMPP.PodID
	}
	host, err := os.Hostname()
	if err != nil {
		logger.Warn("smpp: could not resolve hostname for pod id; using empty id", "err", err)
		return ""
	}
	return host
}
