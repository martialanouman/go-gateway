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
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "smpp-server-svc"

// Accepted-row worker pool sizing, mirroring rest-api-svc: the pool absorbs bursts off the
// connection's path and is ample for M3.
const (
	acceptedWorkers   = 4
	acceptedQueueSize = 1024
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// PostgreSQL (bind auth), Kafka (produce mt.inbound), ClickHouse (accepted CDR row) and gRPC to
	// session-manager (max_sessions). No HTTP business surface of its own — the SMPP listener is it.
	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse, config.SectionSMPP)
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

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	accepted := ingest.NewAcceptedWriter(clickhouse.NewCDRWriter(chConn), acceptedWorkers, acceptedQueueSize, logger)
	ingestor := ingest.NewIngestor(producer, accepted, logger)

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
		ingestor,
		smppserver.Options{
			Addr:        fmt.Sprintf(":%d", cfg.SMPP.Port),
			PodID:       podID(cfg, logger),
			SystemID:    serviceName,
			IdleTimeout: cfg.SMPP.IdleTimeout,
			Tracer:      observability.Tracer(nil, serviceName),
		},
		logger,
	)

	// Vital dependencies (plan §1.5): PostgreSQL gates authenticating a bind, and Kafka gates durably
	// accepting a submit_sm — both remove the pod from the LB when unreachable. ClickHouse is
	// deliberately NOT vital here: unlike rest-api-svc it backs no GET surface, only the best-effort
	// accepted CDR row (Enqueue drops on saturation, the connector's enroute row supersedes it), so a
	// ClickHouse outage must not refuse binds and submits while the durable path (Kafka) is healthy.
	// The SessionRegistry dependency is surfaced per-bind (ESME_RSYSERR) rather than gating readiness,
	// since a lazy gRPC client reports no meaningful state until traffic flows.
	ops, err := observability.NewOpsServer(cfg, logger,
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
		producer.ReadyCheck("kafka", cfg.Kafka.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ordered shutdown matters here (as in rest-api-svc): the SMPP listener must stop and fully drain
	// its connections BEFORE the accepted-row writer, or a submit_sm that earns its submit_sm_resp
	// during the drain would Enqueue after the writer's workers have exited — silently dropping the
	// accepted CDR row and re-opening the get-message 404 window (§1.10). Each component gets its own
	// cancel, detached from the signal context, and shutdown drives them in order: listener → writer →
	// ops. (Cancelling one shared context would stop all three at once.)
	listenerCtx, cancelListener := context.WithCancel(context.WithoutCancel(ctx))
	writerCtx, cancelWriter := context.WithCancel(context.WithoutCancel(ctx))
	opsCtx, cancelOps := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelOps()
	defer cancelWriter()
	defer cancelListener()

	errCh := make(chan error, 3)
	var listenerWg, writerWg, opsWg sync.WaitGroup

	supervise := func(wg *sync.WaitGroup, name string, c context.Context, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(c); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}

	supervise(&opsWg, "ops server", opsCtx, func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	supervise(&writerWg, "accepted writer", writerCtx, accepted.Run)
	supervise(&listenerWg, "smpp listener", listenerCtx, listener.Run)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}

	// Drain in order: stop the listener and let it finish in-flight connections (their Enqueue lands
	// in the writer's buffer), then stop the writer so it drains that buffer, then the ops server.
	cancelListener()
	listenerWg.Wait()
	cancelWriter()
	writerWg.Wait()
	cancelOps()
	opsWg.Wait()

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
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
