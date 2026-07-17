// Command admin-api-svc serves the internal Admin API: the HTTP surface an operator uses to
// provision the control plane (plan §1.4, port 8081). It follows the canonical service lifecycle of
// cmd/router-svc, adding a Postgres pool and a business HTTP listener as supervised components.
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
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
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
	// the sections it uses, exactly as cmd/migrate does.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionHTTP)
	if err != nil {
		return err
	}

	// Operator tokens are specific to this service (not the pipeline binaries that share the HTTP
	// section), so the "at least one in production" policy is enforced here, at the point of use,
	// rather than in the shared config validator. Without it a production Admin API would boot,
	// pass readiness, and answer every request with 401 — a silent, fully non-functional service.
	if cfg.Environment.IsProduction() && len(cfg.HTTP.AdminTokens) == 0 {
		return fmt.Errorf("HTTP_ADMIN_TOKENS must be set in production: " +
			"the Admin API would otherwise reject every operator request")
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
	// nolint:contextcheck // Detaching is the point: see drainTracing's comment.
	defer drainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	verifier, err := auth.NewStaticVerifier(cfg.HTTP.AdminTokens)
	if err != nil {
		return fmt.Errorf("build operator token verifier: %w", err)
	}

	router, _ := adminapi.New(adminapi.Deps{
		Customers:   postgres.NewCustomerRepo(pool),
		Accounts:    postgres.NewAccountRepo(pool),
		Credentials: postgres.NewCredentialRepo(pool),
		Connectors:  postgres.NewConnectorRepo(pool),
		Routes:      postgres.NewRouteRepo(pool),
		SenderIDs:   postgres.NewSenderIDRepo(pool),
		Verifier:    verifier,
		Logger:      logger,
	})

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.HTTP.Port),
		Handler:           router,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
	}

	// Postgres is vital: without it the Admin API can neither read nor write the control plane, so a
	// pod that cannot reach it must leave the load balancer (plan §1.5). The ping probes the pool,
	// not a TCP address.
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)
	ops, err := observability.NewOpsServer(cfg, logger, pgCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The ops server and the business HTTP server are supervised together: one failing brings the
	// service down predictably rather than leaving a half-dead pod (guide de codage §5).
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ops.Run(runCtx, cfg.ShutdownTimeout); err != nil {
			select {
			case errCh <- fmt.Errorf("ops server: %w", err):
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runHTTP(runCtx, srv, cfg.ShutdownTimeout, logger); err != nil {
			select {
			case errCh <- fmt.Errorf("admin http server: %w", err):
			default:
			}
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}
	cancel()
	wg.Wait()

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

// drainTracing flushes buffered spans on the way out, on a context detached from the cancelled
// service context.
func drainTracing(shutdown observability.ShutdownFunc, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("flush traces on shutdown", "err", err)
	}
}
