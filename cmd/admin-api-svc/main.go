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
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/async"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
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
		config.SectionOTel, config.SectionPostgres, config.SectionHTTP, config.SectionSMPP, config.SectionRedis)
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
	//nolint:contextcheck // Detaching is the point: see DrainTracing's comment.
	defer observability.DrainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	// The bulk-import runner runs exact-route MNP imports in the background (step-103), bounded and
	// drained on shutdown. Its jobs use the pool, so its drain must complete before pool.Close: this
	// defer is registered after pool's, and defers run LIFO, so it runs first. The drain uses a
	// cancel-detached deadline so a SIGTERM does not cut it short.
	importRunner := async.New(4, logger)
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if derr := importRunner.Close(dctx); derr != nil {
			logger.Error("import runner drain incomplete", "err", derr)
		}
	}()

	// Redis carries the config-change announcement (step-105): the Admin API publishes a coarse event
	// after each mutation. A publish failure is best-effort (logged, not fatal), so — unlike Postgres —
	// Redis is not a hard readiness dependency here.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("open redis client: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	verifier, err := auth.NewStaticVerifier(cfg.HTTP.AdminTokens)
	if err != nil {
		return fmt.Errorf("build operator token verifier: %w", err)
	}

	// The SessionRegistry client fans a force-disconnect out to the smpp-server pods when a credential
	// is revoked/disabled or an account/customer suspended (step-032). Transport security terminates at
	// the mesh (insecure). NewClient is lazy: it opens no connection until the first Disconnect, so a
	// session-manager that is briefly down does not block startup — and a Disconnect during that window
	// fails best-effort without failing the control-plane mutation.
	registryConn, err := grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial session manager at %q: %w", cfg.SMPP.SessionManagerAddr, err)
	}
	defer func() { _ = registryConn.Close() }()

	router, _ := adminapi.New(adminapi.Deps{
		Customers:        postgres.NewCustomerRepo(pool),
		Accounts:         postgres.NewAccountRepo(pool),
		Credentials:      postgres.NewCredentialRepo(pool),
		Connectors:       postgres.NewConnectorRepo(pool),
		ConnectorControl: status.NewReader(rdb),
		Routes:           postgres.NewRouteRepo(pool),
		SenderIDs:        postgres.NewSenderIDRepo(pool),
		InboundNumbers:   postgres.NewInboundNumberRepo(pool),
		InboundKeywords:  postgres.NewInboundKeywordRepo(pool),
		UnroutedMO:       postgres.NewUnroutedMORepo(pool),
		Suppressions:     postgres.NewSuppressionRepo(pool),
		OptOutKeywords:   postgres.NewOptOutKeywordRepo(pool),
		AntispamRules:    postgres.NewAntispamRuleRepo(pool),
		ExactRoutes:      postgres.NewExactRouteRepo(pool),
		RoutingScripts:   postgres.NewRoutingScriptRepo(pool),
		Imports:          importRunner,
		Disconnector:     adminapi.NewGRPCDisconnector(registrypb.NewSessionRegistryClient(registryConn)),
		Billing:          postgres.NewBillingRepo(pool),
		BalanceCache:     redisBalanceCache{rdb: rdb},
		RatePlans:        postgres.NewRatePlanRepo(pool),
		BillingProviders: postgres.NewExternalBillingProviderRepo(pool),
		Verifier:         verifier,
		Logger:           logger,
	})

	// A single seam announces every control-plane mutation on config:changed; config-sync coalesces
	// those into a data-plane invalidation (step-105). A publish failure never fails the request.
	handler := adminapi.PublishConfigChanges(router, redisstore.NewPubSubPublisher(rdb), config.ChannelConfigChanged, logger)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.HTTP.Port),
		Handler:           handler,
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
	// service down predictably rather than leaving a half-dead pod (guide de codage §5). Neither has a
	// teardown-ordering constraint, so the unordered supervisor fits.
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("admin http server", func(c context.Context) error { return runHTTP(c, srv, cfg.ShutdownTimeout, logger) })
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

// redisBalanceCache adapts the Redis client to adminapi.BalanceCacheInvalidator: it deletes the balance-cache
// keys an admin money op just changed durably, so the next reserve rehydrates from Postgres (step-148).
type redisBalanceCache struct{ rdb *goredis.Client }

func (c redisBalanceCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, keys...).Err()
}
