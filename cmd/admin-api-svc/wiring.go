package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	contentkeypb "github.com/martialanouman/go-gateway/internal/contentkeys/pb"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/async"
	"github.com/martialanouman/go-gateway/internal/realtime"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// adminApp is admin-api-svc fully wired and not yet running: every connection is open, every
// component built, but no port is bound and no background runner started. Separating "assemble the
// graph" from "run it" is what makes the wiring testable — a test can build the whole service
// against test dependencies and assert it holds together, without serving a request.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a
// value the caller (or a test) can inspect.
type adminApp struct {
	ops      *observability.OpsServer
	http     *http.Server
	retainer *clickhouse.Retainer
	hub      *realtime.Hub
	stream   *kafka.Consumer

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred
	// Closes in run() used to provide. They are named because that order is the property worth
	// guarding, and an anonymous stack cannot be asserted against.
	closers []closer
}

// closer is a release step and the name it answers to. The name carries no behaviour: it exists so
// that the release ORDER — a property of newAdminApp, and one a wrong edit breaks silently — can be
// asserted on the graph the service actually builds.
type closer struct {
	name string
	fn   func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *adminApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name: name, fn: f})
}

// close releases every connection the app holds, and drains the background runners. It is safe to
// call on a partially built app: only what was actually opened is registered.
func (a *adminApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}

// newAdminApp builds the whole graph: stores, the CDR retention pass, the background runners, the
// control-plane clients, the realtime feed, the HTTP surface and the ops server — in that order,
// which is the order in which a degraded dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds
// nothing.
func newAdminApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *adminApp, err error) {
	a := &adminApp{}
	defer func() {
		if err != nil {
			a.close()
		}
	}()

	st, err := openStores(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.onClose("stores", st.close)

	retention, err := newRetainer(cfg, st.ch, logger)
	if err != nil {
		return nil, err
	}
	a.retainer = retention.retainer

	runners := newRunners(ctx, cfg, logger)
	a.onClose("runners", runners.close)

	// Redis carries the config-change announcement (step-105): the Admin API publishes a coarse event
	// after each mutation. A publish failure is best-effort (logged, not fatal), so — unlike Postgres —
	// Redis is not a hard readiness dependency here.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("open redis client: %w", err)
	}
	a.onClose("redis", func() { _ = rdb.Close() })

	verifier, err := auth.NewStaticVerifier(cfg.HTTP.AdminTokens)
	if err != nil {
		return nil, fmt.Errorf("build operator token verifier: %w", err)
	}

	clients, err := newControlPlaneClients(cfg)
	if err != nil {
		return nil, err
	}
	a.onClose("clients", clients.close)

	feed, err := newRealtimeFeed(cfg)
	if err != nil {
		return nil, err
	}
	a.onClose("feed", feed.close)
	a.hub = feed.hub
	a.stream = feed.reader

	//nolint:contextcheck // The boot context has no business inside a request handler: the config-change
	// middleware deliberately detaches the REQUEST context (see PublishConfigChanges) so the announcement
	// outlives the response.
	a.http = newHTTPServer(cfg, logger, st, rdb, runners, clients, feed, verifier)

	// Postgres is vital: without it the Admin API can neither read nor write the control plane, so a
	// pod that cannot reach it must leave the load balancer (plan §1.5). The ping probes the pool,
	// not a TCP address.
	pgCheck := postgres.PingCheck("postgres", st.pg, cfg.Postgres.Timeout)
	a.ops, err = observability.NewOpsServer(cfg, logger, pgCheck)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	a.ops.Registry().MustRegister(retention.outcomes)
	return a, nil
}

// stores are the two data stores the Admin API holds for its whole lifetime.
type stores struct {
	pg *pgxpool.Pool
	ch *clickhouse.Conn
}

func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	// ClickHouse backs the audited content read (get-message-content, step-163): the operator reads a decrypted
	// body, which requires the CDR row (ciphertext + key id). It is not a readiness dependency — content read
	// is a rare admin operation, not the control-plane happy path.
	s.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}
	return s, nil
}

// close releases the connections in reverse order of opening; a nil field is one that was never
// opened.
func (s *stores) close() {
	if s.ch != nil {
		_ = s.ch.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
}

// retention is the CDR retention pass (§6.14, step-165): expired daily partitions are archived (when
// a destination is configured) and then DROPPED — a metadata operation, never a delete-by-predicate,
// which would rewrite parts continuously at the 8000 msg/s target. It lives here because this service
// already holds the ClickHouse connection and is the control-plane surface. Running it on several
// replicas is tolerable (a re-drop is a no-op, and archives never share a destination object) but
// archives a partition twice, so a deploy that scales this service out should schedule retention on a
// single runner instead.
type retention struct {
	retainer *clickhouse.Retainer

	// outcomes carries a bounded label (four fixed outcomes) — a pass that has been failing for weeks
	// must be an alert, not a log line, because its consequence is a disk filling up.
	outcomes *prometheus.CounterVec
}

func newRetainer(cfg config.Config, ch *clickhouse.Conn, logger *slog.Logger) (*retention, error) {
	outcomes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cdr_retention_partitions_total",
		Help: "CDR partitions processed by the retention pass, by outcome.",
	}, []string{"outcome"})
	opts := []clickhouse.RetainerOption{
		clickhouse.WithRetainerLogger(logger),
		clickhouse.WithRetentionMetric(retentionMetric{c: outcomes}),
	}
	if prefix := cfg.ClickHouse.ArchivePrefix; prefix != "" {
		// The prefix is interpolated into the archive statement, so a malformed one is a boot failure, never
		// a surprise at the first purge.
		if !clickhouse.ValidArchivePrefix(prefix) {
			return nil, fmt.Errorf("CLICKHOUSE_ARCHIVE_PREFIX %q is not a plain name ([A-Za-z0-9._/-], max 128)", prefix)
		}
		opts = append(opts, clickhouse.WithArchiver(
			clickhouse.NewPartitionArchiver(ch, clickhouse.FileDestination(prefix))))
	}
	return &retention{retainer: clickhouse.NewRetainer(ch, cfg.ClickHouse.CDRRetention, opts...), outcomes: outcomes}, nil
}

// runners are the bounded background job pools. The bulk-import runner runs exact-route MNP imports
// (step-103); RGPD erasures get their OWN runner, because a legally-mandated erasure must never be
// refused because bulk MNP imports filled the shared pool (and a long erasure must not starve them
// either).
type runners struct {
	imports *async.Runner
	gdpr    *async.Runner

	// close drains both. Their jobs use the Postgres pool, so the drain must complete BEFORE the pool
	// is closed — which holds because this closer is registered after the stores', and closers run in
	// reverse. TestNewAdminAppReleasesInDependencyOrder is what enforces that, not this sentence.
	// The drain uses a cancel-detached deadline so a SIGTERM does not cut it short.
	close func()
}

func newRunners(ctx context.Context, cfg config.Config, logger *slog.Logger) *runners {
	r := &runners{imports: async.New(4, logger), gdpr: async.New(2, logger)}
	r.close = func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if derr := r.imports.Close(dctx); derr != nil {
			logger.Error("import runner drain incomplete", "err", derr)
		}
		if derr := r.gdpr.Close(dctx); derr != nil {
			logger.Error("gdpr runner drain incomplete", "err", derr)
		}
	}
	return r
}

// controlPlaneClients are the gRPC clients a mutation fans out to. Both are lazy: NewClient opens no
// connection until the first call, so a peer that is briefly down does not block startup.
type controlPlaneClients struct {
	// registry fans a force-disconnect out to the smpp-server pods when a credential is
	// revoked/disabled or an account/customer suspended (step-032). Transport security terminates at
	// the mesh (insecure). A Disconnect during a session-manager outage fails best-effort, without
	// failing the control-plane mutation.
	registry *grpc.ClientConn

	// contentKey delegates content-key rotation, the guarded read and the crypto-shred to
	// content-key-svc, the sole holder of the KMS (step-167).
	contentKey *grpc.ClientConn
}

func (c *controlPlaneClients) close() {
	if c.contentKey != nil {
		_ = c.contentKey.Close()
	}
	if c.registry != nil {
		_ = c.registry.Close()
	}
}

func newControlPlaneClients(cfg config.Config) (_ *controlPlaneClients, err error) {
	c := &controlPlaneClients{}
	defer func() {
		if err != nil {
			c.close()
		}
	}()

	c.registry, err = grpc.NewClient(cfg.SMPP.SessionManagerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial session manager at %q: %w", cfg.SMPP.SessionManagerAddr, err)
	}

	c.contentKey, err = grpc.NewClient(cfg.ContentKey.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial content key service at %q: %w", cfg.ContentKey.Addr, err)
	}
	return c, nil
}

// realtimeFeed is the dashboard fan-out (step-183). Groupless and from end-of-log: every replica must
// see every record to serve its own clients, and a live feed has nothing to resume — committed offsets
// would replay the retained backlog into dashboards that only want what is happening now.
type realtimeFeed struct {
	hub    *realtime.Hub
	reader *kafka.Consumer

	// quit ends the hijacked WebSocket connections. They leave net/http's active set, so Shutdown
	// neither waits for nor closes them; closing this channel is the only thing that does.
	quit chan struct{}
}

func (f *realtimeFeed) close() {
	if f.reader != nil {
		f.reader.Close()
	}
}

func newRealtimeFeed(cfg config.Config) (*realtimeFeed, error) {
	reader, err := kafka.NewTailReader(cfg.Kafka, kafka.TopicMetricsStream)
	if err != nil {
		return nil, fmt.Errorf("metrics stream reader: %w", err)
	}
	return &realtimeFeed{hub: realtime.NewHub(realtime.Config{}), reader: reader, quit: make(chan struct{})}, nil
}

// newHTTPServer assembles the Admin API surface over the control-plane repositories. It binds
// nothing: the listener opens in runHTTP.
func newHTTPServer(
	cfg config.Config,
	logger *slog.Logger,
	st *stores,
	rdb *goredis.Client,
	runners *runners,
	clients *controlPlaneClients,
	feed *realtimeFeed,
	verifier *auth.StaticVerifier,
) *http.Server {
	router, _ := adminapi.New(adminapi.Deps{
		StreamHub:        feed.hub,
		Trace:            clickhouse.NewCDRReader(st.ch),
		Quit:             feed.quit,
		Customers:        postgres.NewCustomerRepo(st.pg),
		Accounts:         postgres.NewAccountRepo(st.pg),
		Credentials:      postgres.NewCredentialRepo(st.pg),
		Connectors:       postgres.NewConnectorRepo(st.pg),
		ConnectorControl: status.NewReader(rdb),
		Routes:           postgres.NewRouteRepo(st.pg),
		SenderIDs:        postgres.NewSenderIDRepo(st.pg),
		InboundNumbers:   postgres.NewInboundNumberRepo(st.pg),
		InboundKeywords:  postgres.NewInboundKeywordRepo(st.pg),
		UnroutedMO:       postgres.NewUnroutedMORepo(st.pg),
		Suppressions:     postgres.NewSuppressionRepo(st.pg),
		OptOutKeywords:   postgres.NewOptOutKeywordRepo(st.pg),
		AntispamRules:    postgres.NewAntispamRuleRepo(st.pg),
		ExactRoutes:      postgres.NewExactRouteRepo(st.pg),
		ExactRouteCache:  exact.NewInvalidator(rdb),
		RoutingScripts:   postgres.NewRoutingScriptRepo(st.pg),
		Imports:          runners.imports,
		Disconnector:     adminapi.NewGRPCDisconnector(registrypb.NewSessionRegistryClient(clients.registry)),
		Billing:          postgres.NewBillingRepo(st.pg),
		BalanceCache:     redisBalanceCache{rdb: rdb},
		RatePlans:        postgres.NewRatePlanRepo(st.pg),
		BillingProviders: postgres.NewExternalBillingProviderRepo(st.pg),
		ContentKeys:      adminapi.NewGRPCContentKeyRotator(contentkeypb.NewContentKeysClient(clients.contentKey)),
		ContentKeyReader: adminapi.NewGRPCContentKeyReader(contentkeypb.NewContentKeysClient(clients.contentKey)),
		ContentKeyEraser: adminapi.NewGRPCContentKeyEraser(contentkeypb.NewContentKeysClient(clients.contentKey)),
		Messages:         clickhouse.NewCDRReader(st.ch),
		MessageSearch:    clickhouse.NewCDRReader(st.ch),
		ExportJobs:       postgres.NewMessageExportJobRepo(st.pg),
		ExportSink:       exportSink(cfg),
		ContentAudit:     postgres.NewContentAccessAuditRepo(st.pg),
		GDPRJobs:         postgres.NewGDPREraseJobRepo(st.pg),
		GDPRRunner:       runners.gdpr,
		CDREraser:        clickhouse.NewCDREraser(st.ch),
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
	// Hijacked connections leave net/http's active set, so Shutdown neither waits for nor closes a
	// WebSocket. This hook is the only thing that ends them.
	srv.RegisterOnShutdown(func() { close(feed.quit) })
	return srv
}

// exportSink is the destination asynchronous exports write to, or nil when the deployment configures
// none — create-message-export then answers 503 instead of queueing a job that could never produce a
// file. The local directory is the file tier; an object tier plugs into the same seam.
func exportSink(cfg config.Config) adminapi.ExportSink {
	if cfg.HTTP.ExportDir == "" {
		return nil
	}
	return adminapi.NewFileExportSink(cfg.HTTP.ExportDir)
}

// validateAdminConfig enforces the policies specific to this service, at the point of use rather than
// in the shared config validator.
//
// Operator tokens are specific to this service (not the pipeline binaries that share the HTTP
// section). Without this check a production Admin API would boot, pass readiness, and answer every
// request with 401 — a silent, fully non-functional service.
func validateAdminConfig(cfg config.Config) error {
	if cfg.Environment.IsProduction() && len(cfg.HTTP.AdminTokens) == 0 {
		return fmt.Errorf("HTTP_ADMIN_TOKENS must be set in production: " +
			"the Admin API would otherwise reject every operator request")
	}
	return nil
}

// retentionMetric adapts a Prometheus counter to clickhouse.RetentionMetric.
type retentionMetric struct{ c *prometheus.CounterVec }

func (m retentionMetric) Observe(outcome string) { m.c.WithLabelValues(outcome).Inc() }

// redisBalanceCache adapts the Redis client to adminapi.BalanceCacheInvalidator: it deletes the balance-cache
// keys an admin money op just changed durably, so the next reserve rehydrates from Postgres (step-148).
type redisBalanceCache struct{ rdb *goredis.Client }

func (c redisBalanceCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, keys...).Err()
}
