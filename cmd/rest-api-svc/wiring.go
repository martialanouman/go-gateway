package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/buildinfo"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// restAPIApp is rest-api-svc fully wired and not yet running: every connection is open and the API
// surface assembled, but no goroutine is started and no port is bound. Separating "assemble the graph"
// from "run it" is what makes the wiring testable — a test can build the whole service against test
// dependencies and assert it holds together, without a single request being served.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a value
// the caller (or a test) can inspect.
type restAPIApp struct {
	ops  *observability.OpsServer
	http *http.Server

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred Closes
	// in run() used to provide.
	closers []func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *restAPIApp) onClose(f func()) { a.closers = append(a.closers, f) }

// close releases every connection the app holds. It is safe to call on a partially built app: only what
// was actually opened is registered.
func (a *restAPIApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// newRestAPIApp builds the public API: the stores, the client-facing HTTP surface over them, and the ops
// port — in that order, which is the order in which a degraded dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds nothing.
func newRestAPIApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *restAPIApp, err error) {
	a := &restAPIApp{}
	defer func() {
		if err != nil {
			a.close()
		}
	}()

	st, err := openStores(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.onClose(st.close)

	//nolint:contextcheck // The boot context has no business inside a request handler: the API-key
	// middleware authenticates on the REQUEST context (ctx.Context()), which is the only correct one —
	// a lookup must be cancelled when its client hangs up, not when the process shuts down.
	a.http = newHTTPServer(cfg, st, logger)

	a.ops, err = newOpsServer(cfg, logger, st)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores are the external connections rest-api-svc opens at boot and holds for its whole lifetime:
// Postgres (API-key lookup), ClickHouse (CDR reads for get-message), Kafka (produce mt.inbound) and
// Redis (the Idempotency-Key window).
type stores struct {
	pg       *pgxpool.Pool
	ch       *clickhouse.Conn
	producer *kafka.Producer
	rdb      *goredis.Client
}

// openStores opens them in dependency-free order, releasing what it already holds if a later one fails.
//
// Redis is required at boot like the others (NewClient pings and fails fast) but stays out of readiness
// — see newOpsServer.
func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.pg, err = postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	s.ch, err = clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("connect clickhouse: %w", err)
	}

	s.producer, err = kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	s.rdb, err = redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return s, nil
}

// close releases the connections in reverse order of opening; a nil field is one that was never opened.
func (s *stores) close() {
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	if s.ch != nil {
		_ = s.ch.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
}

// newHTTPServer assembles the public API surface over the stores. It binds nothing: the listener opens
// in runHTTP.
func newHTTPServer(cfg config.Config, st *stores, logger *slog.Logger) *http.Server {
	// The second return is the huma API handle, which nothing here needs: the routes are already
	// registered on the mux by then.
	handler, _ := restapi.New(restapi.Deps{
		Principals:  postgres.NewAPIKeyRepo(st.pg),
		Ingestor:    ingest.NewIngestor(st.producer, logger),
		CDRReader:   clickhouse.NewCDRReader(st.ch),
		Accounts:    postgres.NewAccountRepo(st.pg),
		SenderIDs:   postgres.NewSenderIDRepo(st.pg),
		RateLimits:  postgres.NewRateLimitRepo(st.pg),
		Idempotency: idempotency.New(st.rdb),
		Tracer:      observability.Tracer(nil, serviceName),
		Logger:      logger,
		Version:     buildinfo.Version,
	})

	return &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.HTTP.Port),
		Handler:           handler,
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
	}
}

// newOpsServer builds the ops port.
//
// Vital dependencies (plan §1.5): Kafka and Postgres gate accepting a message (POST), ClickHouse gates
// reading its status (GET). All three remove the pod from the LB when unreachable.
//
// Redis backs the Idempotency-Key window on POST /messages. At runtime a Redis outage fails only
// idempotent submits — each returns a per-request 503 — so Redis is deliberately NOT wired in: a blip
// must not pull the pod out and take reads and non-idempotent submits down with it.
func newOpsServer(cfg config.Config, logger *slog.Logger, st *stores) (*observability.OpsServer, error) {
	ops, err := observability.NewOpsServer(cfg, logger,
		st.producer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		postgres.PingCheck("postgres", st.pg, cfg.Postgres.Timeout),
		st.ch.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	return ops, nil
}
