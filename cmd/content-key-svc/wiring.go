package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/contentkeys"
	"github.com/martialanouman/go-gateway/internal/contentkeys/pb"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

// contentKeyApp is content-key-svc fully wired and not yet running: the pool is open and the gRPC
// service registered, but no goroutine is started and no port is bound. Separating "assemble the graph"
// from "run it" is what makes the wiring testable — a test can build the whole service against test
// dependencies and assert it holds together, without a single key being wrapped.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a value
// the caller (or a test) can inspect.
type contentKeyApp struct {
	ops  *observability.OpsServer
	grpc *grpc.Server

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred Closes
	// in run() used to provide. They are named because that order is the property worth guarding,
	// and an anonymous stack cannot be asserted against.
	closers []closer
}

// closer is a release step and the name it answers to. The name carries no behaviour: it exists so
// that the release ORDER — a property of newContentKeyApp, and one a wrong edit breaks silently — can be
// asserted on the graph the service actually builds.
type closer struct {
	name string
	fn   func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *contentKeyApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name: name, fn: f})
}

// close releases every connection the app holds. It is safe to call on a partially built app: only what
// was actually opened is registered.
func (a *contentKeyApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}

// newContentKeyApp builds the key custodian: the Postgres pool, the KMS holding the master key, the gRPC
// service and the ops port — in that order, which is the order in which a degraded dependency must
// surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds nothing.
func newContentKeyApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *contentKeyApp, err error) {
	a := &contentKeyApp{}
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

	kms, err := loadContentKMS(cfg.Environment, logger)
	if err != nil {
		return nil, err
	}

	a.grpc = grpc.NewServer()
	pb.RegisterContentKeysServer(a.grpc,
		contentkeys.NewContentKeyServer(kms, postgres.NewContentKeyRepo(st.pg)))

	a.ops, err = newOpsServer(cfg, logger, st.pg)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores is the single external connection content-key-svc holds: the content_keys table. Every
// dependency it does not have is one that cannot be used to reach the KEK (ADR-0011).
type stores struct {
	pg *pgxpool.Pool
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
	return s, nil
}

// close releases the connection; a nil field is one that was never opened.
func (s *stores) close() {
	if s.pg != nil {
		s.pg.Close()
	}
}

// newOpsServer builds the ops port.
//
// Postgres is vital: without it no key can be read, created or shredded, so a pod that cannot reach it
// must leave the load balancer (plan §1.5).
func newOpsServer(cfg config.Config, logger *slog.Logger, pool *pgxpool.Pool) (*observability.OpsServer, error) {
	pgCheck := postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout)

	ops, err := observability.NewOpsServer(cfg, logger, pgCheck)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	return ops, nil
}
