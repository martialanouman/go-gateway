package main

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/pb"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// sessionManagerApp is session-manager-svc fully wired and not yet running: the Redis client is open and
// the registry registered, but no goroutine is started and no port is bound. Separating "assemble the
// graph" from "run it" is what makes the wiring testable — a test can build the whole service against
// test dependencies and assert it holds together, without a single bind happening.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a value
// the caller (or a test) can inspect.
type sessionManagerApp struct {
	ops  *observability.OpsServer
	grpc *grpc.Server

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred Closes
	// in run() used to provide. They are named because that order is the property worth guarding,
	// and an anonymous stack cannot be asserted against.
	closers []closer
}

// closer is a release step and the name it answers to. The name carries no behaviour: it exists so
// that the release ORDER — a property of newSessionManagerApp, and one a wrong edit breaks
// silently — can be asserted on the graph the service actually builds.
type closer struct {
	name string
	fn   func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *sessionManagerApp) onClose(name string, f func()) {
	a.closers = append(a.closers, closer{name: name, fn: f})
}

// close releases every connection the app holds. It is safe to call on a partially built app: only what
// was actually opened is registered.
func (a *sessionManagerApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i].fn()
	}
}

// newSessionManagerApp builds the registry: the Redis client, the gRPC service over it and the ops port
// — in that order, which is the order in which a degraded dependency must surface.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds nothing.
func newSessionManagerApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *sessionManagerApp, err error) {
	a := &sessionManagerApp{}
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

	a.grpc = grpc.NewServer()
	pb.RegisterSessionRegistryServer(a.grpc,
		session.NewServer(session.NewRegistry(st.rdb), redisstore.NewPubSubPublisher(st.rdb)))

	a.ops, err = newOpsServer(cfg, logger, st.rdb)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores is the single external connection session-manager-svc holds: the session registry state. No
// Postgres, Kafka or HTTP surface.
type stores struct {
	rdb *goredis.Client
}

func openStores(ctx context.Context, cfg config.Config) (_ *stores, err error) {
	s := &stores{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	s.rdb, err = redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("open redis client: %w", err)
	}
	return s, nil
}

// close releases the connection; a nil field is one that was never opened.
func (s *stores) close() {
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
}

// newOpsServer builds the ops port.
//
// Redis is vital: without it the registry can neither enforce max_sessions nor answer a lookup, so a pod
// that cannot reach it must leave the load balancer (plan §1.5). The probe pings the client, not a TCP
// address.
func newOpsServer(cfg config.Config, logger *slog.Logger, rdb *goredis.Client) (*observability.OpsServer, error) {
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)

	ops, err := observability.NewOpsServer(cfg, logger, redisCheck)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	return ops, nil
}
