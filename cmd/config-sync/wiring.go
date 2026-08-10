package main

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// configSyncApp is config-sync fully wired and not yet running: the Redis client is open and the relay
// built, but no goroutine is started and no port is bound — not even a subscription is live, since the
// watcher subscribes inside its own Run.
//
// Every step that can fail returns an error rather than ending the process, so a boot failure is a value
// the caller (or a test) can inspect. The graph is small; the point of extracting it is that step-205
// (Redis TLS) and step-207 (probes) will add to it, and they must have something a test can hold.
type configSyncApp struct {
	ops   *observability.OpsServer
	relay *config.Watcher

	// closers release what was opened, in reverse order of opening — the exact LIFO the deferred Closes
	// in run() used to provide.
	closers []func()
}

// onClose registers a release step, to be run in reverse order by close.
func (a *configSyncApp) onClose(f func()) { a.closers = append(a.closers, f) }

// close releases every connection the app holds. It is safe to call on a partially built app: only what
// was actually opened is registered.
func (a *configSyncApp) close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
}

// newConfigSyncApp builds the relay and the ops port.
//
// On failure it releases whatever it had already opened, so a caller that gets an error holds nothing.
func newConfigSyncApp(ctx context.Context, cfg config.Config, logger *slog.Logger) (_ *configSyncApp, err error) {
	a := &configSyncApp{}
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

	a.relay = newRelay(st.rdb, logger)

	a.ops, err = newOpsServer(cfg, logger, st.rdb)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// stores is the single external connection config-sync holds: the pub/sub bus it listens on and
// announces to.
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

// newRelay builds the watcher: subscribe to config:changed, coalesce a burst, republish one invalidation
// on the snapshot-invalidation channel. It cannot fail — the subscription itself opens in Run.
func newRelay(rdb *goredis.Client, logger *slog.Logger) *config.Watcher {
	pub := redisstore.NewPubSubPublisher(rdb)
	return config.NewWatcher(
		func(ctx context.Context) (config.Stream, error) {
			return redisstore.Subscribe(ctx, rdb, config.ChannelConfigChanged), nil
		},
		func(ctx context.Context) error {
			return pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`))
		},
		config.WithLogger(logger),
	)
}

// newOpsServer builds the ops port.
//
// Redis is vital: without it config-sync can neither hear a change nor announce one, so a pod that
// cannot reach it must leave the load balancer (plan §1.5). The probe pings the client.
func newOpsServer(cfg config.Config, logger *slog.Logger, rdb *goredis.Client) (*observability.OpsServer, error) {
	redisCheck := redisstore.PingCheck("redis", rdb, cfg.Redis.Timeout)

	ops, err := observability.NewOpsServer(cfg, logger, redisCheck)
	if err != nil {
		return nil, fmt.Errorf("init ops server: %w", err)
	}
	return ops, nil
}
