package config_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestWatcherRelayOverRedis proves the real pub/sub path end-to-end: a Watcher subscribed to one
// channel republishes an invalidation on another (the config-sync relay shape). A publish on
// config:changed yields exactly one message on breaker:events.
func TestWatcherRelayOverRedis(t *testing.T) {
	rdb := redistest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub := redisstore.NewPubSubPublisher(rdb)

	// A downstream subscriber on the invalidation channel, so the republished message is observable.
	sink := redisstore.Subscribe(ctx, rdb, config.ChannelSnapshotInvalidation)
	defer func() { _ = sink.Close() }()
	// Redis SUBSCRIBE is asynchronous; give the sink a beat to register before anything is published.
	time.Sleep(100 * time.Millisecond)

	relay := config.NewWatcher(
		func(ctx context.Context) (config.Stream, error) {
			return redisstore.Subscribe(ctx, rdb, config.ChannelConfigChanged), nil
		},
		func(ctx context.Context) error {
			return pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`))
		},
		config.WithWindow(20*time.Millisecond),
	)
	errc := make(chan error, 1)
	go func() { errc <- relay.Run(ctx) }()

	time.Sleep(100 * time.Millisecond) // let the relay's subscription register too

	if err := pub.Publish(ctx, config.ChannelConfigChanged, []byte("changed")); err != nil {
		t.Fatalf("publish config:changed: %v", err)
	}

	// The relay coalesces then republishes; the sink must receive exactly one invalidation.
	recvCtx, recvCancel := context.WithTimeout(ctx, 3*time.Second)
	defer recvCancel()
	if _, err := sink.Receive(recvCtx); err != nil {
		t.Fatalf("no invalidation republished on %s: %v", config.ChannelSnapshotInvalidation, err)
	}

	cancel()
	if err := <-errc; err != nil {
		t.Errorf("relay Run returned %v, want nil", err)
	}
}
