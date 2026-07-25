// Package redis opens the operational-state store shared by the data-plane services: the session
// registry, the throttling token buckets, the Bloom filters and the balance cache all live in Redis
// (plan §1). It mirrors internal/storage/postgres: a constructor that fails the boot on an
// unreachable store, and a client-level readiness probe for /readyz.
//
// The go-redis package is imported as goredis so this package can keep the natural name "redis"
// without shadowing it.
package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// NewClient opens the operational-state client from cfg. It returns a ready client: one PING is sent
// eagerly, so a bad URL or an unreachable Redis fails the boot rather than the first command (guide
// de codage §10). Close the client when done.
func NewClient(ctx context.Context, cfg config.Redis) (*goredis.Client, error) {
	opt, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		// ParseURL echoes the URL, which may carry a password, in its error. Never surface it.
		return nil, fmt.Errorf("parse redis url: invalid connection string")
	}
	opt.DialTimeout = cfg.Timeout
	opt.ReadTimeout = cfg.Timeout
	opt.WriteTimeout = cfg.Timeout

	client := goredis.NewClient(opt)

	pingCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("reach redis on boot: %w", err)
	}
	return client, nil
}

// PubSubPublisher adapts a go-redis client to a payload-oriented Publish, the shape the session
// force-disconnect fan-out (step-032) depends on. Keeping the adapter here lets consumers depend on a
// tiny interface instead of the whole go-redis client.
type PubSubPublisher struct{ client *goredis.Client }

// NewPubSubPublisher returns a publisher that PUBLISHes on client.
func NewPubSubPublisher(client *goredis.Client) *PubSubPublisher {
	return &PubSubPublisher{client: client}
}

// Publish sends payload on channel. It returns the PUBLISH error (never the count of receivers): a
// fan-out with no live subscriber is not an error here.
func (p *PubSubPublisher) Publish(ctx context.Context, channel string, payload []byte) error {
	return p.client.Publish(ctx, channel, payload).Err()
}

// Subscribe opens a pub/sub subscription on channel and returns a Subscription that yields payloads.
// Close it when done. It is the consumer side of the disconnect fan-out (step-032).
func Subscribe(ctx context.Context, client *goredis.Client, channel string) *Subscription {
	return &Subscription{ps: client.Subscribe(ctx, channel)}
}

// Subscription wraps a go-redis PubSub as a payload stream. ReceiveMessage handles reconnection
// internally, so a transient Redis blip surfaces as a retryable error rather than a dead subscription.
type Subscription struct{ ps *goredis.PubSub }

// Receive blocks for the next message and returns its payload, or ctx's error on cancellation.
func (s *Subscription) Receive(ctx context.Context) ([]byte, error) {
	msg, err := s.ps.ReceiveMessage(ctx)
	if err != nil {
		return nil, err
	}
	return []byte(msg.Payload), nil
}

// Close ends the subscription.
func (s *Subscription) Close() error { return s.ps.Close() }

// PingCheck reports the client's health for /readyz. It probes the client (a real PING round-trip),
// not a TCP address: a Redis in a failed state answers a dial happily and a command with an error. It
// honours ctx and bounds itself at timeout, because the kubelet calls it every few seconds.
func PingCheck(name string, client *goredis.Client, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, func(ctx context.Context) error {
		return client.Ping(ctx).Err()
	})
}
