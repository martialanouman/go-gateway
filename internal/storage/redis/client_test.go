package redis_test

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// TestPingCheckFailsWhenRedisUnreachable is the substance of "readyz goes red when Redis is cut": the
// registry's vital-dependency probe must report an error, not hang or pass, when the store cannot be
// reached. session-manager-svc wires this exact check into /readyz, so a red probe removes the pod
// from the load balancer (plan §1.5). 127.0.0.1:1 is a reserved port nothing listens on, so the dial
// fails fast without needing Docker.
func TestPingCheckFailsWhenRedisUnreachable(t *testing.T) {
	t.Parallel()

	client := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })

	check := redisstore.PingCheck("redis", client, 200*time.Millisecond)
	if check.Name != "redis" {
		t.Errorf("check.Name = %q, want redis", check.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := check.Probe(ctx); err == nil {
		t.Fatal("Probe() = nil, want an error when Redis is unreachable")
	}
}
