// Package redistest starts a throwaway Redis for integration tests, so a test exercises real Redis
// (real Lua, real sorted-set semantics) rather than a mock.
//
// The container is shared across a package's tests (starting one per test would add seconds to a run);
// each test isolates itself with a unique account id, not a fresh server. Tests skip cleanly when
// Docker is unavailable or under `go test -short`, so `make test` still works on a laptop with Docker
// off while CI, which has Docker, runs them for real.
package redistest

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/martialanouman/go-gateway/internal/config"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/tcpproxy"
)

// image must match docker-compose.yml so the test stack and the dev stack cannot drift. Keep the two
// in lockstep.
const image = "redis:7-alpine"

var (
	once      sync.Once
	shared    *redis.Client
	sharedURL string
	sharedErr error
)

// connectTimeout bounds a boot connection made from Config. It is generous: the container is already
// up by the time Config returns, and a tight timeout would only make a busy CI flaky.
const connectTimeout = 10 * time.Second

// Config returns a config.Redis pointing at the same shared Redis as Client. Use it when the code
// under test opens its own client from configuration — a service's wiring, say — rather than
// receiving one.
func Config(t *testing.T) config.Redis {
	t.Helper()

	Client(t) // shares the skip/start discipline; leaves sharedURL set
	return config.Redis{URL: sharedURL, Timeout: connectTimeout}
}

// Client returns a client bound to a shared Redis. It skips the test when Docker is not available or
// under -short. The returned client is shared across the calling package's tests; do not Close it —
// the process teardown (testcontainers' reaper) reclaims the container.
func Client(t *testing.T) *redis.Client {
	t.Helper()

	if testing.Short() {
		t.Skip("redistest: skipped under -short (needs Docker)")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	once.Do(func() { shared, sharedErr = start() })
	if sharedErr != nil {
		t.Fatalf("redistest: start shared redis: %v", sharedErr)
	}
	return shared
}

// start brings up the container and opens the client. It runs once.
func start() (*redis.Client, error) {
	ctx := context.Background()

	container, err := tcredis.Run(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("run container: %w", err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	sharedURL = url
	client := redis.NewClient(opt)

	// A short readiness ping so the first test does not race the container's port opening.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return client, nil
}

// Cuttable returns a client to the SAME shared Redis as Client, reached through an in-process TCP proxy
// the test can Cut (Redis disappears) and Resume (it comes back). It is how a chaos test proves a
// failure policy against a real outage instead of a fake that returns an error (step-250).
//
// It proxies rather than stopping the container, for two reasons and not merely as a convenience.
// First, it is the only option available: the container handle is a package-local variable inside
// start(), deliberately never exposed, because every helper here shares one container per test package
// (see the package doc). Second, and more importantly, stopping it would be WRONG — the sibling tests
// of the same package are talking to that container, and a chaos test must not decide their fate. A
// proxy severs one client's link and nothing else.
//
// The client is built by redisstore.NewClient, the production constructor, so the test inherits the
// real dial/read/write timeouts rather than test-special ones — the point is to exercise what
// production would do. (Those timeouts are an upper bound the outage never reaches: Cut accepts a
// connection and immediately closes it, so commands fail on a dead socket, not on a deadline.) Because
// the constructor pings eagerly, the client is opened while the link is still up; call Cut afterwards.
//
// The returned client is this test's own — unlike Client's — and is closed by t.Cleanup.
func Cuttable(t *testing.T) (*redis.Client, *tcpproxy.Proxy) {
	t.Helper()

	cfg, proxy := CuttableConfig(t)
	client, err := redisstore.NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("redistest: open client through proxy: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, proxy
}

// CuttableConfig is Cuttable for code that opens its own client from configuration — a service's
// wiring, say — rather than receiving one. It returns a config.Redis addressed to the proxy, and the
// proxy that cuts it. Same discipline as Cuttable: build whatever reads the config BEFORE calling Cut.
func CuttableConfig(t *testing.T) (config.Redis, *tcpproxy.Proxy) {
	t.Helper()

	cfg := Config(t) // skips without Docker, and leaves sharedURL set
	u, err := url.Parse(cfg.URL)
	if err != nil {
		t.Fatalf("redistest: parse shared url: %v", err)
	}
	proxy := tcpproxy.New(t, u.Host)
	u.Host = proxy.Addr()
	cfg.URL = u.String()
	return cfg, proxy
}
