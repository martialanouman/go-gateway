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
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// image must match docker-compose.yml so the test stack and the dev stack cannot drift. Keep the two
// in lockstep.
const image = "redis:7-alpine"

var (
	once      sync.Once
	shared    *redis.Client
	sharedErr error
)

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
	client := redis.NewClient(opt)

	// A short readiness ping so the first test does not race the container's port opening.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return client, nil
}
