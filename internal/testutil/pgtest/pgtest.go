// Package pgtest starts a throwaway PostgreSQL for integration tests and applies the repository
// migrations to it, so a test exercises real SQL against the real schema rather than a mock.
//
// The container is shared across a package's tests (starting one per test would add minutes to a
// run); each test isolates itself with fresh rows, not a fresh database. Tests skip cleanly when
// Docker is unavailable or under `go test -short`, so `make test` still works on a laptop with
// Docker off while CI, which has Docker, runs them for real.
package pgtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/ciguard"
)

// image must be PostgreSQL 18: uuidv7() is native only there, and migrations/0001 RAISE EXCEPTIONs
// on an older server. It matches docker-compose.yml so the test stack and the dev stack cannot
// drift. Keep the two in lockstep.
const image = "postgres:18-alpine"

var (
	once      sync.Once
	shared    *pgxpool.Pool
	sharedURL string
	sharedErr error
)

// connectTimeout bounds a boot connection made from Config. It is generous: the container is already
// up by the time Config returns, and a tight timeout would only make a busy CI flaky.
const connectTimeout = 10 * time.Second

// Config returns a config.Postgres pointing at the same shared, migrated PostgreSQL 18 as Pool. Use
// it when the code under test opens its own pool from configuration — a service's wiring, say —
// rather than receiving one.
func Config(t *testing.T) config.Postgres {
	t.Helper()

	Pool(t) // shares the skip/start discipline; leaves sharedURL set
	return config.Postgres{URL: sharedURL, MaxConns: 4, Timeout: connectTimeout}
}

// Pool returns a pool bound to a shared, migrated PostgreSQL 18. It skips the test when Docker is
// not available or under -short. The returned pool is shared across the calling package's tests;
// do not Close it — the process teardown (testcontainers' reaper) reclaims the container.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	if testing.Short() {
		ciguard.Skip(t, "pgtest: skipped under -short (needs Docker)")
	}
	ciguard.RequireDocker(t)

	once.Do(func() { shared, sharedErr = start() })
	if sharedErr != nil {
		t.Fatalf("pgtest: start shared postgres: %v", sharedErr)
	}
	return shared
}

// start brings up the container, applies the migrations, and opens the pool. It runs once.
func start() (*pgxpool.Pool, error) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("gateway"),
		tcpostgres.WithUsername("gateway"),
		tcpostgres.WithPassword("gateway"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("run container: %w", err)
	}

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}

	if err := applyMigrations(url); err != nil {
		return nil, err
	}
	sharedURL = url

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	// A short readiness ping so the first test does not race the container's port opening.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// applyMigrations runs the real migration path (postgres.NewMigrator) against the container, so the
// integration tests validate the same migrations that ship, not a parallel copy.
func applyMigrations(url string) error {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	migrator, err := postgres.NewMigrator(url, migrationsDir(), silent)
	if err != nil {
		return fmt.Errorf("open migrator: %w", err)
	}
	defer func() { _ = migrator.Close() }()

	if err := migrator.Up(); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// migrationsDir resolves migrations/ from this source file's location, so tests find it whatever
// package they run from.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	// this file: internal/testutil/pgtest/pgtest.go -> repo root is three levels up.
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations")
}
