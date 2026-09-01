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
	"net/url"
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
	"github.com/martialanouman/go-gateway/internal/testutil/tcpproxy"
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

// productionMinConns mirrors config.Postgres's MIN_CONNS default; see CuttableConfig's doc comment for why
// it is fidelity rather than mechanism.
const productionMinConns = 2

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

// Cuttable returns a pool to the SAME shared, migrated PostgreSQL as Pool, reached through an in-process
// TCP proxy the test can Cut (Postgres disappears) and Resume (it comes back). It is how a chaos test
// proves a failure policy against a real outage instead of a fake LedgerStore that returns an error
// (step-260b) — a fake imitates the SHAPE of a fault, never its contract: a real pgx error travels through
// postgres.translate, which wraps it in a platform code a hand-rolled errors.New never carries.
//
// It proxies rather than stopping the container, for the same two reasons as redistest.Cuttable. First, it
// is the only option available: the container handle is a package-local variable inside start(),
// deliberately never exposed, because every helper here shares one container per test package (see the
// package doc). Second, and more importantly, stopping it would be WRONG — the sibling tests of the same
// package are talking to that container, and a chaos test must not decide their fate. A proxy severs one
// client's link and nothing else.
//
// The pool is built by postgres.NewPool, the production constructor, so the test inherits the real
// timeouts and lifetimes rather than test-special ones. Because that constructor pings eagerly, the pool
// is opened while the link is still up; call Cut afterwards.
//
// A test that asserts on durable state MUST read it through a second, uncut pool (pgtest.Pool) — reading
// it through this one makes the verification die with the dependency and pass by observing nothing.
//
// The pool is this test's own — unlike Pool's — and is closed by t.Cleanup.
func Cuttable(t *testing.T) (*pgxpool.Pool, *tcpproxy.Proxy) {
	t.Helper()

	cfg, proxy := CuttableConfig(t)
	pool, err := postgres.NewPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgtest: open pool through proxy: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, proxy
}

// CuttableConfig is Cuttable for code that opens its own pool from configuration — a service's wiring,
// say — rather than receiving one. It returns a config.Postgres addressed to the proxy, and the proxy that
// cuts it. Same discipline as Cuttable: build whatever reads the config BEFORE calling Cut.
//
// It starts from Config, so the pool lands on the shared container that is ALREADY MIGRATED; a second
// container would come up empty and every query would fail on a missing relation rather than on the
// outage under test.
//
// MinConns is set to the production default rather than left at Config's zero, for fidelity and nothing
// more: Cuttable exists to exercise what production does, and production keeps two connections warm. It is
// deliberately NOT load-bearing, and that was measured rather than assumed — with MinConns at zero the
// tests still pass and the outage is still immediate, because Cut closes the pool's boot connection like
// any other and a redial lands in the proxy's accept-then-close. Do not write a test that depends on it.
func CuttableConfig(t *testing.T) (config.Postgres, *tcpproxy.Proxy) {
	t.Helper()

	cfg := Config(t) // skips without Docker, and leaves sharedURL set
	u, err := url.Parse(cfg.URL)
	if err != nil {
		t.Fatalf("pgtest: parse shared url: %v", err)
	}
	proxy := tcpproxy.New(t, u.Host)
	u.Host = proxy.Addr()
	cfg.URL = u.String()
	cfg.MinConns = productionMinConns
	return cfg, proxy
}
