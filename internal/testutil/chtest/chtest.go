// Package chtest starts a throwaway ClickHouse for integration tests and applies the CDR
// migration to it, so a test exercises the real versioned-CDR SQL against the real table rather
// than a mock. It mirrors pgtest and kafkatest: one shared container per package, cleanly skipped
// when Docker is unavailable or under -short.
package chtest

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

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/martialanouman/go-gateway/internal/config"
	chstorage "github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/ciguard"
)

// image must match docker-compose.yml so the test store and the dev store cannot drift.
const image = "clickhouse/clickhouse-server:24.8-alpine"

const (
	database = "gateway"
	username = "gateway"
	password = "gateway"
)

var (
	once      sync.Once
	shared    config.ClickHouse
	sharedErr error
)

// Config returns a config.ClickHouse bound to a shared, migrated ClickHouse. It skips the test when
// Docker is not available or under -short. Do not stop the container; the process teardown reclaims
// it. Each test isolates itself by using distinct message ids, not a fresh database.
func Config(t *testing.T) config.ClickHouse {
	t.Helper()

	if testing.Short() {
		ciguard.Skip(t, "chtest: skipped under -short (needs Docker)")
	}
	ciguard.RequireDocker(t)

	once.Do(func() { shared, _, sharedErr = start() })
	if sharedErr != nil {
		t.Fatalf("chtest: start shared clickhouse: %v", sharedErr)
	}
	return shared
}

// Start runs a dedicated, migrated ClickHouse customised by opts (a server config file, a network) and
// terminates it when the test ends. Use it only when the shared server cannot carry the configuration.
func Start(t *testing.T, opts ...testcontainers.ContainerCustomizer) config.ClickHouse {
	t.Helper()

	if testing.Short() {
		ciguard.Skip(t, "chtest: skipped under -short (needs Docker)")
	}
	ciguard.RequireDocker(t)

	cfg, container, err := start(opts...)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("chtest: start dedicated clickhouse: %v", err)
	}
	return cfg
}

func start(opts ...testcontainers.ContainerCustomizer) (config.ClickHouse, *tcclickhouse.ClickHouseContainer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tcclickhouse.Run(ctx, image, append([]testcontainers.ContainerCustomizer{
		tcclickhouse.WithDatabase(database),
		tcclickhouse.WithUsername(username),
		tcclickhouse.WithPassword(password),
	}, opts...)...)
	if err != nil {
		return config.ClickHouse{}, container, fmt.Errorf("run container: %w", err)
	}

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		return config.ClickHouse{}, container, fmt.Errorf("connection host: %w", err)
	}

	cfg := config.ClickHouse{
		Addr:     []string{host},
		Database: database,
		Username: username,
		Password: password,
		Timeout:  5 * time.Second,
	}

	if err := applyMigrations(cfg); err != nil {
		return config.ClickHouse{}, container, err
	}
	return cfg, container, nil
}

// applyMigrations runs the shipping migration path (clickhouse.NewMigrator) against the container,
// so the tests validate the same CDR migration that ships.
func applyMigrations(cfg config.ClickHouse) error {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	migrator, err := chstorage.NewMigrator(cfg, migrationsDir(), silent)
	if err != nil {
		return fmt.Errorf("open migrator: %w", err)
	}
	defer func() { _ = migrator.Close() }()

	if err := migrator.Up(); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// migrationsDir resolves migrations/clickhouse/ from this source file's location.
func migrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	// this file: internal/testutil/chtest/chtest.go -> repo root is three levels up.
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "migrations", "clickhouse")
}
