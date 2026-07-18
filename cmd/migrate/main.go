// Command migrate applies or reverses the schema migrations of a store.
//
// It is a tool, not a deployable service: it has no ops port and exits when it is done. It backs
// `make migrate` and is the same binary a Kubernetes init container would run before a rollout.
//
// Usage:
//
//	migrate [-store postgres|clickhouse] up               apply every pending migration (default)
//	migrate [-store ...] down             reverse every migration — destructive
//	migrate [-store ...] version          print the current schema version
//	migrate [-store ...] force <version>  clear a dirty flag after a manual repair
//
// PostgreSQL is the control plane; ClickHouse is the separate CDR store (its migration set lives in
// migrations/clickhouse/).
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

const serviceName = "migrate"

// migrator is the store-agnostic surface both the Postgres and ClickHouse migrators expose.
type migrator interface {
	Up() error
	Down() error
	Version() (uint, bool, error)
	Force(version int) error
	Close() error
}

func main() {
	store := flag.String("store", "postgres", "which store to migrate: postgres or clickhouse")
	dir := flag.String("dir", "", "directory holding the migration files (defaults per store)")
	flag.Parse()

	if err := run(*store, *dir, flag.Args()); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run(store, dir string, args []string) error {
	// Declare only the section for the chosen store: validating the other would refuse a production
	// migration Job that legitimately sets only that store's connection.
	section := config.SectionPostgres
	if store == "clickhouse" {
		section = config.SectionClickHouse
	}
	cfg, err := config.Load(serviceName, section)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	m, err := openMigrator(store, dir, cfg, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := m.Close(); err != nil {
			logger.Warn("close migrator", "err", err)
		}
	}()

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "up":
		return m.Up()
	case "down":
		return m.Down()
	case "version":
		version, dirty, err := m.Version()
		if err != nil {
			return err
		}
		logger.Info("schema version", "version", version, "dirty", dirty)
		return nil
	case "force":
		if len(args) < 2 {
			return errors.New("force: missing version argument")
		}
		version, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("force: %q is not a version number: %w", args[1], err)
		}
		return m.Force(version)
	default:
		return fmt.Errorf("unknown command %q: use up, down, version or force", cmd)
	}
}

// openMigrator builds the migrator for the chosen store, defaulting the directory to that store's
// migration set.
func openMigrator(store, dir string, cfg config.Config, logger *slog.Logger) (migrator, error) {
	switch store {
	case "postgres", "":
		if dir == "" {
			dir = postgres.DefaultMigrationsDir
		}
		return postgres.NewMigrator(cfg.Postgres.URL, dir, logger)
	case "clickhouse":
		if dir == "" {
			dir = clickhouse.DefaultMigrationsDir
		}
		return clickhouse.NewMigrator(cfg.ClickHouse, dir, logger)
	default:
		return nil, fmt.Errorf("unknown store %q: use postgres or clickhouse", store)
	}
}
