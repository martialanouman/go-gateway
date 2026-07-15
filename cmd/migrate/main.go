// Command migrate applies or reverses the control-plane schema migrations.
//
// It is a tool, not a deployable service: it has no ops port and exits when it is done. It backs
// `make migrate` and is the same binary a Kubernetes init container would run before a rollout.
//
// Usage:
//
//	migrate up               apply every pending migration (default)
//	migrate down             reverse every migration — destructive
//	migrate version          print the current schema version
//	migrate force <version>  clear a dirty flag after a manual repair
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
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

const serviceName = "migrate"

func main() {
	dir := flag.String("dir", postgres.DefaultMigrationsDir, "directory holding the migration files")
	flag.Parse()

	if err := run(*dir, flag.Args()); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run(dir string, args []string) error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	m, err := postgres.NewMigrator(cfg.Postgres.URL, dir, logger)
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
