package clickhouse

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	// clickhouse is the migration database driver; the blank import registers the "clickhouse" scheme.
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	// file is the migration source; the blank import registers the "file://" scheme.
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/martialanouman/go-gateway/internal/config"
)

// DefaultMigrationsDir is where the ClickHouse SQL files live, relative to the repository root. It
// is a SEPARATE set from the Postgres migrations (different engine, different driver): the CDR
// store evolves independently of the control plane.
const DefaultMigrationsDir = "migrations/clickhouse"

// Migrator applies the versioned CDR schema in migrations/clickhouse/ to a ClickHouse database. It
// mirrors postgres.Migrator: golang-migrate tracks the applied version in its own table, so Up is
// idempotent.
type Migrator struct {
	m   *migrate.Migrate
	log *slog.Logger
}

// NewMigrator opens a migrator against the configured ClickHouse for the files in dir. It connects
// to the first configured address; a single node is enough to hold the migration lock at M2.
func NewMigrator(cfg config.ClickHouse, dir string, log *slog.Logger) (*Migrator, error) {
	if len(cfg.Addr) == 0 {
		return nil, fmt.Errorf("clickhouse: no address configured for migrations")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve migrations dir %q: %w", dir, err)
	}

	m, err := migrate.New("file://"+abs, migrateURL(cfg))
	if err != nil {
		// The URL carries a password: report the directory, never the URL.
		return nil, fmt.Errorf("open clickhouse migrator on %s: %w", abs, err)
	}
	return &Migrator{m: m, log: log}, nil
}

// migrateURL builds the golang-migrate ClickHouse DSN. x-multi-statement is deliberately NOT set:
// the driver's multi-statement splitter is a naive split on ';', which breaks on a ';' inside an
// SQL comment. The convention is therefore ONE statement per migration file (add 0002, 0003… for
// further tables), which the driver runs as a single Exec — robust and simple.
func migrateURL(cfg config.ClickHouse) string {
	q := url.Values{}
	q.Set("username", cfg.Username)
	q.Set("password", cfg.Password)
	q.Set("database", cfg.Database)
	u := url.URL{Scheme: "clickhouse", Host: cfg.Addr[0], RawQuery: q.Encode()}
	return u.String()
}

// Up applies every pending migration. It reports nil when the schema is already current, so a
// deploy that runs migrations on every start does not fail when there is nothing to do.
func (m *Migrator) Up() error {
	err := m.m.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		m.log.Info("clickhouse schema already up to date")
		return nil
	}
	if err != nil {
		return fmt.Errorf("apply clickhouse migrations: %w", err)
	}
	m.log.Info("clickhouse migrations applied")
	return nil
}

// Down reverses every applied migration, dropping the CDR schema. It is destructive and exists for
// development and tests.
func (m *Migrator) Down() error {
	err := m.m.Down()
	if errors.Is(err, migrate.ErrNoChange) {
		m.log.Info("no clickhouse migration to reverse")
		return nil
	}
	if err != nil {
		return fmt.Errorf("reverse clickhouse migrations: %w", err)
	}
	m.log.Info("clickhouse migrations reversed")
	return nil
}

// Close releases the migrator's own connection. It does not touch the service's Conn.
func (m *Migrator) Close() error {
	srcErr, dbErr := m.m.Close()
	return errors.Join(srcErr, dbErr)
}
