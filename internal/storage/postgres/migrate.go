// Package postgres holds the control-plane database access: the schema migration runner today,
// the repositories as later milestones land.
package postgres

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	// pgx/v5 is the migration driver; the blank import registers it under the "pgx5" URL scheme.
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	// file is the migration source; the blank import registers the "file://" scheme.
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// DefaultMigrationsDir is where the SQL files live, relative to the repository root.
const DefaultMigrationsDir = "migrations"

// Migrator applies the versioned schema in migrations/ to a PostgreSQL database.
//
// The files are derived from db/schema_passerelle_sms.sql, which stays the annotated reference;
// golang-migrate tracks the applied version in a table of its own, so running Up twice is a
// no-op rather than an error.
type Migrator struct {
	m   *migrate.Migrate
	log *slog.Logger
}

// NewMigrator opens a migrator against databaseURL for the migration files in dir.
//
// databaseURL is a pgx connection string; its scheme is rewritten to the driver's own ("pgx5"),
// so callers pass the same POSTGRES_URL the services use rather than a second, subtly different
// spelling of it. Close the migrator when done.
func NewMigrator(databaseURL, dir string, log *slog.Logger) (*Migrator, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve migrations dir %q: %w", dir, err)
	}

	driverURL, err := toPgxURL(databaseURL)
	if err != nil {
		return nil, err
	}

	m, err := migrate.New("file://"+abs, driverURL)
	if err != nil {
		// The URL carries a password: report the directory, never the URL.
		return nil, fmt.Errorf("open migrator on %s: %w", abs, err)
	}
	return &Migrator{m: m, log: log}, nil
}

// Up applies every pending migration. It reports nil when the schema is already current: a
// deploy that runs migrations on every start must not fail because there was nothing to do.
func (m *Migrator) Up() error {
	err := m.m.Up()
	if errors.Is(err, migrate.ErrNoChange) {
		m.log.Info("schema already up to date")
		return nil
	}
	if err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	version, dirty, verr := m.Version()
	if verr != nil {
		// The migration itself succeeded; failing to read the version back is not a reason to
		// report the deploy as failed.
		m.log.Warn("migrations applied but version unreadable", "err", verr)
		return nil
	}
	m.log.Info("migrations applied", "version", version, "dirty", dirty)
	return nil
}

// Down reverses every applied migration, dropping the schema. It is destructive and exists for
// development and tests; production rollbacks go through a forward migration.
func (m *Migrator) Down() error {
	err := m.m.Down()
	if errors.Is(err, migrate.ErrNoChange) {
		m.log.Info("no migration to reverse")
		return nil
	}
	if err != nil {
		return fmt.Errorf("reverse migrations: %w", err)
	}
	m.log.Info("migrations reversed")
	return nil
}

// Steps applies n migrations up (n > 0) or reverses n migrations down (n < 0).
func (m *Migrator) Steps(n int) error {
	if n == 0 {
		return errors.New("migrate steps: n is zero")
	}
	err := m.m.Steps(n)
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("apply %d migration steps: %w", n, err)
	}
	return nil
}

// Version returns the current schema version and whether it is dirty.
//
// A dirty version means a migration failed part-way and the schema is in an unknown state:
// golang-migrate refuses to continue until an operator inspects it and calls Force. Do not
// automate that away — the point of the flag is that a human looks.
func (m *Migrator) Version() (uint, bool, error) {
	version, dirty, err := m.m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read schema version: %w", err)
	}
	return version, dirty, nil
}

// Force pins the version and clears the dirty flag, without running any migration. It is the
// manual repair path after a failed migration, once an operator has checked what actually
// landed.
func (m *Migrator) Force(version int) error {
	if err := m.m.Force(version); err != nil {
		return fmt.Errorf("force schema version %d: %w", version, err)
	}
	m.log.Warn("schema version forced", "version", version)
	return nil
}

// Close releases the migrator's database connection.
func (m *Migrator) Close() error {
	srcErr, dbErr := m.m.Close()
	return errors.Join(srcErr, dbErr)
}

// toPgxURL rewrites a postgres:// or postgresql:// URL to the pgx/v5 driver's scheme. Any other
// scheme is rejected rather than passed through: golang-migrate would otherwise pick a different
// driver than the one this package registered, and fail with a confusing error.
func toPgxURL(databaseURL string) (string, error) {
	const driverScheme = "pgx5://"

	switch {
	case strings.HasPrefix(databaseURL, driverScheme):
		return databaseURL, nil
	case strings.HasPrefix(databaseURL, "postgres://"):
		return driverScheme + strings.TrimPrefix(databaseURL, "postgres://"), nil
	case strings.HasPrefix(databaseURL, "postgresql://"):
		return driverScheme + strings.TrimPrefix(databaseURL, "postgresql://"), nil
	default:
		// Never echo the URL: it carries the password.
		return "", errors.New("database url must start with postgres://, postgresql:// or pgx5://")
	}
}
