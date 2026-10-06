package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// migrateFreshTo creates a database of its own in the shared container and migrates it to version, so a
// test can seed the state a later migration has to cope with. The shared pgtest database is already fully
// migrated, which is why a migration's effect on existing rows cannot be tested there.
func migrateFreshTo(t *testing.T, version int) (*postgres.Migrator, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	admin := pgtest.Pool(t)

	name := "mig_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})
	u, err := url.Parse(pgtest.Config(t).URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Path = "/" + name

	m, err := postgres.NewMigrator(u.String(), "../../../migrations", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Steps(version); err != nil {
		t.Fatalf("migrate to %d: %v", version, err)
	}
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return m, conn
}
