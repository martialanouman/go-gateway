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

// migrationsBeforePriorityFlagCheck is the version just below 0033_connector_priority_flag_default_check.
const migrationsBeforePriorityFlagCheck = 32

// TestPriorityFlagCheckMigrationClampsExistingRows: before step-294 the column was set only in SQL and
// had no bound, so a value out of the SMPP range may exist. The migration must clamp it to what the pool
// already sent (it clamped on read) instead of failing and leaving the database dirty.
func TestPriorityFlagCheckMigrationClampsExistingRows(t *testing.T) {
	ctx := context.Background()
	admin := pgtest.Pool(t)

	name := "pf_" + uuid.NewString()[:8]
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
	dbURL := u.String()

	m, err := postgres.NewMigrator(dbURL, "../../../migrations", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Steps(migrationsBeforePriorityFlagCheck); err != nil {
		t.Fatalf("migrate to %d: %v", migrationsBeforePriorityFlagCheck, err)
	}

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for label, value := range map[string]int{"above": 5, "below": -1, "inside": 2} {
		if _, err := conn.Exec(ctx, `INSERT INTO control_plane.smsc_connectors
			(name, host, port, bind_type, system_id, password_sealed, password_kms_key_ref, priority_flag_default)
			VALUES ($1, 'h', 2775, 'trx', 's', '\x01', 'test/v1', $2)`, label, value); err != nil {
			t.Fatalf("seed %s: %v", label, err)
		}
	}

	if err := m.Steps(1); err != nil {
		t.Fatalf("migrate 0033: %v", err)
	}
	for label, want := range map[string]int{"above": 3, "below": 0, "inside": 2} {
		var got int
		if err := conn.QueryRow(ctx, `SELECT priority_flag_default FROM control_plane.smsc_connectors WHERE name = $1`, label).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", label, err)
		}
		if got != want {
			t.Errorf("%s: priority_flag_default = %d, want %d", label, got, want)
		}
	}
}
