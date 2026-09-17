package postgres_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// migrationsBeforeFingerprint is the version just below 0014_operator_fingerprint.
const migrationsBeforeFingerprint = 13

// TestOperatorFingerprintMigrationMatchesGo proves migration 0014 rewrites every recorded operator token
// into exactly auth.Fingerprint — the SQL and Go computations must agree, or every row written before
// the migration names an operator no live principal can match. It also pins what the migration leaves
// alone: 'unknown', and a value already shaped like a fingerprint, so a binary that wrote fingerprints
// before the migration ran is not hashed twice.
//
// The shared pgtest database is already fully migrated, so the test migrates a fresh database of its own
// in the same container, stopping at 0013 to seed the pre-fingerprint state.
func TestOperatorFingerprintMigrationMatchesGo(t *testing.T) {
	ctx := context.Background()
	admin := pgtest.Pool(t)
	base := pgtest.Config(t).URL

	name := "fp_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})

	u, err := url.Parse(base)
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
	if err := m.Steps(migrationsBeforeFingerprint); err != nil {
		t.Fatalf("migrate to %d: %v", migrationsBeforeFingerprint, err)
	}

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	const rawToken = "raw-token-0123456789-0123456789-abc"
	already := auth.Fingerprint("another-operator-token-0123456789")
	// A token that merely STARTS like a fingerprint must still be rewritten: the regex is anchored at both
	// ends. And a token with a backslash and non-ASCII bytes must hash the same way: convert_to yields the
	// UTF-8 bytes Go hashes, where a ::bytea cast parses the backslash as an escape and rejects the value.
	prefixed := already + "-and-the-rest-of-a-real-token"
	const exotic = `jeton-opérateur\x41-0123456789-0123456789`
	operators := []string{rawToken, "unknown", already, prefixed, exotic}

	seed := map[string]string{
		"content_access_audit": `INSERT INTO control_plane.content_access_audit (operator, message_id, outcome)
			VALUES ($1, uuidv7(), 'granted')`,
		"gdpr_erase_jobs": `INSERT INTO control_plane.gdpr_erase_jobs (subject_type, subject_id, operator)
			VALUES ('msisdn', '22507000000', $1)`,
		"message_export_jobs": `INSERT INTO control_plane.message_export_jobs (format, filters, operator, expires_at)
			VALUES ('csv', '{}', $1, now() + interval '1 day')`,
	}
	for table, stmt := range seed {
		for _, op := range operators {
			if _, err := conn.Exec(ctx, stmt, op); err != nil {
				t.Fatalf("seed %s: %v", table, err)
			}
		}
	}

	if err := m.Steps(1); err != nil {
		t.Fatalf("apply 0014: %v", err)
	}

	want := map[string]bool{
		auth.Fingerprint(rawToken): true, "unknown": true, already: true,
		auth.Fingerprint(prefixed): true, auth.Fingerprint(exotic): true,
	}
	for table := range seed {
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT operator FROM control_plane.%s", table))
		if err != nil {
			t.Fatalf("read %s: %v", table, err)
		}
		got, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatalf("collect %s: %v", table, err)
		}
		if len(got) != len(operators) {
			t.Fatalf("%s holds %d rows, want %d", table, len(got), len(operators))
		}
		seen := map[string]bool{}
		for _, op := range got {
			if op == rawToken || op == prefixed || op == exotic {
				t.Errorf("%s still records a raw token", table)
			}
			if !want[op] {
				t.Errorf("%s operator = %q, want one of %v (SQL and Go fingerprints disagree, or a kept value changed)", table, op, want)
			}
			seen[op] = true
		}
		if len(seen) != len(want) {
			t.Errorf("%s operators = %v, want exactly %v", table, got, want)
		}
	}
}
