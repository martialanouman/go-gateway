package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

const day = 24 * time.Hour

func insertAuditAged(t *testing.T, pool *pgxpool.Pool, operator string, age time.Duration) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := pool.QueryRow(context.Background(), `
		INSERT INTO control_plane.audit_log (operator, operation_id, method, target, at)
		VALUES ($1, 'create-customer', 'POST', '/v1/admin/customers', now() - make_interval(secs => $2))
		RETURNING id`, operator, age.Seconds()).Scan(&id)
	if err != nil {
		t.Fatalf("insert aged row: %v", err)
	}
	return id
}

func survivingAudit(t *testing.T, pool *pgxpool.Pool, operator string) map[uuid.UUID]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id FROM control_plane.audit_log WHERE operator = $1`, operator)
	if err != nil {
		t.Fatalf("read survivors: %v", err)
	}
	defer rows.Close()
	out := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out[id] = true
	}
	return out
}

// TestAuditLogPurgeRemovesOnlyWhatOutlivedItsRetention: the purge deletes the rows past the retention, and
// nothing younger — on a real database, through the trigger's door.
func TestAuditLogPurgeRemovesOnlyWhatOutlivedItsRetention(t *testing.T) {
	pool := pgtest.Pool(t)
	operator := "tok_purge_" + uuid.NewString()
	expired := insertAuditAged(t, pool, operator, 500*day)
	justExpired := insertAuditAged(t, pool, operator, 401*day)
	justKept := insertAuditAged(t, pool, operator, 399*day)
	young := insertAuditAged(t, pool, operator, time.Hour)

	n, err := postgres.NewAuditLogRepo(pool).Purge(context.Background(), 400*day)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	left := survivingAudit(t, pool, operator)
	if left[expired] || left[justExpired] {
		t.Errorf("rows past the retention survived: %v", left)
	}
	if !left[justKept] || !left[young] {
		t.Errorf("rows within the retention were purged: %v", left)
	}
	if n < 2 {
		t.Errorf("purged = %d, want at least this test's 2 expired rows", n)
	}
}

// TestAuditLogPurgeCannotReachBelowTheFloor: the trigger holds the spec's one-year floor, whatever retention
// the caller asks for. One row under the floor refuses the whole statement, so nothing is deleted.
func TestAuditLogPurgeCannotReachBelowTheFloor(t *testing.T) {
	pool := pgtest.Pool(t)
	operator := "tok_floor_" + uuid.NewString()
	old := insertAuditAged(t, pool, operator, 500*day)
	underFloor := insertAuditAged(t, pool, operator, 364*day)

	_, err := postgres.NewAuditLogRepo(pool).Purge(context.Background(), 30*day)
	wantRefusedByDatabase(t, "a purge reaching under the 365-day floor", err)
	if left := survivingAudit(t, pool, operator); !left[old] || !left[underFloor] {
		t.Errorf("a refused purge deleted rows: %v", left)
	}
}

// TestAuditLogDeleteOutsideThePurgeIsRefused: an old row is deletable only through the purge's door. A plain
// DELETE, or a SET LOCAL outside a transaction (a silent no-op), is refused by the trigger itself.
func TestAuditLogDeleteOutsideThePurgeIsRefused(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	old := insertAuditAged(t, pool, "tok_door_"+uuid.NewString(), 500*day)

	_, err := pool.Exec(ctx, `DELETE FROM control_plane.audit_log WHERE id = $1`, old)
	wantRefusedByDatabase(t, "a plain DELETE of an expired row", err)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET LOCAL audit_log.purge = 'on'`); err != nil {
		t.Fatalf("set local: %v", err)
	}
	_, err = conn.Exec(ctx, `DELETE FROM control_plane.audit_log WHERE id = $1`, old)
	wantRefusedByDatabase(t, "a DELETE after an untransacted SET LOCAL", err)
}
