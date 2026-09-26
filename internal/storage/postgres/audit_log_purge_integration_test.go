package postgres_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
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

// TestAuditLogPurgeAtTheDefaultRetentionMeetsTheFloor: the default retention IS the floor, so Go and the
// trigger must draw the same line to the minute, or one row between them refuses the whole purge.
func TestAuditLogPurgeAtTheDefaultRetentionMeetsTheFloor(t *testing.T) {
	pool := pgtest.Pool(t)
	operator := "tok_default_" + uuid.NewString()
	past := insertAuditAged(t, pool, operator, 365*day+time.Minute)
	within := insertAuditAged(t, pool, operator, 365*day-time.Minute)

	if _, err := postgres.NewAuditLogRepo(pool).Purge(context.Background(), 365*day); err != nil {
		t.Fatalf("purge at the default retention: %v", err)
	}
	if left := survivingAudit(t, pool, operator); left[past] || !left[within] {
		t.Errorf("survivors = %v, want only the row within the retention", left)
	}
}

// TestAuditLogPurgeCannotReachBelowTheFloor: the trigger holds the floor whatever retention the caller asks
// for. The retention sits minutes under it, so only rows just under the floor can be what refuses the purge.
func TestAuditLogPurgeCannotReachBelowTheFloor(t *testing.T) {
	pool := pgtest.Pool(t)
	operator := "tok_floor_" + uuid.NewString()
	old := insertAuditAged(t, pool, operator, 500*day)
	underFloor := insertAuditAged(t, pool, operator, 365*day-time.Minute)

	_, err := postgres.NewAuditLogRepo(pool).Purge(context.Background(), 365*day-2*time.Minute)
	wantRefusedByDatabase(t, "a purge reaching under the floor", err)
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

// TestAuditLogRetentionRunsFromTheStart: the pass purges on its first round, not a whole interval later, and
// stops cleanly when the service does.
func TestAuditLogRetentionRunsFromTheStart(t *testing.T) {
	pool := pgtest.Pool(t)
	operator := "tok_run_" + uuid.NewString()
	expired := insertAuditAged(t, pool, operator, 500*day)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- postgres.NewAuditLogRepo(pool).RunRetention(ctx, time.Hour, 400*day, slog.New(slog.DiscardHandler))
	}()
	deadline := time.Now().Add(10 * time.Second)
	for survivingAudit(t, pool, operator)[expired] && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run = %v, want nil on shutdown", err)
	}
	if survivingAudit(t, pool, operator)[expired] {
		t.Error("the first pass did not purge the expired row")
	}
}

// TestAuditLogRetentionSurvivesAFailedPass: a refused pass is logged, never returned — returned, it would stop
// admin-api-svc through its supervisor.
func TestAuditLogRetentionSurvivesAFailedPass(t *testing.T) {
	pool := pgtest.Pool(t)
	insertAuditAged(t, pool, "tok_fail_"+uuid.NewString(), 364*day)

	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- postgres.NewAuditLogRepo(pool).RunRetention(ctx, time.Hour, 30*day, slog.New(slog.NewTextHandler(&logs, nil)))
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "audit log retention pass failed") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run = %v, want nil: a failed pass must not stop the service", err)
	}
	if !strings.Contains(logs.String(), "audit log retention pass failed") {
		t.Errorf("the refused pass was not logged:\n%s", logs.String())
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
