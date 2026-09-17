package postgres_test

import (
	"context"
	"testing"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestAuditLogRecordsIntentThenOutcomeOnce: an audited request is recorded before its handler runs (no
// outcome yet — NULL reads "not recorded", never "succeeded"), then its outcome is written once. A second
// outcome for the same row is ignored: an audit row is never rewritten.
func TestAuditLogRecordsIntentThenOutcomeOnce(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewAuditLogRepo(pool)

	id, err := repo.Begin(ctx, cp.AuditIntent{
		Operator: "tok_0123456789abcdef", OperationID: "create-customer",
		Method: "POST", Target: "/v1/admin/customers", RequestID: "req-290c",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	read := func() (*int16, *time.Time, *string) {
		t.Helper()
		var status *int16
		var finished *time.Time
		var reqID *string
		if err := pool.QueryRow(ctx,
			`SELECT status, finished_at, request_id FROM control_plane.audit_log WHERE id = $1`, id,
		).Scan(&status, &finished, &reqID); err != nil {
			t.Fatalf("read row: %v", err)
		}
		return status, finished, reqID
	}

	status, finished, reqID := read()
	if status != nil || finished != nil {
		t.Fatalf("fresh intent has status=%v finished=%v, want both NULL", status, finished)
	}
	if reqID == nil || *reqID != "req-290c" {
		t.Errorf("request_id = %v, want req-290c", reqID)
	}

	if err := repo.Finish(ctx, id, 201); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if status, finished, _ = read(); status == nil || *status != 201 || finished == nil {
		t.Fatalf("after finish status=%v finished=%v, want 201 and a time", status, finished)
	}

	if err := repo.Finish(ctx, id, 500); err != nil {
		t.Fatalf("second finish: %v", err)
	}
	if status, _, _ = read(); *status != 201 {
		t.Errorf("status = %d after a second finish, want the first outcome 201 kept", *status)
	}
}

// TestAuditLogStoresAnAbsentRequestIDAsNull: no request id is NULL, not an empty string an auditor would
// have to special-case.
func TestAuditLogStoresAnAbsentRequestIDAsNull(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	id, err := postgres.NewAuditLogRepo(pool).Begin(ctx, cp.AuditIntent{
		Operator: "unknown", OperationID: "delete-route", Method: "DELETE", Target: "/v1/admin/routes/x",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var reqID *string
	if err := pool.QueryRow(ctx, `SELECT request_id FROM control_plane.audit_log WHERE id = $1`, id).Scan(&reqID); err != nil {
		t.Fatalf("read: %v", err)
	}
	if reqID != nil {
		t.Errorf("request_id = %q, want NULL", *reqID)
	}
}
