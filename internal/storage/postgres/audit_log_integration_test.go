package postgres_test

import (
	"context"
	"strings"
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
		var operator, operationID, method, target string
		if err := pool.QueryRow(ctx,
			`SELECT status, finished_at, request_id, operator, operation_id, method, target
			   FROM control_plane.audit_log WHERE id = $1`, id,
		).Scan(&status, &finished, &reqID, &operator, &operationID, &method, &target); err != nil {
			t.Fatalf("read row: %v", err)
		}
		// Every column is read back: Operator, OperationID, Method and Target are all strings, so a pair
		// swapped in the repository would compile and pass a test that only checks the outcome.
		if operator != "tok_0123456789abcdef" || operationID != "create-customer" ||
			method != "POST" || target != "/v1/admin/customers" {
			t.Errorf("row = operator %q, operation %q, method %q, target %q — want them in their own columns",
				operator, operationID, method, target)
		}
		return status, finished, reqID
	}

	status, finished, reqID := read()
	if status != nil || finished != nil {
		t.Fatalf("fresh intent has status=%v finished=%v, want both NULL", deref(status), finished)
	}
	if reqID == nil || *reqID != "req-290c" {
		t.Errorf("request_id = %v, want req-290c", derefString(reqID))
	}

	// An outcome that is not an HTTP status is refused rather than written: a row must read as NULL
	// ("outcome not recorded") or as a status, never as 0.
	if err := repo.Finish(ctx, id, 0); err == nil {
		t.Error("Finish(0) = nil, want a refusal: 0 is not an HTTP status")
	}
	if status, _, _ = read(); status != nil {
		t.Errorf("status = %v after a refused outcome, want it still NULL", deref(status))
	}

	if err := repo.Finish(ctx, id, 201); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if status, finished, _ = read(); status == nil || *status != 201 || finished == nil {
		t.Fatalf("after finish status=%v finished=%v, want 201 and a time", deref(status), finished)
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

// TestAuditLogBoundsTheRequestID: the request id comes from a client header (chi echoes X-Request-Id), so
// the trail bounds it rather than storing whatever an audited operator sends — including bytes Postgres
// would refuse, which would turn every write into a 503.
func TestAuditLogBoundsTheRequestID(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	id, err := postgres.NewAuditLogRepo(pool).Begin(ctx, cp.AuditIntent{
		Operator: "unknown", OperationID: "delete-route", Method: "DELETE", Target: "/v1/admin/routes/x",
		// A long id exercises the truncation branch; the short one below is what proves the sanitising,
		// since converting to runes would already replace an invalid byte on this path.
		RequestID: "req-\xff-" + strings.Repeat("x", 300),
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var reqID *string
	if err := pool.QueryRow(ctx, `SELECT request_id FROM control_plane.audit_log WHERE id = $1`, id).Scan(&reqID); err != nil {
		t.Fatalf("read: %v", err)
	}
	if reqID == nil || len(*reqID) > 64 {
		t.Errorf("request_id = %v, want it kept but bounded to 64 characters", derefString(reqID))
	}

	// A SHORT invalid id is the case truncation cannot save: Postgres refuses an invalid byte outright
	// (SQLSTATE 22021), and a failed intent refuses the request.
	if _, err := postgres.NewAuditLogRepo(pool).Begin(ctx, cp.AuditIntent{
		Operator: "unknown", OperationID: "delete-route", Method: "DELETE", Target: "/v1/admin/routes/x",
		RequestID: "req-\xff",
	}); err != nil {
		t.Errorf("begin with an invalid byte in the request id: %v — the trail must sanitise it, not refuse the write", err)
	}

	// An id made ONLY of invalid bytes sanitises to nothing: that is no id at all, so it reads as NULL
	// rather than as an empty string an auditor would have to interpret.
	id, err = postgres.NewAuditLogRepo(pool).Begin(ctx, cp.AuditIntent{
		Operator: "unknown", OperationID: "delete-route", Method: "DELETE", Target: "/v1/admin/routes/x",
		RequestID: "\xff\xfe",
	})
	if err != nil {
		t.Fatalf("begin with an all-invalid request id: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT request_id FROM control_plane.audit_log WHERE id = $1`, id).Scan(&reqID); err != nil {
		t.Fatalf("read: %v", err)
	}
	if reqID != nil {
		t.Errorf("request_id = %q, want NULL", *reqID)
	}
}

// deref renders a nullable status for a failure message: %v on a *int16 prints an address.
func deref(v *int16) any {
	if v == nil {
		return "NULL"
	}
	return *v
}

// derefString renders a nullable text column for a failure message.
func derefString(v *string) any {
	if v == nil {
		return "NULL"
	}
	return *v
}
