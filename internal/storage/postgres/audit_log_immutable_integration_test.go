package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

func beginAudit(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id, err := postgres.NewAuditLogRepo(pool).Begin(context.Background(), cp.AuditIntent{
		Operator: "tok_immutable", OperationID: "create-customer", Method: "POST", Target: "/v1/admin/customers",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return id
}

func wantRefusedByDatabase(t *testing.T, what string, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Errorf("%s: err = %v, want the database to refuse it (SQLSTATE 42501)", what, err)
	}
}

// TestAuditLogIsAppendOnlyInTheDatabase: the trail is immutable by constraint, not by convention. The test
// pool is a superuser, which privileges do not bind: what refuses here is the trigger.
func TestAuditLogIsAppendOnlyInTheDatabase(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewAuditLogRepo(pool)

	open := beginAudit(t, pool)
	_, err := pool.Exec(ctx, `UPDATE control_plane.audit_log SET finished_at = now() WHERE id = $1`, open)
	wantRefusedByDatabase(t, "finishing an open row without an outcome", err)

	_, err = pool.Exec(ctx, `UPDATE control_plane.audit_log SET status = 201 WHERE id = $1`, open)
	wantRefusedByDatabase(t, "closing a row without its finish time", err)

	_, err = pool.Exec(ctx, `UPDATE control_plane.audit_log SET status = 201, finished_at = '2020-01-01' WHERE id = $1`, open)
	wantRefusedByDatabase(t, "closing a row with a backdated finish time", err)

	// Each rewrite rides along the one allowed transition: alone, the missing status would refuse it and
	// hide whether the column itself is guarded.
	for _, rewrite := range []string{
		"id = uuidv7()", "operator = 'tok_other'", "operation_id = 'delete-customer'", "method = 'GET'",
		"target = '/v1/admin/elsewhere'", "request_id = 'forged'", "at = at - interval '1 day'",
	} {
		_, err = pool.Exec(ctx, `UPDATE control_plane.audit_log SET status = 201, `+rewrite+` WHERE id = $1`, open)
		wantRefusedByDatabase(t, "closing a row while rewriting "+rewrite, err)
	}

	if err := repo.Finish(ctx, open, 201); err != nil {
		t.Fatalf("finish: %v — the one allowed transition must still pass", err)
	}
	_, err = pool.Exec(ctx, `UPDATE control_plane.audit_log SET status = 500 WHERE id = $1`, open)
	wantRefusedByDatabase(t, "a second outcome on a closed row", err)

	_, err = pool.Exec(ctx, `UPDATE control_plane.audit_log SET finished_at = now() WHERE id = $1`, open)
	wantRefusedByDatabase(t, "moving finished_at of a closed row", err)

	_, err = pool.Exec(ctx, `DELETE FROM control_plane.audit_log WHERE id = $1`, open)
	wantRefusedByDatabase(t, "DELETE", err)

	_, err = pool.Exec(ctx, `TRUNCATE control_plane.audit_log`)
	wantRefusedByDatabase(t, "TRUNCATE", err)

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.audit_log WHERE id = $1 AND status = 201`, open).Scan(&n); err != nil || n != 1 {
		t.Errorf("row after the refused writes: count = %d, err = %v — want it intact with its first outcome", n, err)
	}
}

// TestAuditLogOwnerHoldsNoDeletePrivilege: the owner is the application role today (one POSTGRES_URL). In
// production it is not a superuser, so the revoked privilege is what refuses it before the trigger does.
func TestAuditLogOwnerHoldsNoDeletePrivilege(t *testing.T) {
	pool := pgtest.Pool(t)
	var held []string
	rows, err := pool.Query(context.Background(), `
		SELECT a.privilege_type
		  FROM pg_class c, aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
		 WHERE c.oid = 'control_plane.audit_log'::regclass AND a.grantee = c.relowner`)
	if err != nil {
		t.Fatalf("read acl: %v", err)
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		held = append(held, p)
	}
	for _, p := range held {
		if p == "DELETE" || p == "TRUNCATE" {
			t.Errorf("owner privileges = %v, want neither DELETE nor TRUNCATE", held)
		}
	}
	if len(held) == 0 {
		t.Fatal("owner holds no privilege at all: the query read nothing, so it proves nothing")
	}
}
