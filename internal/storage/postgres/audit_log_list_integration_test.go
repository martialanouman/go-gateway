package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestAuditLogListFiltersAndPages: newest first, filtered by operator and a [from, to) window, and paged
// by keyset without skipping or repeating a row — ties on at included, since id breaks them.
func TestAuditLogListFiltersAndPages(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewAuditLogRepo(pool)

	operator := "tok_" + uuid.NewString()
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ats := []time.Time{base, base.Add(time.Hour), base.Add(time.Hour), base.Add(2 * time.Hour), base.Add(3 * time.Hour)}
	for _, at := range ats {
		if _, err := pool.Exec(ctx, `INSERT INTO control_plane.audit_log (operator, operation_id, method, target, at)
			VALUES ($1, 'create-customer', 'POST', '/v1/admin/customers', $2)`, operator, at); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.audit_log (operator, operation_id, method, target, at)
		VALUES ($1, 'mt-replay', 'REPLAY', 'mt.dead-letter', $2)`, "declared:"+uuid.NewString(), base.Add(time.Hour)); err != nil {
		t.Fatalf("seed other operator: %v", err)
	}

	// A page of 3 ends between the two rows that share base+1h: only the id tie-break keeps the second.
	const pageSize = 3
	var got []cp.AuditEntry
	var after *cp.AuditLogKey
	for range 10 {
		page, err := repo.List(ctx, cp.AuditLogFilter{Operator: operator}, pageSize, after)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		got = append(got, page...)
		if len(page) < pageSize {
			break
		}
		last := page[len(page)-1]
		after = &cp.AuditLogKey{At: last.At, ID: last.ID}
	}
	if len(got) != len(ats) {
		t.Fatalf("paged %d rows, want the operator's %d", len(got), len(ats))
	}
	seen := map[uuid.UUID]bool{}
	for i, e := range got {
		if e.Operator != operator || e.OperationID != "create-customer" || e.Method != "POST" || e.Target != "/v1/admin/customers" {
			t.Errorf("row %d = %+v, want the seeded columns in their own fields", i, e)
		}
		if seen[e.ID] {
			t.Errorf("row %s returned twice", e.ID)
		}
		seen[e.ID] = true
		if i > 0 && e.At.After(got[i-1].At) {
			t.Errorf("row %d at %v is newer than row %d at %v, want newest first", i, e.At, i-1, got[i-1].At)
		}
	}

	from, to := base.Add(time.Hour), base.Add(3*time.Hour)
	window, err := repo.List(ctx, cp.AuditLogFilter{Operator: operator, From: &from, To: &to}, 50, nil)
	if err != nil {
		t.Fatalf("list window: %v", err)
	}
	if len(window) != 3 {
		t.Errorf("window [%v, %v) = %d rows, want 3 (from inclusive, to exclusive)", from, to, len(window))
	}
	for _, e := range window {
		if e.At.Before(from) || !e.At.Before(to) {
			t.Errorf("row at %v outside [%v, %v)", e.At, from, to)
		}
	}
}

// TestAuditLogListCarriesTheOutcome: status, finished_at and request_id reach the entry, NULL as nil.
func TestAuditLogListCarriesTheOutcome(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewAuditLogRepo(pool)
	operator := "tok_" + uuid.NewString()

	closed, err := repo.Begin(ctx, cp.AuditIntent{Operator: operator, OperationID: "delete-route", Method: "DELETE",
		Target: "/v1/admin/routes/x", RequestID: "req-315"})
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Finish(ctx, closed, 204); err != nil {
		t.Fatal(err)
	}
	open, err := repo.Begin(ctx, cp.AuditIntent{Operator: operator, OperationID: "delete-route", Method: "DELETE",
		Target: "/v1/admin/routes/y"})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := repo.List(ctx, cp.AuditLogFilter{Operator: operator}, 50, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	byID := map[uuid.UUID]cp.AuditEntry{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	c, o := byID[closed], byID[open]
	if c.Status == nil || *c.Status != 204 || c.FinishedAt == nil || c.RequestID == nil || *c.RequestID != "req-315" {
		t.Errorf("closed row = %+v, want status 204, a finished_at and request_id req-315", c)
	}
	if o.ID != open || o.Status != nil || o.FinishedAt != nil || o.RequestID != nil {
		t.Errorf("open row = %+v, want it listed with status, finished_at and request_id nil", o)
	}
}
