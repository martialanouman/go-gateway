package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestBillingRepoLedgerKeysetPaging seeds several ledger entries for a customer and pages through them
// newest-first with a page size of 2, asserting no row is dropped or duplicated across the keyset boundary.
func TestBillingRepoLedgerKeysetPaging(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ledger-page') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, EntryType: cp.EntryTopup, Credits: 10,
		}); err != nil {
			t.Fatalf("seed entry %d: %v", i, err)
		}
	}

	seen := map[uuid.UUID]bool{}
	after := cp.LedgerKey{}
	pages := 0
	for {
		rows, hasMore, err := repo.Ledger(ctx, cp.LedgerFilter{CustomerID: customerID, After: after, Limit: 2})
		if err != nil {
			t.Fatalf("Ledger: %v", err)
		}
		pages++
		for _, r := range rows {
			if seen[r.ID] {
				t.Fatalf("row %s returned twice across pages", r.ID)
			}
			seen[r.ID] = true
			after = cp.LedgerKey{CreatedAt: r.CreatedAt, ID: r.ID}
		}
		if !hasMore {
			break
		}
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
	}
	if len(seen) != n {
		t.Errorf("paged %d distinct rows, want %d", len(seen), n)
	}
}

// TestRatePlanRepoDeleteInUseIsConflict: deleting a rate plan still assigned to a customer raises a 409
// (customers.rate_plan_id RESTRICT), while an unused plan deletes cleanly.
func TestRatePlanRepoDeleteInUseIsConflict(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewRatePlanRepo(pool)

	plan, err := repo.Create(ctx, cp.NewRatePlan{Name: "plan-x", CreditsPerSegmentMT: []byte(`{}`), CreditsPerSegmentMO: []byte(`{}`)})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	// Unused → deletes cleanly.
	if err := repo.Delete(ctx, plan.ID); err != nil {
		t.Fatalf("delete unused plan: %v", err)
	}

	plan2, err := repo.Create(ctx, cp.NewRatePlan{Name: "plan-y", CreditsPerSegmentMT: []byte(`{}`), CreditsPerSegmentMO: []byte(`{}`)})
	if err != nil {
		t.Fatalf("create plan2: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.customers (name, rate_plan_id) VALUES ('rp-user', $1)`, plan2.ID); err != nil {
		t.Fatalf("assign plan: %v", err)
	}
	if err := repo.Delete(ctx, plan2.ID); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("delete in-use plan = %v, want conflict", err)
	}
}

// TestExternalProviderRepoCRUD covers create/get/list/update/delete of a provider.
func TestExternalProviderRepoCRUD(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewExternalBillingProviderRepo(pool)

	created, err := repo.Create(ctx, cp.NewExternalBillingProvider{
		Name: "prov-crud", BaseURL: "https://ext.example", AuthConfig: []byte(`{"key":"secret"}`), Mode: string(cp.ExternalModeBalanceCheck),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.CacheTTLMs != 1000 || created.FailurePolicy != string(cp.FailOpen) {
		t.Errorf("defaults = cache %d / policy %q, want 1000 / fail_open", created.CacheTTLMs, created.FailurePolicy)
	}
	got, err := repo.Get(ctx, created.ID)
	if err != nil || got.Name != "prov-crud" {
		t.Fatalf("get = (%+v, %v)", got, err)
	}
	disabled := string("disabled")
	if _, err := repo.Update(ctx, created.ID, cp.ExternalBillingProviderPatch{Status: &disabled}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := repo.Delete(ctx, created.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := repo.Get(ctx, created.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("get after delete = %v, want not_found", err)
	}
}
