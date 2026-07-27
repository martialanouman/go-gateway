package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// cleanExactRoutes empties the table so the shared-pool tests are isolated from one another.
func cleanExactRoutes(t *testing.T) {
	t.Helper()
	pool := pgtest.Pool(t)
	if _, err := pool.Exec(context.Background(), "DELETE FROM control_plane.exact_routes"); err != nil {
		t.Fatalf("clean exact_routes: %v", err)
	}
}

// TestExactRouteRepoUpsertIsIdempotent: upserting the same msisdn twice overwrites the target and keeps
// a single row (the primary key is the msisdn).
func TestExactRouteRepoUpsertIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cleanExactRoutes(t)
	repo := postgres.NewExactRouteRepo(pgtest.Pool(t))

	first, second := uuid.New(), uuid.New()
	if _, err := repo.Upsert(ctx, exact.Route{
		MSISDN: "+2250700000001", Target: exact.Target{Type: exact.TargetConnector, ID: first}, Source: exact.SourceManual,
	}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// Re-upsert the same number to a different target.
	got, err := repo.Upsert(ctx, exact.Route{
		MSISDN: "+2250700000001", Target: exact.Target{Type: exact.TargetRoute, ID: second}, Source: exact.SourceCarrierFeed,
	})
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if got.Target.Type != exact.TargetRoute || got.Target.ID != second || got.Source != exact.SourceCarrierFeed {
		t.Errorf("upserted route = %+v, want the second target (route %s, carrier_feed)", got, second)
	}

	// Exactly one row: Get returns the latest, and a page holds just it.
	back, found, err := repo.Get(ctx, "+2250700000001")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if back.Target.ID != second {
		t.Errorf("get target = %s, want the overwritten %s", back.Target.ID, second)
	}
	if page, err := repo.List(ctx, "", 10); err != nil || len(page) != 1 {
		t.Fatalf("list after two upserts of one msisdn: len=%d err=%v, want 1 (idempotent)", len(page), err)
	}
}

// TestExactRouteRepoDeleteAndGetMissing: Get and Delete distinguish present from absent — a missing
// MSISDN is the "no override" state (found=false, no error), the L0 short-cut's normal fall-through.
func TestExactRouteRepoDeleteAndGetMissing(t *testing.T) {
	ctx := context.Background()
	cleanExactRoutes(t)
	repo := postgres.NewExactRouteRepo(pgtest.Pool(t))

	// Get of an unconfigured number: not found, no error.
	if _, found, err := repo.Get(ctx, "+2250799999999"); err != nil || found {
		t.Fatalf("get missing = (found %v, err %v), want (false, nil)", found, err)
	}
	// Delete of an unconfigured number: not found, no error.
	if found, err := repo.Delete(ctx, "+2250799999999"); err != nil || found {
		t.Fatalf("delete missing = (found %v, err %v), want (false, nil)", found, err)
	}

	// Configure one, then delete it: found=true, and it is gone afterwards.
	if _, err := repo.Upsert(ctx, exact.Route{
		MSISDN: "+2250700000009", Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()}, Source: exact.SourceManual,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if found, err := repo.Delete(ctx, "+2250700000009"); err != nil || !found {
		t.Fatalf("delete existing = (found %v, err %v), want (true, nil)", found, err)
	}
	if _, found, err := repo.Get(ctx, "+2250700000009"); err != nil || found {
		t.Errorf("get after delete = (found %v, err %v), want (false, nil)", found, err)
	}
}

// TestExactRouteRepoListPaginates: List returns msisdn-ordered pages, and the keyset cursor covers
// every row exactly once.
func TestExactRouteRepoListPaginates(t *testing.T) {
	ctx := context.Background()
	cleanExactRoutes(t)
	repo := postgres.NewExactRouteRepo(pgtest.Pool(t))

	const n = 5
	for i := 0; i < n; i++ {
		if _, err := repo.Upsert(ctx, exact.Route{
			MSISDN: fmt.Sprintf("+22507000000%02d", i),
			Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()}, Source: exact.SourceManual,
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	seen := map[string]bool{}
	after := ""
	var last string
	for {
		page, err := repo.List(ctx, after, 2)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			if r.MSISDN <= last && last != "" {
				t.Errorf("page not in msisdn order: %q after %q", r.MSISDN, last)
			}
			if seen[r.MSISDN] {
				t.Errorf("msisdn %q seen twice across pages", r.MSISDN)
			}
			seen[r.MSISDN] = true
			last = r.MSISDN
		}
		after = page[len(page)-1].MSISDN
	}
	if len(seen) != n {
		t.Errorf("paged %d rows, want %d", len(seen), n)
	}
}

// TestExactRouteRepoBulkUpsert: a bulk import inserts N rows in one batch, and re-importing is
// idempotent (no duplicates, no error).
func TestExactRouteRepoBulkUpsert(t *testing.T) {
	ctx := context.Background()
	cleanExactRoutes(t)
	repo := postgres.NewExactRouteRepo(pgtest.Pool(t))

	importedAt := time.Now().UTC().Truncate(time.Second)
	const n = 20
	routes := make([]exact.Route, n)
	for i := range routes {
		routes[i] = exact.Route{
			MSISDN: fmt.Sprintf("+22507000001%02d", i),
			Target: exact.Target{Type: exact.TargetConnector, ID: uuid.New()},
			Source: exact.SourceMNPImport, ImportedAt: &importedAt,
		}
	}
	if err := repo.BulkUpsert(ctx, routes); err != nil {
		t.Fatalf("bulk upsert: %v", err)
	}
	if err := repo.BulkUpsert(ctx, routes); err != nil {
		t.Fatalf("re-import (idempotent) failed: %v", err)
	}

	all, err := repo.List(ctx, "", n+1)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != n {
		t.Fatalf("after two identical bulk imports: %d rows, want %d (idempotent)", len(all), n)
	}
	if all[0].Source != exact.SourceMNPImport || all[0].ImportedAt == nil || !all[0].ImportedAt.Equal(importedAt) {
		t.Errorf("imported row = {source:%q imported_at:%v}, want mnp_import at %s", all[0].Source, all[0].ImportedAt, importedAt)
	}

	// An empty bulk is a no-op.
	if err := repo.BulkUpsert(ctx, nil); err != nil {
		t.Errorf("empty bulk upsert = %v, want nil (no-op)", err)
	}
}
