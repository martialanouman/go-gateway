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

// TestSuppressionAdminRepo exercises the Admin write/read surface against real Postgres: a returning
// create (409 on duplicate), a filtered paginated list, an idempotent bulk import, and delete-by-id.
func TestSuppressionAdminRepo(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewSuppressionRepo(pool)

	inbID := uuid.New()
	created, err := repo.CreateReturning(ctx, cp.NewSuppression{
		Scope: cp.SuppressionScopeInboundNumber, ScopeID: &inbID, MSISDN: "2250700000001", Source: cp.SuppressionSourceAdmin,
	})
	if err != nil {
		t.Fatalf("CreateReturning: %v", err)
	}

	// A duplicate is a conflict (unlike the idempotent MO STOP write).
	if _, err := repo.CreateReturning(ctx, cp.NewSuppression{
		Scope: cp.SuppressionScopeInboundNumber, ScopeID: &inbID, MSISDN: "2250700000001", Source: cp.SuppressionSourceAdmin,
	}); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("duplicate create = %v, want ErrConflict", err)
	}

	// Bulk import is idempotent: the already-present number is skipped, the new one inserted.
	inserted, err := repo.Import(ctx, cp.SuppressionScopeInboundNumber, &inbID, cp.SuppressionSourceImport,
		[]string{"2250700000001", "2250700000002", "2250700000003"})
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if inserted != 2 {
		t.Errorf("import inserted %d, want 2 (one was a duplicate)", inserted)
	}

	// Filtered list on the inbound_number scope returns all three.
	page, err := repo.ListPage(ctx, cp.SuppressionFilter{Scope: ptrScope(cp.SuppressionScopeInboundNumber), ScopeID: &inbID, Limit: 50})
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if len(page.Items) != 3 {
		t.Errorf("list returned %d, want 3", len(page.Items))
	}

	// Delete by id, then a second delete is ErrNotFound.
	if err := repo.DeleteByID(ctx, created.ID); err != nil {
		t.Fatalf("DeleteByID: %v", err)
	}
	if err := repo.DeleteByID(ctx, created.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
}

func ptrScope(s cp.SuppressionScope) *cp.SuppressionScope { return &s }

// TestOptOutKeywordRepo exercises the opt-out keyword CRUD against real Postgres.
func TestOptOutKeywordRepo(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewOptOutKeywordRepo(pool)

	tmpl := "You are unsubscribed."
	k, err := repo.Create(ctx, cp.NewOptOutKeyword{Keyword: "STOP", Action: cp.OptOutActionSuppress, AutoReplyTemplate: &tmpl})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if k.MatchType != cp.OptOutMatchExact || k.Status != cp.OptOutKeywordActive {
		t.Errorf("create defaults = %s/%s, want exact/active", k.MatchType, k.Status)
	}

	disabled := cp.OptOutKeywordDisabled
	updated, err := repo.Update(ctx, k.ID, cp.OptOutKeywordPatch{Status: &disabled})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Status != cp.OptOutKeywordDisabled {
		t.Errorf("updated status = %s, want disabled", updated.Status)
	}

	// A disabled keyword is excluded from ListActive but present in List.
	active, _ := repo.ListActive(ctx)
	if len(active) != 0 {
		t.Errorf("ListActive returned %d, want 0 (the keyword is disabled)", len(active))
	}
	all, _ := repo.List(ctx)
	if len(all) != 1 {
		t.Errorf("List returned %d, want 1", len(all))
	}

	if err := repo.Delete(ctx, k.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, k.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
}

// TestSuppressionWriteRoundTrip proves the write side against real Postgres: a STOP creates a
// suppression, a repeated STOP is idempotent (no duplicate, per suppressions_uq), IsSuppressed
// confirms it, and an unsuppress removes it.
func TestSuppressionWriteRoundTrip(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewSuppressionRepo(pool)

	inbID := uuid.New()
	const msisdn = "2250700000001"
	in := cp.NewSuppression{
		Scope:   cp.SuppressionScopeInboundNumber,
		ScopeID: &inbID,
		MSISDN:  msisdn,
		Source:  cp.SuppressionSourceMOStop,
	}

	created, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !created {
		t.Fatal("first STOP should report created=true")
	}

	// A repeated STOP is idempotent: no duplicate, created=false.
	again, err := repo.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create (repeat): %v", err)
	}
	if again {
		t.Error("a repeated STOP must not create a duplicate (created=false)")
	}

	// IsSuppressed confirms it in the inbound_number scope.
	ok, err := repo.IsSuppressed(ctx, cp.SuppressionScopeInboundNumber, &inbID, msisdn)
	if err != nil {
		t.Fatalf("IsSuppressed: %v", err)
	}
	if !ok {
		t.Error("the written suppression must confirm as suppressed")
	}

	// An unsuppress (START) removes it.
	removed, err := repo.DeleteByKey(ctx, cp.SuppressionScopeInboundNumber, &inbID, msisdn)
	if err != nil {
		t.Fatalf("DeleteByKey: %v", err)
	}
	if !removed {
		t.Error("DeleteByKey should report removed=true")
	}
	if ok, _ := repo.IsSuppressed(ctx, cp.SuppressionScopeInboundNumber, &inbID, msisdn); ok {
		t.Error("after unsuppress, the number must no longer be suppressed")
	}
}
