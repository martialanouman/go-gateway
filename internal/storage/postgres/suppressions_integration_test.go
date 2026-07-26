package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

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
