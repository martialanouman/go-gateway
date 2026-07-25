package modlrrouter_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestMOResolutionAgainstPostgres proves the router resolves against a real control plane: a dedicated
// number routes to its account, a shared number routes by keyword, and an unknown number is persisted
// to the unrouted queue.
func TestMOResolutionAgainstPostgres(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	numbers := postgres.NewInboundNumberRepo(pool)
	keywords := postgres.NewInboundKeywordRepo(pool)
	unrouted := postgres.NewUnroutedMORepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "MOResolutionCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	dedicatedAcct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "mo-dedicated"})
	if err != nil {
		t.Fatalf("create dedicated account: %v", err)
	}
	keywordAcct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "mo-keyword"})
	if err != nil {
		t.Fatalf("create keyword account: %v", err)
	}

	// A dedicated number, and a shared number with a keyword.
	dedicated, err := numbers.Create(ctx, cp.NewInboundNumber{
		Address: "40100", NumberType: cp.NumberShortcode, CountryCode: "FR", AccountID: &dedicatedAcct.ID,
	})
	if err != nil {
		t.Fatalf("create dedicated number: %v", err)
	}
	shared, err := numbers.Create(ctx, cp.NewInboundNumber{
		Address: "40200", NumberType: cp.NumberShortcode, CountryCode: "FR",
	})
	if err != nil {
		t.Fatalf("create shared number: %v", err)
	}
	if _, err := keywords.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: shared.ID, Keyword: "INFO", MatchType: cp.MatchPrefix, AccountID: keywordAcct.ID,
	}); err != nil {
		t.Fatalf("create keyword: %v", err)
	}

	snap, err := modlrrouter.LoadSnapshot(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), numbers, keywords, accounts)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	// Dedicated number -> its account.
	prod := &fakeProducer{}
	if _, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: unrouted},
		moRecord(t, uuid.New(), "22507000001", dedicated.Address, "hello")); err != nil {
		t.Fatalf("run dedicated: %v", err)
	}
	if len(prod.recs) != 1 {
		t.Fatalf("dedicated: want 1 routed record, got %d", len(prod.recs))
	}
	routed, _ := pipeline.DecodeMORouted(prod.recs[0])
	if routed.AccountID != dedicatedAcct.ID || routed.CustomerID != customer.ID {
		t.Errorf("dedicated routed to %s/%s, want %s/%s", routed.AccountID, routed.CustomerID, dedicatedAcct.ID, customer.ID)
	}

	// Shared number + keyword -> the keyword's account.
	prod = &fakeProducer{}
	if _, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: unrouted},
		moRecord(t, uuid.New(), "22507000001", shared.Address, "info please")); err != nil {
		t.Fatalf("run shared: %v", err)
	}
	if len(prod.recs) != 1 {
		t.Fatalf("shared: want 1 routed record, got %d", len(prod.recs))
	}
	routed, _ = pipeline.DecodeMORouted(prod.recs[0])
	if routed.AccountID != keywordAcct.ID {
		t.Errorf("shared+keyword routed to %s, want %s", routed.AccountID, keywordAcct.ID)
	}

	// Unknown number -> persisted to the unrouted queue.
	prod = &fakeProducer{}
	if _, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: unrouted, Metric: &fakeMetric{}},
		moRecord(t, uuid.New(), "22507000001", "49999", "hello")); err != nil {
		t.Fatalf("run unknown: %v", err)
	}
	if len(prod.recs) != 0 {
		t.Errorf("unknown: want 0 routed, got %d", len(prod.recs))
	}
	list, err := unrouted.List(ctx, 10, nil)
	if err != nil {
		t.Fatalf("list unrouted: %v", err)
	}
	var found bool
	for _, u := range list {
		if u.DestAddr == "49999" && u.Reason == cp.UnroutedUnknownNumber {
			found = true
		}
	}
	if !found {
		t.Errorf("unknown MO not persisted to unrouted queue: %+v", list)
	}
}
