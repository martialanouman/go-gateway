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

// TestBillingRepoOrphanedReservations proves the reaper's detection query (step-190): a reservation is
// orphaned when its message has a `reserve` claim but NEITHER a capture NOR a release, i.e. the settle
// loop failed open and nothing ever closed the money. Detection reads control_plane.billing_idempotency
// — the partition-free authoritative guard — because the ledger's own idempotency index cannot span day
// partitions and would miss a reservation settled across midnight.
func TestBillingRepoOrphanedReservations(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('reaper-orphan-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	entry := func(et cp.EntryType, mid *uuid.UUID, credits int) cp.LedgerEntry {
		return cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: mid, EntryType: et, Credits: credits,
		}
	}
	record := func(et cp.EntryType, mid *uuid.UUID, credits int) {
		t.Helper()
		if _, applied, err := repo.RecordDurable(ctx, entry(et, mid, credits)); err != nil || !applied {
			t.Fatalf("record %s: applied=%v err=%v", et, applied, err)
		}
	}

	// Fund the customer so every reserve below has credit to move.
	record(cp.EntryTopup, nil, 1000)

	orphan := uuid.New()   // reserve only          → orphaned
	captured := uuid.New() // reserve + capture     → settled
	released := uuid.New() // reserve + release     → settled
	record(cp.EntryReserve, &orphan, -7)
	record(cp.EntryReserve, &captured, -3)
	record(cp.EntryCapture, &captured, 0)
	record(cp.EntryReserve, &released, -5)
	record(cp.EntryRelease, &released, 5)

	// A future cutoff makes every seeded reservation "old enough" to be swept.
	got, err := repo.OrphanedReservations(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("OrphanedReservations: %v", err)
	}

	byID := make(map[uuid.UUID]cp.OrphanedReservation, len(got))
	for _, o := range got {
		byID[o.MessageID] = o
	}
	if _, ok := byID[captured]; ok {
		t.Error("a captured reservation was reported as orphaned — the reaper would double-settle it")
	}
	if _, ok := byID[released]; ok {
		t.Error("a released reservation was reported as orphaned — the reaper would refund it twice")
	}

	o, ok := byID[orphan]
	if !ok {
		t.Fatalf("the unsettled reservation %s was not reported; got %d orphan(s)", orphan, len(got))
	}
	if o.OwnerType != cp.OwnerTypeCustomer || o.OwnerID != customerID || o.CustomerID != customerID {
		t.Errorf("owner = %s/%s (customer %s), want customer/%s", o.OwnerType, o.OwnerID, o.CustomerID, customerID)
	}
	// The signed reserve delta is what the reaper needs to reason about the amount at stake.
	if o.Credits != -7 {
		t.Errorf("Credits = %d, want -7 (the signed reserve delta)", o.Credits)
	}
	if o.ReservedAt.IsZero() {
		t.Error("ReservedAt is zero — the reaper cannot tell how long the money has been stuck")
	}
}

// TestBillingRepoOrphanedReservationsRespectsAgeCutoff proves the guard that keeps the reaper off the
// nominal path: a reservation younger than the cutoff is NOT swept. Without it the reaper would race the
// connector pool and settle messages that are still legitimately in flight.
func TestBillingRepoOrphanedReservationsRespectsAgeCutoff(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('reaper-cutoff-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	messageID := uuid.New()
	for _, e := range []cp.LedgerEntry{
		{OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, EntryType: cp.EntryTopup, Credits: 100},
		{OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: &messageID, EntryType: cp.EntryReserve, Credits: -4},
	} {
		if _, applied, err := repo.RecordDurable(ctx, e); err != nil || !applied {
			t.Fatalf("record %s: applied=%v err=%v", e.EntryType, applied, err)
		}
	}

	// Cutoff in the past: the just-written reservation is younger, so nothing is due.
	got, err := repo.OrphanedReservations(ctx, time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("OrphanedReservations: %v", err)
	}
	for _, o := range got {
		if o.MessageID == messageID {
			t.Fatal("a reservation younger than the cutoff was swept — the reaper would race the nominal settle path")
		}
	}
}
