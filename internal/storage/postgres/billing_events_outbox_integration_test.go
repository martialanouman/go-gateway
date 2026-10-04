package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

func moCharge(customerID uuid.UUID, balanceAfter int, floor *int) cp.LedgerEntry {
	messageID := uuid.New()
	return cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMO,
		CustomerID: customerID, MessageID: &messageID, EntryType: cp.EntryMOCharge, Credits: -1,
		BalanceAfter: &balanceAfter, MOFloorReached: floor,
	}
}

func TestRecordDurableWritesTheFloorEventInTheSameTransaction(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-outbox-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	floor := -100
	entry := moCharge(customerID, -100, &floor)

	if _, applied, err := repo.RecordDurable(ctx, entry); err != nil || !applied {
		t.Fatalf("record mo_charge = (%v, %v), want applied", applied, err)
	}
	if _, applied, err := repo.RecordDurable(ctx, entry); err != nil || applied {
		t.Fatalf("replay mo_charge = (%v, %v), want not applied", applied, err)
	}

	var rows, gotBalance, gotFloor int
	var gotOwner, gotCustomer uuid.UUID
	var gotOwnerType string
	if err := pool.QueryRow(ctx,
		`SELECT count(*) OVER (), owner_type, owner_id, customer_id, balance_after, floor
		   FROM control_plane.billing_events_outbox WHERE customer_id = $1`, customerID).
		Scan(&rows, &gotOwnerType, &gotOwner, &gotCustomer, &gotBalance, &gotFloor); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if rows != 1 {
		t.Errorf("outbox rows = %d, want 1: the replay must not add a second event", rows)
	}
	if gotOwnerType != cp.OwnerTypeCustomer || gotOwner != customerID || gotCustomer != customerID ||
		gotBalance != -100 || gotFloor != floor {
		t.Errorf("outbox row = (%s, %s, %s, %d, %d), want (customer, %s, %s, -100, %d)",
			gotOwnerType, gotOwner, gotCustomer, gotBalance, gotFloor, customerID, customerID, floor)
	}
}

// TestBatcherKeepsTheFloorEvent: the crossing mo_charge carries a message and a Redis balance, so it is
// batchable — yet the batch writes no outbox. billing-svc records through the Batcher, so a crossing it
// grouped would be charged and never announced.
func TestBatcherKeepsTheFloorEvent(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	batcher := postgres.NewBillingBatcher(postgres.NewBillingRepo(pool), &batchSizes{})
	t.Cleanup(batcher.Close)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-outbox-batch') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	floor := -10
	if _, applied, err := batcher.RecordDurable(ctx, moCharge(customerID, -12, &floor)); err != nil || !applied {
		t.Fatalf("record crossing through the batcher = (%v, %v), want applied", applied, err)
	}
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_events_outbox WHERE customer_id = $1`, customerID).Scan(&rows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if rows != 1 {
		t.Errorf("outbox rows = %d, want 1: the crossing must not lose its event to the batch", rows)
	}
}

func TestRecordDurableWritesNoEventWithoutACrossing(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-outbox-none') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, _, err := repo.RecordDurable(ctx, moCharge(customerID, -1, nil)); err != nil {
		t.Fatalf("record mo_charge: %v", err)
	}
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_events_outbox WHERE customer_id = $1`, customerID).Scan(&rows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if rows != 0 {
		t.Errorf("outbox rows = %d, want 0 without a crossing", rows)
	}
}

func TestRelayBillingEventsDeletesOnlyWhatWasPublished(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-outbox-relay') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	floor := -10
	if _, _, err := repo.RecordDurable(ctx, moCharge(customerID, -12, &floor)); err != nil {
		t.Fatalf("record mo_charge: %v", err)
	}
	pending := func() int {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM control_plane.billing_events_outbox WHERE customer_id = $1`, customerID).Scan(&n); err != nil {
			t.Fatalf("count outbox: %v", err)
		}
		return n
	}

	brokerDown := errors.New("broker down")
	failing := func(context.Context, []cp.BillingEvent) error { return brokerDown }
	if _, err := repo.RelayBillingEvents(ctx, 100, failing); !errors.Is(err, brokerDown) {
		t.Fatalf("relay with a failing publish = %v, want %v", err, brokerDown)
	}
	if pending() != 1 {
		t.Fatalf("outbox rows after a failed publish = %d, want 1: an unpublished event must stay", pending())
	}

	var mine []cp.BillingEvent
	n, err := repo.RelayBillingEvents(ctx, 100, func(_ context.Context, evs []cp.BillingEvent) error {
		for _, e := range evs {
			if e.CustomerID == customerID {
				mine = append(mine, e)
			}
		}
		// A produce can hang on a degraded broker: no row lock may outlive the read.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx,
			`SELECT 1 FROM control_plane.billing_events_outbox WHERE customer_id = $1 FOR UPDATE NOWAIT`, customerID); err != nil {
			t.Errorf("another session cannot lock the event while it is being published: %v", err)
		}
		if _, _, err := repo.RecordDurable(ctx, moCharge(customerID, -13, &floor)); err != nil {
			t.Errorf("queue a crossing during the publish: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	if n < 1 || len(mine) != 1 {
		t.Fatalf("relay published %d events (%d for this customer), want this customer's one", n, len(mine))
	}
	if e := mine[0]; e.ID == uuid.Nil || e.OwnerType != cp.OwnerTypeCustomer || e.OwnerID != customerID ||
		e.BalanceAfter != -12 || e.Floor != floor || e.CreatedAt.IsZero() {
		t.Errorf("published event = %+v, want customer %s at -12, floor %d, with an id and a time", e, customerID, floor)
	}
	if pending() != 1 {
		t.Errorf("outbox rows after the relay = %d, want 1: only the published event leaves, the one queued meanwhile waits", pending())
	}
}
