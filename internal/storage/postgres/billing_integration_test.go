package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestBillingRepoBalanceAndConfig proves the read side against real Postgres: an unseeded balance reads
// as absent (a valid zero), a configured billing customer maps every column (including the nullable
// overdraft/credit limits), and an unconfigured customer reads as not-found rather than an error.
func TestBillingRepoBalanceAndConfig(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-repo-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers
		   (customer_id, billing_mode, overdraft_enabled, overdraft_limit, credit_limit, credit_limit_is_hard)
		 VALUES ($1, 'prepaid', true, 500, 1000, true)`, customerID); err != nil {
		t.Fatalf("seed billing_customers: %v", err)
	}

	// Balance not yet recorded → absent (0, false), not an error.
	if credits, found, err := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); err != nil || found || credits != 0 {
		t.Fatalf("Balance(unseeded) = (%d, %v, %v), want (0, false, nil)", credits, found, err)
	}

	// Billing config maps every column, nullable limits included.
	bc, found, err := repo.BillingCustomer(ctx, customerID)
	if err != nil || !found {
		t.Fatalf("BillingCustomer = (found=%v, err=%v), want found", found, err)
	}
	if bc.BillingMode != cp.BillingPrepaid || !bc.OverdraftEnabled || !bc.CreditLimitIsHard {
		t.Errorf("config flags = %+v, want prepaid/overdraft/hard-limit", bc)
	}
	if bc.OverdraftLimit == nil || *bc.OverdraftLimit != 500 || bc.CreditLimit == nil || *bc.CreditLimit != 1000 {
		t.Errorf("limits = overdraft %v / credit %v, want 500 / 1000", bc.OverdraftLimit, bc.CreditLimit)
	}

	// An unconfigured customer reads as not-found, not an error.
	if _, found, err := repo.BillingCustomer(ctx, uuid.New()); err != nil || found {
		t.Fatalf("BillingCustomer(unconfigured) = (found=%v, err=%v), want (false, nil)", found, err)
	}
}

// TestBillingRepoRecordAndIdempotency proves the durable write path: RecordDurable appends the ledger AND
// reconciles the balance in one transaction, the ledger is append-only (every entry accumulates, none is
// updated), and LedgerEntryExists is the cross-partition idempotency guard the capture path reads.
func TestBillingRepoRecordAndIdempotency(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-ledger-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	messageID := uuid.New()

	entry := func(et cp.EntryType, mid *uuid.UUID, credits, balanceAfter int) cp.LedgerEntry {
		return cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: mid, EntryType: et, Credits: credits, BalanceAfter: balanceAfter,
		}
	}

	// topup +100 → 100, then reserve -3 → 97 for the message.
	if err := repo.RecordDurable(ctx, entry(cp.EntryTopup, nil, 100, 100)); err != nil {
		t.Fatalf("record topup: %v", err)
	}
	if err := repo.RecordDurable(ctx, entry(cp.EntryReserve, &messageID, -3, 97)); err != nil {
		t.Fatalf("record reserve: %v", err)
	}

	// Balance reconciled to the last balance_after.
	if credits, found, err := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); err != nil || !found || credits != 97 {
		t.Fatalf("Balance after reserve = (%d, %v, %v), want (97, true, nil)", credits, found, err)
	}

	// Pre-capture idempotency guard: reserve exists, capture does not yet.
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryReserve); err != nil || !ok {
		t.Fatalf("LedgerEntryExists(reserve) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryCapture); err != nil || ok {
		t.Fatalf("LedgerEntryExists(capture, pre) = (%v, %v), want (false, nil)", ok, err)
	}

	// Capture confirms the reservation with credits=0: the reserve already debited, so the balance is
	// unchanged and the ledger stays self-consistent (balance_after == prev + credits: 97 + 0 == 97).
	if err := repo.RecordDurable(ctx, entry(cp.EntryCapture, &messageID, 0, 97)); err != nil {
		t.Fatalf("record capture: %v", err)
	}
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryCapture); err != nil || !ok {
		t.Fatalf("LedgerEntryExists(capture, post) = (%v, %v), want (true, nil)", ok, err)
	}
	if credits, _, _ := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); credits != 97 {
		t.Errorf("Balance after capture = %d, want 97 (capture must not double-debit)", credits)
	}

	// Append-only: every RecordDurable added a row, none was updated — 3 entries for this owner.
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		customerID).Scan(&rows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if rows != 3 {
		t.Errorf("ledger rows = %d, want 3 (topup, reserve, capture — append-only)", rows)
	}

	// Audit invariant (§6.14): the balance can be reconstructed by summing the signed credits, so
	// SUM(credits) must equal the current balance (100 - 3 + 0 = 97).
	var sum int
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		customerID).Scan(&sum); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	if sum != 97 {
		t.Errorf("SUM(credits) = %d, want 97 (must equal the balance — ledger self-consistency)", sum)
	}
}
