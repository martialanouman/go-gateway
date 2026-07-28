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

	entry := func(et cp.EntryType, mid *uuid.UUID, credits int) cp.LedgerEntry {
		return cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: mid, EntryType: et, Credits: credits,
		}
	}

	// topup +100 → the returned balance is the atomic post-adjustment value (applied, no message_id).
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryTopup, nil, 100)); err != nil || bal != 100 || !applied {
		t.Fatalf("record topup = (%d, %v, %v), want (100, true, nil)", bal, applied, err)
	}
	// reserve -3 → 97.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryReserve, &messageID, -3)); err != nil || bal != 97 || !applied {
		t.Fatalf("record reserve = (%d, %v, %v), want (97, true, nil)", bal, applied, err)
	}

	// Balance reflects the running sum of deltas.
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

	// Replaying the SAME reserve is idempotent: no second debit, no second row, applied=false, balance held.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryReserve, &messageID, -3)); err != nil || bal != 97 || applied {
		t.Fatalf("replay reserve = (%d, %v, %v), want (97, false, nil) — idempotent, no double debit", bal, applied, err)
	}

	// Capture confirms the reservation with credits=0: the reserve already debited, so the balance is
	// unchanged (delta 0 → balance stays 97) and the ledger stays self-consistent.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryCapture, &messageID, 0)); err != nil || bal != 97 || !applied {
		t.Fatalf("record capture = (%d, %v, %v), want (97, true, nil)", bal, applied, err)
	}
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryCapture); err != nil || !ok {
		t.Fatalf("LedgerEntryExists(capture, post) = (%v, %v), want (true, nil)", ok, err)
	}
	if credits, _, _ := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); credits != 97 {
		t.Errorf("Balance after capture = %d, want 97 (capture must not double-debit)", credits)
	}

	// Append-only, and the idempotent replay added NOTHING: 3 entries for this owner (topup, reserve,
	// capture) — the second reserve was a no-op, not a fourth row.
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		customerID).Scan(&rows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if rows != 3 {
		t.Errorf("ledger rows = %d, want 3 (topup, reserve, capture — replay added no row)", rows)
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
