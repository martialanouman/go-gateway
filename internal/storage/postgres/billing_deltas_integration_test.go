package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

type deltaFixture struct {
	pool       *pgxpool.Pool
	repo       *postgres.BillingRepo
	customerID uuid.UUID
}

func newDeltaFixture(t *testing.T, scope string) deltaFixture {
	t.Helper()
	pool := pgtest.Pool(t)
	var customerID uuid.UUID
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO control_plane.customers (name, balance_scope) VALUES ('billing-deltas', $1) RETURNING id`,
		scope).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	return deltaFixture{pool: pool, repo: postgres.NewBillingRepo(pool), customerID: customerID}
}

func (f deltaFixture) account(t *testing.T) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := f.pool.QueryRow(context.Background(),
		`INSERT INTO control_plane.smpp_accounts (customer_id, name) VALUES ($1, $2) RETURNING id`,
		f.customerID, uuid.NewString()).Scan(&id); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	return id
}

func (f deltaFixture) entry(ownerType string, ownerID uuid.UUID, et cp.EntryType, credits int) cp.LedgerEntry {
	e := cp.LedgerEntry{
		OwnerType: ownerType, OwnerID: ownerID, Direction: cp.BillingDirectionMT,
		CustomerID: f.customerID, EntryType: et, Credits: credits,
	}
	if ownerType == cp.OwnerTypeSMPPAccount {
		e.AccountID = &ownerID
	}
	return e
}

// hotPath records a movement the way the Accountant does: a fresh message, a delta, no balances write.
func (f deltaFixture) hotPath(t *testing.T, ownerType string, ownerID uuid.UUID, et cp.EntryType, credits int) {
	t.Helper()
	e := f.entry(ownerType, ownerID, et, credits)
	messageID := uuid.New()
	e.MessageID = &messageID
	if _, _, err := f.repo.RecordDurable(context.Background(), e); err != nil {
		t.Fatalf("record %s: %v", et, err)
	}
}

func (f deltaFixture) topup(t *testing.T, ownerType string, ownerID uuid.UUID, credits int) cp.LedgerRow {
	t.Helper()
	row, _, err := f.repo.Topup(context.Background(), f.entry(ownerType, ownerID, cp.EntryTopup, credits))
	if err != nil {
		t.Fatalf("topup: %v", err)
	}
	return row
}

// TestTransferOverdrawGuardSeesPendingDeltas: 8 of the source's 10 credits are reserved but not yet folded.
// A guard reading the folded balance alone would let a transfer of 5 overdraw the source.
func TestTransferOverdrawGuardSeesPendingDeltas(t *testing.T) {
	f := newDeltaFixture(t, cp.OwnerTypeSMPPAccount)
	src, dst := f.account(t), f.account(t)
	f.topup(t, cp.OwnerTypeSMPPAccount, src, 10)
	f.hotPath(t, cp.OwnerTypeSMPPAccount, src, cp.EntryReserve, -8)

	debit := f.entry(cp.OwnerTypeSMPPAccount, src, cp.EntryTransfer, -5)
	credit := f.entry(cp.OwnerTypeSMPPAccount, dst, cp.EntryTransfer, 5)
	if _, _, err := f.repo.Transfer(context.Background(), debit, credit, uuid.New()); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Transfer(5 of 2 available) = %v, want ErrInsufficientCredit", err)
	}
}

// TestAdminBalanceAfterIncludesPendingDeltas: an admin movement reports the balance the customer really has,
// unfolded reserves included, not the folded row it adjusted.
func TestAdminBalanceAfterIncludesPendingDeltas(t *testing.T) {
	f := newDeltaFixture(t, cp.OwnerTypeSMPPAccount)
	src, dst := f.account(t), f.account(t)
	f.topup(t, cp.OwnerTypeSMPPAccount, src, 10)
	f.hotPath(t, cp.OwnerTypeSMPPAccount, src, cp.EntryReserve, -3)

	if row := f.topup(t, cp.OwnerTypeSMPPAccount, src, 1); row.BalanceAfter != 8 {
		t.Errorf("topup balance_after = %d, want 8 (10 - 3 + 1)", row.BalanceAfter)
	}
	debit := f.entry(cp.OwnerTypeSMPPAccount, src, cp.EntryTransfer, -5)
	credit := f.entry(cp.OwnerTypeSMPPAccount, dst, cp.EntryTransfer, 5)
	rows, _, err := f.repo.Transfer(context.Background(), debit, credit, uuid.New())
	if err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	if len(rows) != 2 || rows[0].BalanceAfter != 3 || rows[1].BalanceAfter != 5 {
		t.Errorf("transfer legs = %+v, want source balance_after 3 and destination 5", rows)
	}
}

// TestChangeScopeRefusesPendingDeltas: the owner has no balances row at all, only an unfolded refund. Its
// credit is real; flipping the scope would strand it under an owner nothing reads again.
func TestChangeScopeRefusesPendingDeltas(t *testing.T) {
	f := newDeltaFixture(t, cp.OwnerTypeCustomer)
	f.hotPath(t, cp.OwnerTypeCustomer, f.customerID, cp.EntryRelease, 3)

	owner := cp.BalanceOwner{OwnerType: cp.OwnerTypeCustomer, OwnerID: f.customerID}
	err := f.repo.ChangeBalanceScope(context.Background(), f.customerID, []cp.BalanceOwner{owner}, cp.OwnerTypeSMPPAccount)
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("ChangeBalanceScope with 3 pending credits = %v, want ErrConflict", err)
	}
}
