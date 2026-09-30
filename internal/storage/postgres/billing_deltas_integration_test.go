package postgres_test

import (
	"context"
	"errors"
	"sync"
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

func (f deltaFixture) folded(t *testing.T, ownerType string, ownerID uuid.UUID) (credits, pending int) {
	t.Helper()
	ctx := context.Background()
	if err := f.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT credits FROM control_plane.balances WHERE owner_type = $1 AND owner_id = $2 AND direction = 'mt'), 0)`,
		ownerType, ownerID).Scan(&credits); err != nil {
		t.Fatalf("read balances row: %v", err)
	}
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.balance_deltas WHERE owner_type = $1 AND owner_id = $2`,
		ownerType, ownerID).Scan(&pending); err != nil {
		t.Fatalf("count deltas: %v", err)
	}
	return credits, pending
}

func (f deltaFixture) foldAll(t *testing.T) {
	t.Helper()
	for range 100 {
		n, err := f.repo.FoldOnce(context.Background(), 5000)
		if err != nil {
			t.Fatalf("FoldOnce: %v", err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("FoldOnce never drained the deltas: a fold that does not remove what it folds")
}

func (f deltaFixture) durable(t *testing.T, ownerType string, ownerID uuid.UUID) int {
	t.Helper()
	bal, _, err := f.repo.Balance(context.Background(), ownerType, ownerID, cp.BillingDirectionMT)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	return bal
}

// TestFoldOnceMovesDeltasWithoutChangingTheBalance: folding changes where the credit lives, never how much
// there is — before, during (a partial batch) and after.
func TestFoldOnceMovesDeltasWithoutChangingTheBalance(t *testing.T) {
	f := newDeltaFixture(t, cp.OwnerTypeSMPPAccount)
	f.foldAll(t)
	a, b := f.account(t), f.account(t)
	f.topup(t, cp.OwnerTypeSMPPAccount, a, 20)
	for _, c := range []int{-1, -2, -3} {
		f.hotPath(t, cp.OwnerTypeSMPPAccount, a, cp.EntryReserve, c)
		f.hotPath(t, cp.OwnerTypeSMPPAccount, b, cp.EntryRelease, -c)
	}

	if n, err := f.repo.FoldOnce(context.Background(), 2); err != nil || n != 2 {
		t.Fatalf("FoldOnce(2) = (%d, %v), want (2, nil)", n, err)
	}
	if got := f.durable(t, cp.OwnerTypeSMPPAccount, a); got != 14 {
		t.Errorf("durable balance mid-fold = %d, want 14", got)
	}
	f.foldAll(t)

	for _, tc := range []struct {
		owner uuid.UUID
		want  int
	}{{a, 14}, {b, 6}} {
		credits, pending := f.folded(t, cp.OwnerTypeSMPPAccount, tc.owner)
		if credits != tc.want || pending != 0 {
			t.Errorf("after fold: balances row %d with %d deltas pending, want %d with none", credits, pending, tc.want)
		}
		if got := f.durable(t, cp.OwnerTypeSMPPAccount, tc.owner); got != tc.want {
			t.Errorf("durable balance after fold = %d, want %d", got, tc.want)
		}
	}
	if _, found, err := f.repo.OldestPendingDelta(context.Background()); err != nil || found {
		t.Errorf("OldestPendingDelta on an empty table = (found %v, %v), want none", found, err)
	}
	f.hotPath(t, cp.OwnerTypeSMPPAccount, a, cp.EntryReserve, -1)
	if at, found, err := f.repo.OldestPendingDelta(context.Background()); err != nil || !found || at.IsZero() {
		t.Errorf("OldestPendingDelta = (%v, %v, %v), want a timestamp", at, found, err)
	}
}

// TestConcurrentFoldsLoseAndDuplicateNothing: several folders race writers on the same owner. Every delta
// must land in balances exactly once.
func TestConcurrentFoldsLoseAndDuplicateNothing(t *testing.T) {
	f := newDeltaFixture(t, cp.OwnerTypeCustomer)
	const writers, perWriter = 4, 50
	var wg sync.WaitGroup
	done := make(chan struct{})
	var foldErr error
	var foldMu sync.Mutex
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := f.repo.FoldOnce(context.Background(), 7); err != nil {
					foldMu.Lock()
					foldErr = err
					foldMu.Unlock()
					return
				}
			}
		}()
	}
	var writersWG sync.WaitGroup
	for range writers {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for range perWriter {
				e := f.entry(cp.OwnerTypeCustomer, f.customerID, cp.EntryRelease, 1)
				messageID := uuid.New()
				e.MessageID = &messageID
				if _, _, err := f.repo.RecordDurable(context.Background(), e); err != nil {
					t.Errorf("record: %v", err)
					return
				}
			}
		}()
	}
	writersWG.Wait()
	close(done)
	wg.Wait()
	if foldErr != nil {
		t.Fatalf("concurrent FoldOnce: %v", foldErr)
	}
	f.foldAll(t)

	credits, pending := f.folded(t, cp.OwnerTypeCustomer, f.customerID)
	if credits != writers*perWriter || pending != 0 {
		t.Errorf("balances row = %d with %d pending, want %d with none", credits, pending, writers*perWriter)
	}
}
