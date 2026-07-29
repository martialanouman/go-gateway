package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// BillingRepo is the durable authority for billing: it reads and writes balances, reads billing
// configuration, and appends the append-only ledger (control_plane.balances / billing_customers /
// billing_ledger, §6.9). It holds no business logic — the atomic reserve/capture/release accounting is
// done in Redis Lua (step-142); this repo only persists the durable side that Redis caches, and provides
// the cross-partition idempotency check the Lua path consults.
type BillingRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewBillingRepo returns the billing repository backed by pool.
func NewBillingRepo(pool *pgxpool.Pool) *BillingRepo {
	return &BillingRepo{pool: pool, q: sqlcgen.New(pool)}
}

// Balance reads the durable owner balance for a direction (mt/mo). found=false means no balance row has
// ever been written for the owner — a legitimate "zero" state that the caller treats as 0, not an error.
func (r *BillingRepo) Balance(ctx context.Context, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error) {
	credits, err := r.q.GetBalance(ctx, sqlcgen.GetBalanceParams{
		OwnerType: ownerType, OwnerID: ownerID, Direction: direction,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, translate("get balance", err)
	}
	return int(credits), true, nil
}

// BillingCustomer reads a customer's MT billing configuration. found=false means billing is not
// configured for the customer (a valid "unconfigured" state), not an error.
func (r *BillingRepo) BillingCustomer(ctx context.Context, customerID uuid.UUID) (cp.BillingCustomer, bool, error) {
	row, err := r.q.GetBillingCustomer(ctx, customerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return cp.BillingCustomer{}, false, nil
	}
	if err != nil {
		return cp.BillingCustomer{}, false, translate("get billing customer", err)
	}
	return cp.BillingCustomer{
		CustomerID:                customerID,
		BillingMode:               cp.BillingMode(row.BillingMode),
		OverdraftEnabled:          row.OverdraftEnabled,
		OverdraftLimit:            intptr(row.OverdraftLimit),
		CreditLimit:               intptr(row.CreditLimit),
		CreditLimitIsHard:         row.CreditLimitIsHard,
		ExternalBillingProviderID: row.ExternalBillingProviderID,
	}, true, nil
}

// ListBillingCustomers returns every customer's MT billing configuration, for config-sync to compile the
// reserve-floor snapshot (step-142b). Read whole; the snapshot is swapped atomically.
func (r *BillingRepo) ListBillingCustomers(ctx context.Context) ([]cp.BillingCustomer, error) {
	rows, err := r.q.ListBillingCustomers(ctx)
	if err != nil {
		return nil, translate("list billing customers", err)
	}
	out := make([]cp.BillingCustomer, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.BillingCustomer{
			CustomerID:                row.CustomerID,
			BillingMode:               cp.BillingMode(row.BillingMode),
			OverdraftEnabled:          row.OverdraftEnabled,
			OverdraftLimit:            intptr(row.OverdraftLimit),
			CreditLimit:               intptr(row.CreditLimit),
			CreditLimitIsHard:         row.CreditLimitIsHard,
			ExternalBillingProviderID: row.ExternalBillingProviderID,
		})
	}
	return out, nil
}

// LedgerEntryExists reports whether a ledger entry of entryType already exists for messageID. It is the
// authoritative cross-partition idempotency guard (§6.9): the capture path reads it before committing so
// a redelivered message_id never double-charges, since the same-day unique index cannot span partitions.
func (r *BillingRepo) LedgerEntryExists(ctx context.Context, messageID uuid.UUID, entryType cp.EntryType) (bool, error) {
	exists, err := r.q.LedgerEntryExists(ctx, sqlcgen.LedgerEntryExistsParams{
		MessageID: &messageID, EntryType: string(entryType),
	})
	if err != nil {
		return false, translate("ledger entry exists", err)
	}
	return exists, nil
}

// ReserveEntry reads the reserve ledger entry for messageID: its signed credits (negative — the reserve
// debit) and the balance right after the reserve. The capture and release paths read it to recover the
// reserved amount and the post-reserve balance when the short-TTL Redis hold has already lapsed (§6.9).
// found=false means no reserve entry exists for the message.
func (r *BillingRepo) ReserveEntry(ctx context.Context, messageID uuid.UUID) (credits int, balanceAfter int, found bool, err error) {
	row, e := r.q.GetReserveEntry(ctx, &messageID)
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if e != nil {
		return 0, 0, false, translate("get reserve entry", e)
	}
	return int(row.Credits), int(row.BalanceAfter), true, nil
}

// RecordDurable persists one billing movement in a SINGLE transaction and is IDEMPOTENT by
// (message_id, entry_type). It first claims the movement in the partition-free billing_idempotency table:
// if that (message_id, entry_type) was already recorded — even on an earlier day, which the ledger's
// same-day idem index cannot see — the claim conflicts, no balance/ledger write happens, and it returns
// (currentBalance, applied=false) so the caller can undo any speculative cache change. Otherwise it
// applies the entry's SIGNED credit delta to the owner's durable balance (credits += delta,
// order-independent) and appends the append-only ledger row with the resulting balance as balance_after —
// so the balance is always the exact SUM of the ledger's credits, whatever order concurrent same-owner
// movements commit in — and returns (newBalance, applied=true). The claim INSERT is the lock, so two
// concurrent replays cannot both apply (no read-then-write race), and idempotency holds across day
// boundaries (invariant c), not only within the Redis hold's TTL.
//
// Entries with no message_id (top-ups, adjustments) bypass the claim and always apply — they are not
// message-scoped and carry their own audit reference. The ledger stays append-only; the caller supplies
// only the delta, never an absolute balance.
func (r *BillingRepo) RecordDurable(ctx context.Context, entry cp.LedgerEntry) (newBalance int, applied bool, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, false, translate("begin billing tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit
	qtx := r.q.WithTx(tx)

	// Claim first: a message-scoped movement already recorded (any partition) is a replay — skip the
	// balance/ledger write and report the current balance so the caller can compensate a speculative cache.
	if entry.MessageID != nil {
		claimed, cerr := qtx.ClaimIdempotency(ctx, sqlcgen.ClaimIdempotencyParams{
			MessageID: *entry.MessageID, EntryType: string(entry.EntryType),
		})
		if cerr != nil {
			return 0, false, translate("claim idempotency", cerr)
		}
		if claimed == 0 {
			bal, berr := qtx.GetBalance(ctx, sqlcgen.GetBalanceParams{
				OwnerType: entry.OwnerType, OwnerID: entry.OwnerID, Direction: entry.Direction,
			})
			if errors.Is(berr, pgx.ErrNoRows) {
				return 0, false, nil
			}
			if berr != nil {
				return 0, false, translate("get balance on replay", berr)
			}
			if err := tx.Commit(ctx); err != nil {
				return 0, false, translate("commit billing tx", err)
			}
			return int(bal), false, nil
		}
	}

	//nolint:gosec // credit counts are bounded well within int32 (integer credits, not monetary amounts)
	balance, err := qtx.AdjustBalance(ctx, sqlcgen.AdjustBalanceParams{
		OwnerType: entry.OwnerType,
		OwnerID:   entry.OwnerID,
		Direction: entry.Direction,
		Delta:     int32(entry.Credits),
	})
	if err != nil {
		return 0, false, translate("adjust balance", err)
	}
	//nolint:gosec // see above: balance is an integer credit count
	if err := qtx.InsertLedgerEntry(ctx, sqlcgen.InsertLedgerEntryParams{
		OwnerType:    entry.OwnerType,
		OwnerID:      entry.OwnerID,
		Direction:    entry.Direction,
		CustomerID:   entry.CustomerID,
		AccountID:    entry.AccountID,
		MessageID:    entry.MessageID,
		EntryType:    string(entry.EntryType),
		Credits:      int32(entry.Credits),
		BalanceAfter: balance,
		Reference:    entry.Reference,
	}); err != nil {
		return 0, false, translate("insert ledger entry", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, translate("commit billing tx", err)
	}
	return int(balance), true, nil
}
