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

// RecordDurable persists one billing movement: it appends the ledger entry AND reconciles the owner's
// durable balance to entry.BalanceAfter, both in a SINGLE transaction, so the immutable ledger and the
// authoritative balance can never diverge. The ledger is append-only. It records exactly what the caller
// computed — the amount and the resulting balance are decided by the atomic Redis path (step-142) or the
// admin operation (step-148), never here.
//
// It is NOT self-idempotent. Idempotency (invariant c) is the CALLER's responsibility, upheld by the
// atomic Redis reservation (billing:reservation:{message_id}, step-142) — that is the ONLY guard against
// a double-capture whose two attempts straddle a day boundary, because billing_ledger_idem_idx includes
// created_at and so cannot span partitions. A SAME-DAY duplicate insert is rejected by that index and
// surfaces here as the shared conflict error (errors.Is(err, errs.ErrConflict) from platform/errors); the
// caller must treat it as an idempotent success (the movement already happened), not a hard failure.
//
// Because UpsertBalance sets the balance ABSOLUTELY to entry.BalanceAfter (not a delta), the caller must
// apply durable writes for an owner IN ORDER — the sequential Redis-authoritative mirroring of step-142
// guarantees this; an out-of-order replay would let a stale write regress the balance.
func (r *BillingRepo) RecordDurable(ctx context.Context, entry cp.LedgerEntry) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return translate("begin billing tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	qtx := r.q.WithTx(tx)
	//nolint:gosec // credit counts are bounded well within int32 (integer credits, not monetary amounts)
	if err := qtx.InsertLedgerEntry(ctx, sqlcgen.InsertLedgerEntryParams{
		OwnerType:    entry.OwnerType,
		OwnerID:      entry.OwnerID,
		Direction:    entry.Direction,
		CustomerID:   entry.CustomerID,
		AccountID:    entry.AccountID,
		MessageID:    entry.MessageID,
		EntryType:    string(entry.EntryType),
		Credits:      int32(entry.Credits),
		BalanceAfter: int32(entry.BalanceAfter),
		Reference:    entry.Reference,
	}); err != nil {
		return translate("insert ledger entry", err)
	}
	//nolint:gosec // see above: balance is an integer credit count
	if err := qtx.UpsertBalance(ctx, sqlcgen.UpsertBalanceParams{
		OwnerType: entry.OwnerType,
		OwnerID:   entry.OwnerID,
		Direction: entry.Direction,
		Credits:   int32(entry.BalanceAfter),
	}); err != nil {
		return translate("upsert balance", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return translate("commit billing tx", err)
	}
	return nil
}
