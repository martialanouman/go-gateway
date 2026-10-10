package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// BillingRepo is the durable authority for billing: it reads and writes balances, reads billing
// configuration, and appends the append-only ledger (control_plane.balances / customers /
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

// Balance reads the durable owner balance for a direction (mt/mo). found=false means nothing has ever been
// recorded for the owner — a legitimate "zero" state that the caller treats as 0, not an error.
func (r *BillingRepo) Balance(ctx context.Context, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error) {
	return balanceOn(ctx, r.q, ownerType, ownerID, direction)
}

func balanceOn(ctx context.Context, q *sqlcgen.Queries, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error) {
	row, err := q.GetBalance(ctx, sqlcgen.GetBalanceParams{
		OwnerType: ownerType, OwnerID: ownerID, Direction: direction,
	})
	if err != nil {
		return 0, false, translate("get balance", err)
	}
	return int(row.Credits), row.Found != nil && *row.Found, nil
}

// billingModeVal maps the nullable customers.billing_mode to the domain type; a NULL mode is "" (unset),
// which the floor mapping treats as strict prepaid.
func billingModeVal(m *string) cp.BillingMode {
	if m == nil {
		return ""
	}
	return cp.BillingMode(*m)
}

// BillingCustomer reads a customer's MT billing configuration (which lives on the customer row, §6.9,
// step-142d). found=false means no such customer, not "unconfigured".
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
		BillingMode:               billingModeVal(row.BillingMode),
		OverdraftEnabled:          row.OverdraftEnabled,
		OverdraftLimit:            intptr(row.OverdraftLimit),
		CreditLimit:               intptr(row.CreditLimit),
		CreditLimitIsHard:         row.CreditLimitIsHard,
		MoBillingFloor:            intptr(row.MoBillingFloor),
		ExternalBillingProviderID: row.ExternalBillingProviderID,
	}, true, nil
}

// ListBillingCustomers returns the MT billing configuration of every billing-enabled customer, for
// config-sync to compile the reserve-floor snapshot (step-142b/d). Read whole; the snapshot is swapped
// atomically.
func (r *BillingRepo) ListBillingCustomers(ctx context.Context) ([]cp.BillingCustomer, error) {
	rows, err := r.q.ListBillingCustomers(ctx)
	if err != nil {
		return nil, translate("list billing customers", err)
	}
	out := make([]cp.BillingCustomer, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.BillingCustomer{
			CustomerID:                row.CustomerID,
			BillingMode:               billingModeVal(row.BillingMode),
			OverdraftEnabled:          row.OverdraftEnabled,
			OverdraftLimit:            intptr(row.OverdraftLimit),
			CreditLimit:               intptr(row.CreditLimit),
			CreditLimitIsHard:         row.CreditLimitIsHard,
			MoBillingFloor:            intptr(row.MoBillingFloor),
			ExternalBillingProviderID: row.ExternalBillingProviderID,
		})
	}
	return out, nil
}

// ListBillingScopes returns the identity and balance scope of every billing-enabled customer, for
// router-svc to compile the credit-stage snapshot (step-145). Presence in the result is the billing-enabled
// flag itself; a customer absent from it is not billed. balance_scope is NOT NULL in the schema (default
// 'customer'), so the domain value maps straight through.
func (r *BillingRepo) ListBillingScopes(ctx context.Context) ([]cp.CustomerBillingScope, error) {
	rows, err := r.q.ListBillingScopes(ctx)
	if err != nil {
		return nil, translate("list billing scopes", err)
	}
	out := make([]cp.CustomerBillingScope, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.CustomerBillingScope{
			CustomerID: row.CustomerID,
			Scope:      cp.BalanceScope(row.BalanceScope),
		})
	}
	return out, nil
}

// ConsumedCredits returns a customer's locally settled MT consumption (§6.10 reconciliation): the sum of
// captured reserve debits, as a positive total. In-flight holds and released reservations are excluded.
func (r *BillingRepo) ConsumedCredits(ctx context.Context, customerID uuid.UUID) (int64, error) {
	consumed, err := r.q.ConsumedCredits(ctx, customerID)
	if err != nil {
		return 0, translate("consumed credits", err)
	}
	return consumed, nil
}

// ListExternalBillingConfigs returns the ACTIVE external-provider join for every billing-enabled customer
// that references one (§6.10), for billing-svc to compile the reserve-hot-path snapshot. The inner join +
// status filter mean a disabled or dangling provider yields no row (external layer off).
func (r *BillingRepo) ListExternalBillingConfigs(ctx context.Context) ([]cp.CustomerExternalBilling, error) {
	rows, err := r.q.ListExternalBillingConfigs(ctx)
	if err != nil {
		return nil, translate("list external billing configs", err)
	}
	out := make([]cp.CustomerExternalBilling, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.CustomerExternalBilling{
			CustomerID:    row.CustomerID,
			ProviderID:    row.ProviderID,
			Mode:          cp.ExternalBillingMode(row.Mode),
			SyncTimeoutMs: intptr(row.SyncCallTimeoutMs),
			FailurePolicy: cp.BillingFailurePolicy(row.FailurePolicy),
			CacheTTLMs:    int(row.CacheTtlMs),
		})
	}
	return out, nil
}

// Ledger returns one keyset page of a customer's billing ledger, newest first (step-149 get-billing-ledger),
// plus whether a further page exists. It fetches Limit+1 rows to decide hasMore without a second round-trip.
// The ledger carries no message body (invariant a).
func (r *BillingRepo) Ledger(ctx context.Context, f cp.LedgerFilter) (rows []cp.LedgerRow, hasMore bool, err error) {
	var afterCreated pgtype.Timestamptz
	var afterID *uuid.UUID
	if !f.After.CreatedAt.IsZero() {
		afterCreated = pgtype.Timestamptz{Time: f.After.CreatedAt, Valid: true}
		id := f.After.ID
		afterID = &id
	}
	//nolint:gosec // Limit is clamped to 500 by the handler, so +1 cannot overflow int32.
	dbrows, err := r.q.ListLedger(ctx, sqlcgen.ListLedgerParams{
		CustomerID: f.CustomerID, Direction: f.Direction, AccountID: f.AccountID,
		AfterCreated: afterCreated, AfterID: afterID, Lim: int32(f.Limit) + 1,
	})
	if err != nil {
		return nil, false, translate("list ledger", err)
	}
	hasMore = len(dbrows) > f.Limit
	if hasMore {
		dbrows = dbrows[:f.Limit]
	}
	rows = make([]cp.LedgerRow, 0, len(dbrows))
	for _, row := range dbrows {
		rows = append(rows, cp.LedgerRow{
			ID: row.ID, OwnerType: row.OwnerType, OwnerID: row.OwnerID, Direction: row.Direction,
			CustomerID: row.CustomerID, AccountID: row.AccountID, MessageID: row.MessageID,
			EntryType: cp.EntryType(row.EntryType), Credits: int(row.Credits), BalanceAfter: int(row.BalanceAfter),
			Reference: row.Reference, CreatedAt: tsVal(row.CreatedAt),
		})
	}
	return rows, hasMore, nil
}

// Balances reads the MT and MO durable balance of each owner (step-148 get-customer-balances). A missing
// balance row reads as 0. It returns one BalanceRow per (owner, direction), in owner order then MT before MO.
func (r *BillingRepo) Balances(ctx context.Context, owners []cp.BalanceOwner) ([]cp.BalanceRow, error) {
	rows := make([]cp.BalanceRow, 0, len(owners)*2)
	for _, o := range owners {
		for _, dir := range []string{cp.BillingDirectionMT, cp.BillingDirectionMO} {
			credits, _, err := r.Balance(ctx, o.OwnerType, o.OwnerID, dir)
			if err != nil {
				return nil, err
			}
			rows = append(rows, cp.BalanceRow{OwnerType: o.OwnerType, OwnerID: o.OwnerID, Direction: dir, Credits: credits})
		}
	}
	return rows, nil
}

// Transfer moves MT credit between two owners of the same customer atomically and idempotently (§6.9,
// step-148): debit and credit are two EntryTransfer ledger rows summing to zero, applied in one tx after a
// FOR UPDATE lock + overdraw guard on the source. debit.Credits must be negative and credit.Credits its
// positive mirror; both carry the same MT direction, the shared idemKey as MessageID and a shared reference.
// applied=false means idemKey was already used (a retry) — no double-move. The source may not be overdrawn:
// its resulting balance must stay >= 0 (a transfer never consumes overdraft headroom).
func (r *BillingRepo) Transfer(ctx context.Context, debit, credit cp.LedgerEntry, idemKey uuid.UUID) (rows []cp.LedgerRow, applied bool, err error) {
	amount := -debit.Credits
	if amount <= 0 || credit.Credits != amount || debit.Direction != credit.Direction {
		return nil, false, fmt.Errorf("transfer: debit/credit must be a positive same-direction mirrored pair: %w", errs.ErrValidation)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, translate("begin transfer tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := r.q.WithTx(tx)

	// Claim once for the pair: a retry with the same key finds it claimed and skips both writes.
	claimed, cerr := qtx.ClaimIdempotency(ctx, sqlcgen.ClaimIdempotencyParams{MessageID: idemKey, EntryType: string(cp.EntryTransfer)})
	if cerr != nil {
		return nil, false, translate("claim transfer idempotency", cerr)
	}
	if claimed == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, translate("commit transfer tx", err)
		}
		return nil, false, nil
	}

	// Lock both owners in the fold's order (owner_type, owner_id), or a transfer and a fold touching the same
	// two rows in opposite orders deadlock. Then refuse to overdraw the source.
	legs := []cp.BalanceOwner{{OwnerType: debit.OwnerType, OwnerID: debit.OwnerID}, {OwnerType: credit.OwnerType, OwnerID: credit.OwnerID}}
	if err := lockBalances(ctx, qtx, legs, debit.Direction); err != nil {
		return nil, false, err
	}
	srcBal, _, err := balanceOn(ctx, qtx, debit.OwnerType, debit.OwnerID, debit.Direction)
	if err != nil {
		return nil, false, err
	}
	if srcBal < amount {
		return nil, false, fmt.Errorf("transfer: source balance %d < %d: %w", srcBal, amount, errs.ErrInsufficientCredit)
	}

	// The two legs must NOT carry message_id on the ledger: they share the transfer's idem key, and the
	// partial unique index (message_id, entry_type, created_at) — created_at being constant within the tx —
	// would reject the second leg. Idempotency is the billing_idempotency claim above; the shared idem key
	// travels as the correlation reference so the pair stays linkable.
	ref := idemKey.String()
	for _, e := range []cp.LedgerEntry{debit, credit} {
		e.MessageID = nil
		e.Reference = &ref
		row, aerr := applyEntry(ctx, qtx, e)
		if aerr != nil {
			return nil, false, aerr
		}
		rows = append(rows, row)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, translate("commit transfer tx", err)
	}
	return rows, true, nil
}

// ChangeBalanceScope flips a customer's balance_scope, refusing (conflict) if any balance of its CURRENT
// scope owners is non-zero — otherwise the credit would be stranded under an owner nothing reads again
// (§6.9, step-148). It locks the customer row and every current-owner balance row in one tx so the zero-check
// and the flip are atomic against a concurrent durable mirror write. currentOwners are the customer's owners
// under the current scope (the customer itself, or all its accounts). The same-table CHECK (142c) still
// rejects 'smpp_account' with a hard limit/overdraft — surfaced as a validation error, not a 500.
func (r *BillingRepo) ChangeBalanceScope(ctx context.Context, customerID uuid.UUID, currentOwners []cp.BalanceOwner, newScope string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return translate("begin change-scope tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := r.q.WithTx(tx)

	if _, err := qtx.LockCustomerScope(ctx, customerID); err != nil {
		return translate("lock customer", err)
	}
	if err := lockBalances(ctx, qtx, currentOwners, cp.BillingDirectionMO, cp.BillingDirectionMT); err != nil {
		return err
	}
	for _, o := range currentOwners {
		for _, dir := range []string{cp.BillingDirectionMT, cp.BillingDirectionMO} {
			bal, _, berr := balanceOn(ctx, qtx, o.OwnerType, o.OwnerID, dir)
			if berr != nil {
				return berr
			}
			if bal != 0 {
				return fmt.Errorf("change-scope: %s %s balance is %d, not zero: %w", o.OwnerType, dir, bal, errs.ErrConflict)
			}
		}
	}
	n, err := qtx.UpdateBalanceScope(ctx, sqlcgen.UpdateBalanceScopeParams{ID: customerID, BalanceScope: newScope})
	if err != nil {
		return translate("update balance scope", err)
	}
	if n == 0 {
		return translate("update balance scope", pgx.ErrNoRows)
	}
	if err := tx.Commit(ctx); err != nil {
		return translate("commit change-scope tx", err)
	}
	return nil
}

// applyEntry adjusts the owner's balance by the entry's signed delta and appends the ledger row, on the
// given tx queries, returning the stored row (id, balance_after, created_at). It is Topup's and Transfer's
// apply step; RecordDurable appends a delta instead (ADR-0022).
//
//nolint:gosec // credit counts are bounded well within int32 (integer credits, not monetary amounts)
func applyEntry(ctx context.Context, qtx *sqlcgen.Queries, e cp.LedgerEntry) (cp.LedgerRow, error) {
	if err := qtx.AdjustBalance(ctx, sqlcgen.AdjustBalanceParams{
		OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction, Delta: int32(e.Credits),
	}); err != nil {
		return cp.LedgerRow{}, translate("adjust balance", err)
	}
	// Read in a later statement than the adjust, for the reason LockBalance gives.
	balance, _, err := balanceOn(ctx, qtx, e.OwnerType, e.OwnerID, e.Direction)
	if err != nil {
		return cp.LedgerRow{}, err
	}
	row, err := qtx.InsertLedgerEntry(ctx, sqlcgen.InsertLedgerEntryParams{
		OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction, CustomerID: e.CustomerID,
		AccountID: e.AccountID, MessageID: e.MessageID, EntryType: string(e.EntryType), Credits: int32(e.Credits),
		BalanceAfter: int32(balance), Reference: e.Reference,
	})
	if err != nil {
		return cp.LedgerRow{}, translate("insert ledger entry", err)
	}
	return cp.LedgerRow{
		ID: row.ID, OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction, CustomerID: e.CustomerID,
		AccountID: e.AccountID, MessageID: e.MessageID, EntryType: e.EntryType, Credits: e.Credits,
		BalanceAfter: balance, Reference: e.Reference, CreatedAt: tsVal(row.CreatedAt),
	}, nil
}

// Topup credits an owner's balance and appends a durable ledger entry, idempotently by the entry's MessageID
// (an admin-supplied key, §6.9 step-148). It returns the stored ledger row; applied=false means the key was
// already used (a retry) — no double-credit — and the returned row is empty.
func (r *BillingRepo) Topup(ctx context.Context, entry cp.LedgerEntry) (row cp.LedgerRow, applied bool, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return cp.LedgerRow{}, false, translate("begin topup tx", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := r.q.WithTx(tx)

	if entry.MessageID != nil {
		claimed, cerr := qtx.ClaimIdempotency(ctx, sqlcgen.ClaimIdempotencyParams{MessageID: *entry.MessageID, EntryType: string(entry.EntryType)})
		if cerr != nil {
			return cp.LedgerRow{}, false, translate("claim topup idempotency", cerr)
		}
		if claimed == 0 {
			if err := tx.Commit(ctx); err != nil {
				return cp.LedgerRow{}, false, translate("commit topup tx", err)
			}
			return cp.LedgerRow{}, false, nil
		}
	}
	row, err = applyEntry(ctx, qtx, entry)
	if err != nil {
		return cp.LedgerRow{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return cp.LedgerRow{}, false, translate("commit topup tx", err)
	}
	return row, true, nil
}

// MessageEntries reads which MT lifecycle entries the ledger holds for messageID, in one round trip across
// every partition (step-287k).
func (r *BillingRepo) MessageEntries(ctx context.Context, messageID uuid.UUID) (cp.MessageEntries, error) {
	row, err := r.q.GetMessageEntries(ctx, &messageID)
	if err != nil {
		return cp.MessageEntries{}, translate("message entries", err)
	}
	return cp.MessageEntries{Reserve: row.HasReserve, Capture: row.HasCapture, Release: row.HasRelease}, nil
}

// maxOrphanBatch caps one reaper sweep whatever the caller asks for. It keeps the row cap inside int32 (the
// column type sqlc generates) and makes an unbounded read impossible: this is the one query in billing-svc
// that could otherwise scan an entire backlog in a single pass and stall the service.
const maxOrphanBatch = 10_000

// OrphanedReservations lists reservations the settle loop never closed — a `reserve` claim with neither a
// capture nor a release, older than the cutoff (step-190). connector-pool settles fail-open, so a billing
// outage leaves the reserve debit standing and the customer charged for a message that may never have been
// sent; these rows are what the reaper reconciles.
//
// olderThan keeps the sweep off the nominal path: it must sit well beyond a message's normal time to a
// terminal outcome, or the reaper races the connector pool and settles messages still legitimately in
// flight. limit bounds one pass — the sweep is periodic, so an unbounded read would be the one query able
// to stall the billing service.
func (r *BillingRepo) OrphanedReservations(ctx context.Context, olderThan time.Time, limit int) ([]cp.OrphanedReservation, error) {
	if limit <= 0 || limit > maxOrphanBatch {
		limit = maxOrphanBatch
	}
	rows, err := r.q.ListOrphanedReservations(ctx, sqlcgen.ListOrphanedReservationsParams{
		OlderThan: tsFrom(olderThan), RowLimit: int32(limit),
	})
	if err != nil {
		return nil, translate("list orphaned reservations", err)
	}
	out := make([]cp.OrphanedReservation, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.OrphanedReservation{
			MessageID:  row.MessageID,
			OwnerType:  row.OwnerType,
			OwnerID:    row.OwnerID,
			CustomerID: row.CustomerID,
			AccountID:  row.AccountID,
			Credits:    int(row.Credits),
			ReservedAt: tsVal(row.ReservedAt),
		})
	}
	return out, nil
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
// appends the entry's SIGNED credit delta to balance_deltas — never the owner's balances row, whose lock
// serialised a customer's whole traffic (ADR-0022) — and the append-only ledger row, and returns
// (newBalance, applied=true). The durable balance stays the exact SUM of the ledger's credits. The claim
// INSERT is the lock, so two concurrent replays cannot both apply (no read-then-write race), and idempotency
// holds across day boundaries (invariant c), not only within the Redis hold's TTL.
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
			bal, _, berr := balanceOn(ctx, qtx, entry.OwnerType, entry.OwnerID, entry.Direction)
			if berr != nil {
				return 0, false, berr
			}
			if err := tx.Commit(ctx); err != nil {
				return 0, false, translate("commit billing tx", err)
			}
			return bal, false, nil
		}
	}

	if entry.Credits != 0 {
		//nolint:gosec // credit counts are bounded well within int32 (integer credits, not monetary amounts)
		if err := qtx.InsertBalanceDelta(ctx, sqlcgen.InsertBalanceDeltaParams{
			OwnerType: entry.OwnerType, OwnerID: entry.OwnerID, Direction: entry.Direction, Credits: int32(entry.Credits),
		}); err != nil {
			return 0, false, translate("insert balance delta", err)
		}
	}
	balance, err := balanceAfter(ctx, qtx, entry)
	if err != nil {
		return 0, false, err
	}
	//nolint:gosec // see above: balance is an integer credit count
	if _, err := qtx.InsertLedgerEntry(ctx, sqlcgen.InsertLedgerEntryParams{
		OwnerType:    entry.OwnerType,
		OwnerID:      entry.OwnerID,
		Direction:    entry.Direction,
		CustomerID:   entry.CustomerID,
		AccountID:    entry.AccountID,
		MessageID:    entry.MessageID,
		EntryType:    string(entry.EntryType),
		Credits:      int32(entry.Credits),
		BalanceAfter: int32(balance),
		Reference:    entry.Reference,
	}); err != nil {
		return 0, false, translate("insert ledger entry", err)
	}
	if entry.MOFloorReached != nil {
		//nolint:gosec // see above: integer credit counts
		if err := qtx.InsertBillingEvent(ctx, sqlcgen.InsertBillingEventParams{
			OwnerType: entry.OwnerType, OwnerID: entry.OwnerID, CustomerID: entry.CustomerID,
			BalanceAfter: int32(balance), Floor: int32(*entry.MOFloorReached),
		}); err != nil {
			return 0, false, translate("insert billing event", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, translate("commit billing tx", err)
	}
	return balance, true, nil
}

// balanceAfter is the value Redis computed for the entry when it has one. Reading the durable balance on
// every reserve would sum all the owner's unfolded deltas — a cost that grows with the customer's rate.
func balanceAfter(ctx context.Context, qtx *sqlcgen.Queries, entry cp.LedgerEntry) (int, error) {
	if entry.BalanceAfter != nil {
		return *entry.BalanceAfter, nil
	}
	bal, _, err := balanceOn(ctx, qtx, entry.OwnerType, entry.OwnerID, entry.Direction)
	return bal, err
}

// FoldOnce moves up to limit unfolded deltas into balances (ADR-0022) and returns how many it moved; fewer
// than limit means the backlog is drained.
func (r *BillingRepo) FoldOnce(ctx context.Context, limit int) (int64, error) {
	//nolint:gosec // the caller's batch size is a small constant
	n, err := r.q.FoldBalanceDeltas(ctx, int32(limit))
	if err != nil {
		return 0, translate("fold balance deltas", err)
	}
	return n, nil
}

// RelayBillingEvents hands up to limit queued events to publish and deletes them once it returns nil. A
// failed publish leaves every row queued for the next pass. Returns how many it relayed; fewer than limit
// means none are left.
//
// It holds no transaction and no lock while publish runs: a produce in flight is not bounded by the
// produce timeout, and a lock held through a broker outage would pin a connection of the pool the hot
// path shares, and the vacuum horizon with it. The price is that two replicas may publish the same event
// — consumers deduplicate on its id.
func (r *BillingRepo) RelayBillingEvents(ctx context.Context, limit int, publish func(context.Context, []cp.BillingEvent) error) (int, error) {
	//nolint:gosec // the caller's batch size is a small constant
	rows, err := r.q.ListPendingBillingEvents(ctx, int32(limit))
	if err != nil {
		return 0, translate("list billing events", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	events := make([]cp.BillingEvent, len(rows))
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		events[i] = cp.BillingEvent{
			ID: row.ID, OwnerType: row.OwnerType, OwnerID: row.OwnerID, CustomerID: row.CustomerID,
			BalanceAfter: int(row.BalanceAfter), Floor: int(row.Floor), CreatedAt: tsVal(row.CreatedAt),
		}
		ids[i] = row.ID
	}
	if err := publish(ctx, events); err != nil {
		return 0, err
	}
	if err := r.q.DeleteBillingEvents(ctx, ids); err != nil {
		return 0, translate("delete billing events", err)
	}
	return len(rows), nil
}

// OldestPendingBillingEvent returns when the oldest queued billing event was recorded; found=false when none waits.
func (r *BillingRepo) OldestPendingBillingEvent(ctx context.Context) (time.Time, bool, error) {
	at, err := r.q.OldestBillingEvent(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, translate("oldest billing event", err)
	}
	return tsVal(at), true, nil
}

// OldestPendingDelta returns when the oldest unfolded delta was recorded; found=false when none is pending.
func (r *BillingRepo) OldestPendingDelta(ctx context.Context) (time.Time, bool, error) {
	at, err := r.q.OldestBalanceDelta(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, translate("oldest balance delta", err)
	}
	return tsVal(at), true, nil
}

// lockBalances locks the owners' balances rows in the fold's order — (owner_type, owner_id, direction) — so
// an admin tx and a fold touching the same rows cannot deadlock. directions must be given in ascending order.
func lockBalances(ctx context.Context, qtx *sqlcgen.Queries, owners []cp.BalanceOwner, directions ...string) error {
	sorted := slices.Clone(owners)
	slices.SortFunc(sorted, func(a, b cp.BalanceOwner) int {
		if c := strings.Compare(a.OwnerType, b.OwnerType); c != 0 {
			return c
		}
		return bytes.Compare(a.OwnerID[:], b.OwnerID[:])
	})
	for _, o := range sorted {
		for _, dir := range directions {
			if err := qtx.LockBalance(ctx, sqlcgen.LockBalanceParams{OwnerType: o.OwnerType, OwnerID: o.OwnerID, Direction: dir}); err != nil {
				return translate("lock balance", err)
			}
		}
	}
	return nil
}

// ledgerPartitionLock serialises the replicas creating ledger partitions; it only has to differ from every
// other advisory lock taken on this database.
const ledgerPartitionLock int64 = 0x6c6564676572

// EnsureLedgerPartitions creates the daily (UTC) ledger partitions from from's day on, for days days. Each day
// is its own transaction, so a day that fails — DEFAULT already holding rows of its range — leaves the others.
//
// CREATE TABLE … PARTITION OF would take ACCESS EXCLUSIVE on the parent, which every hot-path write holds in
// ROW EXCLUSIVE; ATTACH PARTITION takes SHARE UPDATE EXCLUSIVE on the parent and ACCESS EXCLUSIVE on DEFAULT
// alone. The advisory lock is shared by every day, so a replica must wait for it rather than skip: skipping
// a day another replica is not on yet leaves it to nobody.
func (r *BillingRepo) EnsureLedgerPartitions(ctx context.Context, from time.Time, days int) error {
	day := from.UTC().Truncate(24 * time.Hour)
	var failed []error
	for range days {
		if err := r.ensureLedgerPartition(ctx, day); err != nil {
			failed = append(failed, fmt.Errorf("ledger partition %s: %w", day.Format(time.DateOnly), err))
		}
		day = day.AddDate(0, 0, 1)
	}
	return errors.Join(failed...)
}

func (r *BillingRepo) ensureLedgerPartition(ctx context.Context, day time.Time) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		// Not the ATTACH's 200ms: the losing replica waits here for the winner's whole transaction.
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '1s'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ledgerPartitionLock); err != nil {
			return err
		}
		name := "control_plane.billing_ledger_" + day.Format("20060102")
		// A table of that name that is not attached is not the day's partition: the CREATE below fails on it, loudly.
		var attached bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_inherits
			WHERE inhparent = 'control_plane.billing_ledger'::regclass AND inhrelid = to_regclass($1))`, name).Scan(&attached); err != nil || attached {
			return err
		}
		for _, stmt := range []string{
			`CREATE TABLE ` + name + ` (LIKE control_plane.billing_ledger INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`,
			// The capture's ledger reads cannot be pruned and hold DEFAULT: those arriving while the ATTACH waits for
			// DEFAULT queue behind it, then wait out its scan of DEFAULT. Both together stay under 200ms.
			`SET LOCAL statement_timeout = '200ms'`,
			fmt.Sprintf(`ALTER TABLE control_plane.billing_ledger ATTACH PARTITION %s FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')`,
				name, day.Format(time.DateOnly), day.AddDate(0, 0, 1).Format(time.DateOnly)),
		} {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	})
}

// LedgerDefaultRows counts the rows in the ledger's DEFAULT partition, capped at 10 000: any is a day that had
// no partition, and a full count of a DEFAULT that took a day's traffic would cost a scan per scrape.
func (r *BillingRepo) LedgerDefaultRows(ctx context.Context) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM (SELECT 1 FROM control_plane.billing_ledger_default LIMIT 10000) s`).Scan(&n)
	return n, translate("count ledger default rows", err)
}
