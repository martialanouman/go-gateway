package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

const (
	batchCap = 256
	// batchWriteTimeout bounds one batch whatever its callers' deadlines: the writer serves every caller, so
	// a hung transaction must not stall them all for longer than the accountant's own durable bound.
	batchWriteTimeout = 4 * time.Second
)

// errBatchCommit marks a batch whose COMMIT failed: it may have landed, so its movements must not be replayed
// one by one — a replay would answer applied=false for movements that were in fact applied.
var errBatchCommit = errors.New("commit billing batch")

// BatchSizeObserver receives the number of movements one batch wrote. A prometheus.Observer satisfies it.
type BatchSizeObserver interface {
	Observe(float64)
}

// BillingBatcher is a BillingRepo whose hot-path RecordDurable shares a transaction with the movements queued
// beside it: one commit, one WAL flush and one round trip per table for up to batchCap movements, where the
// commit dominated each reserve (step-285b). A batch takes whatever waits while the previous one is written,
// so an idle store answers with no added latency and a loaded one batches by itself.
//
// ponytail: one writer goroutine; several if the measured batch size stays at the cap.
type BillingBatcher struct {
	*BillingRepo
	sizes BatchSizeObserver
	queue chan batchedEntry
	stop  chan struct{}
	done  chan struct{}
}

type batchedEntry struct {
	entry cp.LedgerEntry
	reply chan batchResult
}

type batchResult struct {
	balance int
	applied bool
	err     error
}

// NewBillingBatcher starts the batch writer over repo. Close stops it.
func NewBillingBatcher(repo *BillingRepo, sizes BatchSizeObserver) *BillingBatcher {
	b := &BillingBatcher{
		BillingRepo: repo, sizes: sizes,
		queue: make(chan batchedEntry), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go b.run()
	return b
}

// Close stops the writer after the batch in flight; later movements are written one by one.
func (b *BillingBatcher) Close() {
	close(b.stop)
	<-b.done
}

// RecordDurable batches a movement that carries a message and the balance Redis decided. Any other movement
// reads its balance from the database, which inside a batch would depend on its neighbours, so it is written
// alone.
func (b *BillingBatcher) RecordDurable(ctx context.Context, entry cp.LedgerEntry) (int, bool, error) {
	if entry.MessageID == nil || entry.BalanceAfter == nil {
		return b.BillingRepo.RecordDurable(ctx, entry)
	}
	reply := make(chan batchResult, 1)
	select {
	case b.queue <- batchedEntry{entry: entry, reply: reply}:
	case <-b.stop:
		return b.BillingRepo.RecordDurable(ctx, entry)
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
	select {
	case r := <-reply:
		return r.balance, r.applied, r.err
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

func (b *BillingBatcher) run() {
	defer close(b.done)
	for {
		var batch []batchedEntry
		select {
		case e := <-b.queue:
			batch = append(batch, e)
		case <-b.stop:
			return
		}
	collect:
		for len(batch) < batchCap {
			select {
			case e := <-b.queue:
				batch = append(batch, e)
			default:
				break collect
			}
		}
		b.write(batch)
	}
}

func (b *BillingBatcher) write(batch []batchedEntry) {
	b.sizes.Observe(float64(len(batch)))
	ctx, cancel := context.WithTimeout(context.Background(), batchWriteTimeout)
	defer cancel()

	entries := make([]cp.LedgerEntry, len(batch))
	for i, e := range batch {
		entries[i] = e.entry
	}
	results, err := b.recordBatch(ctx, entries)
	switch {
	case err == nil:
		for i, e := range batch {
			e.reply <- results[i]
		}
	case errors.Is(err, errBatchCommit):
		for _, e := range batch {
			e.reply <- batchResult{err: err}
		}
	default:
		// Nothing landed, and one bad movement fails the whole transaction: each one alone gets its own outcome.
		for _, e := range batch {
			balance, applied, err := b.BillingRepo.RecordDurable(ctx, e.entry)
			e.reply <- batchResult{balance: balance, applied: applied, err: err}
		}
	}
}

// recordBatch is RecordDurable for movements that all carry a message and a decided balance, in one
// transaction. Results are in entries order.
func (r *BillingRepo) recordBatch(ctx context.Context, entries []cp.LedgerEntry) ([]batchResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, translate("begin billing batch", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := r.q.WithTx(tx)

	claim := sqlcgen.ClaimIdempotencyBatchParams{
		MessageIds: make([]uuid.UUID, len(entries)), EntryTypes: make([]string, len(entries)),
	}
	for i, e := range entries {
		claim.MessageIds[i], claim.EntryTypes[i] = *e.MessageID, string(e.EntryType)
	}
	rows, err := qtx.ClaimIdempotencyBatch(ctx, claim)
	if err != nil {
		return nil, translate("claim idempotency batch", err)
	}
	claimed := make(map[sqlcgen.ClaimIdempotencyBatchRow]bool, len(rows))
	for _, row := range rows {
		claimed[row] = true
	}

	results := make([]batchResult, len(entries))
	var deltas []sqlcgen.CopyBalanceDeltasParams
	var ledger []sqlcgen.CopyLedgerEntriesParams
	for i, e := range entries {
		key := sqlcgen.ClaimIdempotencyBatchRow{MessageID: *e.MessageID, EntryType: string(e.EntryType)}
		if !claimed[key] {
			bal, _, err := balanceOn(ctx, qtx, e.OwnerType, e.OwnerID, e.Direction)
			if err != nil {
				return nil, err
			}
			results[i] = batchResult{balance: bal}
			continue
		}
		results[i] = batchResult{balance: *e.BalanceAfter, applied: true}
		//nolint:gosec // credit counts and balances are integer credits, well within int32
		if e.Credits != 0 {
			deltas = append(deltas, sqlcgen.CopyBalanceDeltasParams{
				OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction, Credits: int32(e.Credits),
			})
		}
		//nolint:gosec // see above
		ledger = append(ledger, sqlcgen.CopyLedgerEntriesParams{
			OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction,
			CustomerID: e.CustomerID, AccountID: e.AccountID, MessageID: e.MessageID,
			EntryType: string(e.EntryType), Credits: int32(e.Credits), BalanceAfter: int32(*e.BalanceAfter),
			Reference: e.Reference,
		})
	}
	if len(deltas) > 0 {
		if _, err := qtx.CopyBalanceDeltas(ctx, deltas); err != nil {
			return nil, translate("copy balance deltas", err)
		}
	}
	if len(ledger) > 0 {
		if _, err := qtx.CopyLedgerEntries(ctx, ledger); err != nil {
			return nil, translate("copy ledger entries", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", errBatchCommit, translate("commit billing batch", err))
	}
	return results, nil
}
