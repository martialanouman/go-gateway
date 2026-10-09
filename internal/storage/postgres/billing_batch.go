package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

const (
	batchCap = 256
	// batchWriters write batches in parallel (step-287f): one writer was ~95 % busy at 370 writes/s on the
	// test VPS and made every movement wait for the batch before it. The primary key of billing_idempotency
	// arbitrates concurrent transactions, as it did when every movement was its own transaction.
	batchWriters = 4
	// batchWriteTimeout bounds a movement whose caller set no deadline, and so every batch. It matches
	// billing's reserveDurableTimeout: one writer serves every caller, and a hung transaction must not stall
	// them all for longer than a reserve may wait.
	batchWriteTimeout = 4 * time.Second
	// commitResolveBudget is how long a failed COMMIT's outcome is sought. It fits in the margin billing keeps
	// between its terminal critical section (4 s) and the terminal lock's TTL (5 s).
	commitResolveBudget = 500 * time.Millisecond
)

// errBatchCommit marks a batch whose COMMIT failed and whose outcome could not be read back: it may have
// landed, so its movements must not be replayed one by one — a replay would answer applied=false for movements
// that were in fact applied.
var errBatchCommit = errors.New("batch commit failed")

// BatchSizeObserver receives the number of movements one batch wrote. A prometheus.Observer satisfies it.
type BatchSizeObserver interface {
	Observe(float64)
}

// BatchStageObserver times the stages of a durable write (step-287e): per movement, handoff (waiting for the
// single writer to take it) and reply (its batch's write); per batch, begin, claim, copy and commit.
type BatchStageObserver interface {
	Observe(stage string, d time.Duration)
}

// BatcherOption configures a BillingBatcher.
type BatcherOption func(*BillingBatcher)

// WithBatchStages wires the stage timer; without it nothing is timed.
func WithBatchStages(o BatchStageObserver) BatcherOption {
	return func(b *BillingBatcher) { b.stages = o }
}

// BillingBatcher is a BillingRepo whose hot-path RecordDurable shares a transaction with the movements queued
// beside it: one commit, one WAL flush and one statement per table for up to batchCap movements, where the
// commit dominated each reserve (step-285b). A batch takes whatever waits while the previous one is written,
// so an idle store answers with no added latency and a loaded one batches by itself.
//
// ponytail: one writer goroutine; several if the measured batch size stays at the cap.
type BillingBatcher struct {
	*BillingRepo
	sizes   BatchSizeObserver
	stages  BatchStageObserver
	waiting atomic.Int32
	queue   chan batchedEntry
	stop    chan struct{}
	done    sync.WaitGroup
	// xactStatus reads a transaction's outcome; a test swaps it for one Postgres cannot be made to give on
	// demand, a commit that landed while its answer was lost.
	xactStatus func(ctx context.Context, xid string) (string, error)
}

type batchedEntry struct {
	ctx   context.Context
	entry cp.LedgerEntry
	reply chan batchResult
}

type batchResult struct {
	balance int
	applied bool
	err     error
}

// NewBillingBatcher starts the batch writer over repo. Close stops it.
func NewBillingBatcher(repo *BillingRepo, sizes BatchSizeObserver, opts ...BatcherOption) *BillingBatcher {
	return newBillingBatcher(repo, sizes, batchWriters, opts...)
}

func newBillingBatcher(repo *BillingRepo, sizes BatchSizeObserver, writers int, opts ...BatcherOption) *BillingBatcher {
	b := &BillingBatcher{
		BillingRepo: repo, sizes: sizes,
		queue: make(chan batchedEntry), stop: make(chan struct{}),
		xactStatus: repo.q.XactStatus,
	}
	for _, o := range opts {
		o(b)
	}
	for range writers {
		b.done.Add(1)
		go b.run()
	}
	return b
}

// Close stops the writer after the batch in flight; later movements are written one by one.
func (b *BillingBatcher) Close() {
	close(b.stop)
	b.done.Wait()
}

// RecordDurable batches a movement that carries a message and the balance Redis decided. Any other movement
// reads its balance from the database, which inside a batch would depend on its neighbours, so it is written
// alone.
//
// Once its movement is taken, it returns at ctx's deadline, not at its cancellation: the batch ends by then,
// so the movement's fate is sealed when the caller learns it — the terminal lock and the reserve's
// lost-commit check both release or reread right after.
func (b *BillingBatcher) RecordDurable(ctx context.Context, entry cp.LedgerEntry) (int, bool, error) {
	// A floor crossing is written alone too: the batch writes no billing events outbox (step-400), and a
	// crossing is rare enough that it costs the batch nothing.
	if entry.MessageID == nil || entry.BalanceAfter == nil || entry.MOFloorReached != nil {
		return b.BillingRepo.RecordDurable(ctx, entry)
	}
	// Checked before the select, which picks at random when both the queue and Done are ready: one movement
	// past its deadline would end its whole batch at once.
	if err := ctx.Err(); err != nil {
		return 0, false, translate("record durable", err)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, batchWriteTimeout)
		defer cancel()
	}
	reply := make(chan batchResult, 1)
	b.waiting.Add(1)
	start := time.Now()
	select {
	case b.queue <- batchedEntry{ctx: ctx, entry: entry, reply: reply}:
		b.waiting.Add(-1)
		b.time("handoff", start)
	case <-b.stop:
		b.waiting.Add(-1)
		return b.BillingRepo.RecordDurable(ctx, entry)
	case <-ctx.Done():
		b.waiting.Add(-1)
		return 0, false, translate("record durable", ctx.Err())
	}
	start = time.Now()
	r := <-reply
	b.time("reply", start)
	return r.balance, r.applied, r.err
}

func (b *BillingBatcher) time(stage string, start time.Time) {
	if b.stages != nil {
		b.stages.Observe(stage, time.Since(start))
	}
}

func (b *BillingBatcher) run() {
	defer b.done.Done()
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

// write ends the batch at its members' nearest deadline, so no movement can commit after its caller has
// been answered.
func (b *BillingBatcher) write(batch []batchedEntry) {
	b.sizes.Observe(float64(len(batch)))
	deadline := time.Now().Add(batchWriteTimeout)
	entries := make([]cp.LedgerEntry, len(batch))
	for i, e := range batch {
		if d, ok := e.ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		entries[i] = e.entry
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	results, xid, err := b.recordBatch(ctx, entries, b.time)
	if errors.Is(err, errBatchCommit) {
		switch b.commitOutcome(xid) {
		case "committed":
			err = nil
		case "aborted": // nothing landed: replayed below, like a failure before the COMMIT
		default:
			for _, e := range batch {
				e.reply <- batchResult{err: err}
			}
			return
		}
	}
	if err == nil {
		for i, e := range batch {
			e.reply <- results[i]
		}
		return
	}
	// Nothing landed, and one bad movement fails the whole transaction: each one alone gets its own outcome,
	// side by side, so each answer is bounded by its own deadline and not by its neighbours' replays.
	// ponytail: one poisoned movement costs batchCap unitary transactions; bisect if it stops being rare.
	var wg sync.WaitGroup
	for _, e := range batch {
		wg.Go(func() {
			balance, applied, err := b.BillingRepo.RecordDurable(e.ctx, e.entry)
			e.reply <- batchResult{balance: balance, applied: applied, err: err}
		})
	}
	wg.Wait()
}

// commitOutcome asks Postgres, on a fresh connection, what became of a transaction whose COMMIT got no answer.
// A cut client aborts a commit still at work; one already past its commit record lands. "in progress" is
// polled until the budget runs out; anything left unanswered is reported as such.
func (b *BillingBatcher) commitOutcome(xid string) string {
	ctx, cancel := context.WithTimeout(context.Background(), commitResolveBudget)
	defer cancel()
	for {
		if status, err := b.xactStatus(ctx, xid); err == nil && status != "in progress" {
			return status
		}
		select {
		case <-ctx.Done():
			return "unknown"
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// batchRow is one movement as WriteBillingBatch reads it; Ord is its position in the batch.
type batchRow struct {
	Ord          int        `json:"ord"`
	OwnerType    string     `json:"owner_type"`
	OwnerID      uuid.UUID  `json:"owner_id"`
	Direction    string     `json:"direction"`
	CustomerID   uuid.UUID  `json:"customer_id"`
	AccountID    *uuid.UUID `json:"account_id"`
	MessageID    *uuid.UUID `json:"message_id"`
	EntryType    string     `json:"entry_type"`
	Credits      int        `json:"credits"`
	BalanceAfter int        `json:"balance_after"`
	Reference    *string    `json:"reference"`
}

// recordBatch is RecordDurable for movements that all carry a message and a decided balance, in one
// transaction. Results are in entries order. On a failed COMMIT it still returns them, with the transaction's
// id, for the caller to learn whether they hold.
func (r *BillingRepo) recordBatch(ctx context.Context, entries []cp.LedgerEntry, timed func(string, time.Time)) ([]batchResult, string, error) {
	start := time.Now()
	tx, err := r.pool.Begin(ctx)
	timed("begin", start)
	if err != nil {
		return nil, "", translate("begin billing batch", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := r.q.WithTx(tx)

	rows := make([]batchRow, len(entries))
	for i, e := range entries {
		rows[i] = batchRow{
			Ord: i, OwnerType: e.OwnerType, OwnerID: e.OwnerID, Direction: e.Direction, CustomerID: e.CustomerID,
			AccountID: e.AccountID, MessageID: e.MessageID, EntryType: string(e.EntryType), Credits: e.Credits,
			BalanceAfter: *e.BalanceAfter, Reference: e.Reference,
		}
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return nil, "", fmt.Errorf("encode billing batch: %w", err)
	}
	start = time.Now()
	written, err := qtx.WriteBillingBatch(ctx, payload)
	timed("write", start)
	if err != nil {
		return nil, "", translate("write billing batch", err)
	}

	results := make([]batchResult, len(entries))
	for _, ord := range written.Claimed {
		results[ord] = batchResult{balance: *entries[ord].BalanceAfter, applied: true}
	}
	for i, e := range entries {
		if results[i].applied {
			continue
		}
		bal, _, err := balanceOn(ctx, qtx, e.OwnerType, e.OwnerID, e.Direction)
		if err != nil {
			return nil, "", err
		}
		results[i] = batchResult{balance: bal}
	}
	xid := written.Xid
	start = time.Now()
	err = tx.Commit(ctx)
	timed("commit", start)
	if err != nil {
		return results, xid, fmt.Errorf("%w: %w", errBatchCommit, translate("commit billing batch", err))
	}
	return results, xid, nil
}
