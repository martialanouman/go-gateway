package postgres

import "context"

// Queued is the number of movements waiting to be taken by the batch writer.
func (b *BillingBatcher) Queued() int { return int(b.waiting.Load()) }

// AnswerCommitOutcome makes the batcher read status as the outcome of any failed COMMIT.
func (b *BillingBatcher) AnswerCommitOutcome(status string) {
	b.xactStatus = func(context.Context, string) (string, error) { return status, nil }
}

// XactStatus is the outcome query the batcher asks after a failed COMMIT.
func (r *BillingRepo) XactStatus(ctx context.Context, xid string) (string, error) {
	return r.q.XactStatus(ctx, xid)
}

const LedgerPartitionLock = ledgerPartitionLock

// NewBillingBatcherWithWriters builds a batcher with a chosen number of writers. A test of what ONE batch
// does (its cap, a duplicate inside it, a poisoned movement) needs one writer: with several, the movements
// it queues go to whichever writer is free, and no batch has a predictable content.
func NewBillingBatcherWithWriters(repo *BillingRepo, sizes BatchSizeObserver, writers int, opts ...BatcherOption) *BillingBatcher {
	return newBillingBatcher(repo, sizes, writers, opts...)
}
