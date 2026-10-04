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
