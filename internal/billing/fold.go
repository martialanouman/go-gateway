package billing

import (
	"context"
	"log/slog"
	"time"
)

// foldBatch bounds one fold statement; maxBatchesPerPass bounds one pass of the fold or of the events relay,
// so a backlog growing faster than the pass still lets it end.
const (
	foldBatch         = 5000
	maxBatchesPerPass = 10
)

// FoldStore is the durable side of the balance-delta fold (ADR-0022); *postgres.BillingRepo satisfies it.
type FoldStore interface {
	FoldOnce(ctx context.Context, limit int) (int64, error)
	OldestPendingDelta(ctx context.Context) (time.Time, bool, error)
}

// FoldLag receives the age, in seconds, of the oldest delta not yet folded.
type FoldLag interface {
	Set(seconds float64)
}

// Folder moves balance deltas into balances (ADR-0022). The fold never changes a balance, only where it
// lives: its lag costs read time — every durable read sums the unfolded deltas — not correctness.
type Folder struct {
	store  FoldStore
	lag    FoldLag
	logger *slog.Logger
}

// NewFolder builds a Folder over the durable store, publishing its lag to lag.
func NewFolder(store FoldStore, lag FoldLag, logger *slog.Logger) *Folder {
	return &Folder{store: store, lag: lag, logger: logger}
}

// DrainOnce folds until a batch comes back short, then publishes the lag. A failed fold (a 40P01 against an
// admin tx) waits for the next pass.
func (f *Folder) DrainOnce(ctx context.Context) {
	drainBatches(ctx, f.logger, "balance-delta fold", foldBatch, f.store.FoldOnce)
	if ctx.Err() != nil {
		return
	}
	oldest, pending, err := f.store.OldestPendingDelta(ctx)
	if err != nil {
		f.logger.WarnContext(ctx, "billing: could not read the balance-delta lag", "err", err)
		return
	}
	if !pending {
		f.lag.Set(0)
		return
	}
	f.lag.Set(max(0, time.Since(oldest).Seconds()))
}

// drainBatches runs step until a batch comes back short or maxBatchesPerPass ran, so a backlog growing
// faster than the pass still lets it end. A failed step waits for the next pass.
func drainBatches(ctx context.Context, logger *slog.Logger, what string, batch int, step func(context.Context, int) (int64, error)) {
	for range maxBatchesPerPass {
		n, err := step(ctx, batch)
		if err != nil {
			if ctx.Err() == nil {
				logger.WarnContext(ctx, "billing: "+what+" failed — retrying next pass", "err", err)
			}
			return
		}
		if n < int64(batch) {
			return
		}
	}
}
