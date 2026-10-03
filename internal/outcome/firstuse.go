package outcome

import (
	"cmp"
	"context"
	"log/slog"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// SenderIDUseMarker records first uses of sender IDs. *postgres.SenderIDRepo satisfies it.
type SenderIDUseMarker interface {
	MarkFirstUsed(ctx context.Context, uses []cp.SenderIDUse) error
}

// FirstUse marks a sender ID as used from mt.outcome, off the send path (ADR-0023): once marked, it can
// only be disabled, never deleted. Every outcome counts, failed included — its submit_sm reached the SMSC.
type FirstUse struct {
	consumer BatchConsumer
	marker   SenderIDUseMarker
	logger   *slog.Logger
}

// NewFirstUse wires the marker; a nil logger falls back to slog.Default().
func NewFirstUse(consumer BatchConsumer, marker SenderIDUseMarker, logger *slog.Logger) *FirstUse {
	if logger == nil {
		logger = slog.Default()
	}
	return &FirstUse{consumer: consumer, marker: marker, logger: logger}
}

// Run consumes mt.outcome until ctx is cancelled.
func (f *FirstUse) Run(ctx context.Context) error {
	return f.consumer.RunBatch(ctx, f.handleBatch)
}

// handleBatch issues one idempotent mark per poll batch; on failure no offset commits and it replays.
func (f *FirstUse) handleBatch(ctx context.Context, recs []kafka.Record) []error {
	results := make([]error, len(recs))
	var uses []cp.SenderIDUse
	for _, rec := range recs {
		env, err := pipeline.DecodeOutcome(rec)
		if err != nil {
			f.logger.ErrorContext(ctx, "decode mt.outcome for sender ID first use: skipping corrupt record", "err", err)
			continue
		}
		// The submitted address is the one authorized against the customer's sender IDs; a rewrite
		// target (§6.16) was never submitted by this customer.
		uses = append(uses, cp.SenderIDUse{CustomerID: env.CustomerID, Address: cmp.Or(env.OriginalFrom, env.From), UsedAt: env.SubmittedAt})
	}
	if len(uses) == 0 {
		return results
	}
	if err := f.marker.MarkFirstUsed(ctx, uses); err != nil {
		if ctx.Err() == nil {
			f.logger.ErrorContext(ctx, "sender ID first-use mark failed; will reprocess", "sender_ids", len(uses), "err", err)
		}
		for i := range results {
			results[i] = err
		}
	}
	return results
}
