package connectorpool

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// ReplayConsumer consumes mt.dead-letter for the replay tool. *kafka.Consumer satisfies it.
type ReplayConsumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// Replayer re-injects dead-lettered MT messages back onto mt.routed for an operator (step-129). It
// consumes mt.dead-letter and, for each record, drops the dead_letter_reason header (a later
// dead-lettering records a fresh reason), stamps a replayed_at header, and republishes verbatim to
// mt.routed under the same message_id key. The replay therefore stays correlated — same message_id and
// trace_id, so billing remains idempotent (invariant c) — and ordered (same partition key). The body
// rides only the record value, never a header (invariant a): a plain DecodeRouted → EncodeRouted
// round-trip carries it unchanged and cannot leak it.
//
// The pool's max-age SLA bases expiry on max(SubmittedAt, replayed_at), so a replay after a long outage
// is NOT instantly re-expired on the immutable SubmittedAt — the reason the replayed_at stamp exists.
//
// One-shot in spirit: a replay produces to mt.routed, never back to mt.dead-letter, so the tool cannot
// feed its own tail. Fresh dead-letters that arrive from live failures during a run are genuinely dead
// and get replayed too; the operator stops the tool (ctx cancel) once the parked backlog is drained.
type Replayer struct {
	producer Producer
	logger   *slog.Logger
	now      func() time.Time
	count    atomic.Int64
}

// ReplayerDeps are the replayer's collaborators.
type ReplayerDeps struct {
	Producer Producer
	Logger   *slog.Logger
	// Now overrides the replay clock (tests). Nil uses time.Now.
	Now func() time.Time
}

// NewReplayer builds a replayer. A nil producer is a programming error (the caller wires a real one);
// a nil logger defaults to slog.Default and a nil clock to time.Now.
func NewReplayer(deps ReplayerDeps) *Replayer {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Replayer{producer: deps.Producer, logger: deps.Logger, now: deps.Now}
}

// Replayed reports how many records have been replayed so far (for the tool's final summary).
func (r *Replayer) Replayed() int64 { return r.count.Load() }

// Run consumes mt.dead-letter until ctx is cancelled, replaying each record to mt.routed. It is a
// supervised worker with a context stop condition — it starts no unbounded goroutine.
func (r *Replayer) Run(ctx context.Context, consumer ReplayConsumer) error {
	return consumer.Run(ctx, r.handle)
}

func (r *Replayer) handle(ctx context.Context, rec kafka.Record) error {
	routed, err := pipeline.DecodeRouted(rec)
	if err != nil {
		// Skip-and-commit a record we cannot decode: an already-dead, malformed record must not wedge the
		// drain of the rest of the backlog. Logged (ids unavailable — the value did not parse), never a body.
		r.logger.WarnContext(ctx, "connector: skipping undecodable dead-letter record", "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return nil
	}
	// Stamp the replay time: EncodeRouted emits it as the replayed_at header and omits the
	// dead_letter_reason header entirely (it is not a RoutedMT field), so the round-trip both strips the
	// old reason and rebases the max-age clock in one step.
	now := r.now()
	routed.ReplayedAt = &now
	out, err := pipeline.EncodeRouted(routed)
	if err != nil {
		return fmt.Errorf("connectorpool: replay encode: %w", err)
	}
	if err := r.producer.Produce(ctx, out); err != nil {
		return fmt.Errorf("connectorpool: replay produce: %w", err)
	}
	r.count.Add(1)
	r.logger.InfoContext(ctx, "connector: replayed a dead-lettered message",
		"message_id", routed.MessageID, "connector_id", routed.ConnectorID)
	return nil
}
