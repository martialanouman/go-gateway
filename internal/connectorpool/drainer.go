package connectorpool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// defaultDrainRetry is how long the drainer waits before re-checking a saturated target's ceiling.
const defaultDrainRetry = 200 * time.Millisecond

// DrainConsumer consumes mt.reroute-park record-by-record so the drainer can pace each replay against the
// target's ceiling. *kafka.Consumer satisfies it.
type DrainConsumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// Drainer replays parked reroutes (step-126). It consumes mt.reroute-park, skips records for other
// connectors (option B addressing), waits until the target connector's throughput ceiling has room, then
// republishes to mt.routed — so a burst of reroutes parked during a connector outage is drained back
// into the send flow at a controlled rate, never faster than the fallback connector's limit, and never
// lost (a parked record is committed only after its replay is durably produced). The message id remains
// the partition key, so a message's segments stay ordered through park and replay.
type Drainer struct {
	consumer    DrainConsumer
	producer    Producer
	limiter     RerouteLimiter
	connectorID uuid.UUID
	retry       time.Duration
	logger      *slog.Logger
}

// DrainerDeps are the drainer's collaborators.
type DrainerDeps struct {
	Consumer DrainConsumer
	Producer Producer
	// Limiter paces the replay against the target connector's ceiling. Nil replays with no pacing.
	Limiter RerouteLimiter
	// ConnectorID is this pool's connector; parked records for other connectors are skipped-and-committed.
	// uuid.Nil processes every record (tests).
	ConnectorID uuid.UUID
	// Retry is the backoff between ceiling re-checks for a saturated target. Zero uses 200ms.
	Retry  time.Duration
	Logger *slog.Logger
}

// NewDrainer builds a drainer. A nil producer or logger is a programming error (the caller wires real
// ones); a nil limiter simply removes pacing.
func NewDrainer(deps DrainerDeps) *Drainer {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Retry <= 0 {
		deps.Retry = defaultDrainRetry
	}
	return &Drainer{
		consumer:    deps.Consumer,
		producer:    deps.Producer,
		limiter:     deps.Limiter,
		connectorID: deps.ConnectorID,
		retry:       deps.Retry,
		logger:      deps.Logger,
	}
}

// Run consumes mt.reroute-park until ctx is cancelled, replaying each parked reroute to mt.routed at the
// target's controlled rate. It is a supervised worker with a context stop condition — it starts no
// unbounded goroutine.
func (d *Drainer) Run(ctx context.Context) error {
	return d.consumer.Run(ctx, d.handle)
}

func (d *Drainer) handle(ctx context.Context, rec kafka.Record) error {
	routed, err := pipeline.DecodeRouted(rec)
	if err != nil {
		return fmt.Errorf("connectorpool: drain decode mt.reroute-park: %w", err)
	}
	// Only replay parked records addressed to this pool's connector; another connector's pool drains its
	// own (option B). Skip-and-commit the rest.
	if d.connectorID != uuid.Nil && routed.ConnectorID != d.connectorID {
		return nil
	}

	// Pace to the target's ceiling: wait until it has capacity, so a drained burst never exceeds the
	// fallback connector's throughput_limit_per_sec. AllowConnector consumes the tokens when it succeeds.
	for d.limiter != nil && !d.limiter.AllowConnector(ctx, routed.ConnectorID, routed.SegmentCount) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.retry):
		}
	}

	out, err := pipeline.EncodeRouted(routed) // back onto mt.routed, keyed by message_id, chain in header
	if err != nil {
		return fmt.Errorf("connectorpool: drain encode: %w", err)
	}
	if err := d.producer.Produce(ctx, out); err != nil {
		return fmt.Errorf("connectorpool: drain replay: %w", err)
	}
	d.logger.InfoContext(ctx, "connector: replayed a parked reroute", "message_id", routed.MessageID, "connector_id", routed.ConnectorID)
	return nil
}
