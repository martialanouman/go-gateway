package billing

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// settleConcurrency bounds the settlements in flight for one poll batch: each one holds a terminal lock
// and a Postgres round trip, and the batcher behind RecordDurable groups what arrives together.
const settleConcurrency = 32

// OutcomeBatchConsumer reads mt.outcome a poll batch at a time. *kafka.Consumer satisfies it.
type OutcomeBatchConsumer interface {
	RunBatch(ctx context.Context, handle kafka.BatchHandler) error
}

// SettleConsumer settles MT reservations from mt.outcome (step-287d): a submitted message is captured, a
// refused one released. It takes the settlement off the connector pool's send path, where it cost ~173 ms
// a message and timed out four times in five. A billing fault fails its own record, replayed on this
// group's offsets — never on mt.routed, so it can never re-send an SMS.
type SettleConsumer struct {
	consumer OutcomeBatchConsumer
	settler  ReaperSettler
	times    SettleTimes
	logger   *slog.Logger
}

// SettleTimes times one settlement by action (capture|release), lock and durable write included (step-287e).
type SettleTimes interface {
	Observe(action string, d time.Duration)
}

// SettleOption configures a SettleConsumer.
type SettleOption func(*SettleConsumer)

// WithSettleTimes wires the settlement timer; without it nothing is timed.
func WithSettleTimes(t SettleTimes) SettleOption {
	return func(c *SettleConsumer) { c.times = t }
}

// NewSettleConsumer builds the consumer. A nil logger defaults to slog.Default.
func NewSettleConsumer(consumer OutcomeBatchConsumer, settler ReaperSettler, logger *slog.Logger, opts ...SettleOption) *SettleConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	c := &SettleConsumer{consumer: consumer, settler: settler, logger: logger}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Run consumes mt.outcome until ctx is cancelled.
func (c *SettleConsumer) Run(ctx context.Context) error {
	return c.consumer.RunBatch(ctx, c.handleBatch)
}

func (c *SettleConsumer) handleBatch(ctx context.Context, recs []kafka.Record) []error {
	results := make([]error, len(recs))
	sem := make(chan struct{}, settleConcurrency)
	var wg sync.WaitGroup
	for i, rec := range recs {
		ev, err := pipeline.DecodeOutcome(rec)
		if err != nil {
			c.logger.ErrorContext(ctx, "decode mt.outcome for settlement: skipping corrupt record", "err", err)
			continue
		}
		action := decideFromStatus(ev.Status)
		// A record without a reservation — billing disabled, or produced before mt.outcome carried one — is
		// not ours to settle; the reaper is the net for the second case.
		if !ev.Billable || action == actionNone {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = c.settle(ctx, action, ev)
		}()
	}
	wg.Wait()
	return results
}

func (c *SettleConsumer) settle(ctx context.Context, action reaperAction, ev pipeline.OutcomeMT) error {
	owner := ownerOf(ev.OwnerType, ev.CustomerID, ev.AccountID)
	start := time.Now()
	var err error
	if action == actionCapture {
		_, err = c.settler.Capture(ctx, owner, ev.MessageID)
	} else {
		err = c.settler.Release(ctx, owner, ev.MessageID)
	}
	if c.times != nil {
		c.times.Observe(action.String(), time.Since(start))
	}
	if err != nil && ctx.Err() == nil {
		c.logger.WarnContext(ctx, "billing: settlement from mt.outcome failed; will reprocess",
			"message_id", ev.MessageID, "status", ev.Status, "err", err)
	}
	return err
}

// ownerOf rebuilds the balance owner the reservation was made against, from the owner_type pinned on the
// message and its customer/account ids — the key connector-pool's settler builds (step-145).
func ownerOf(ownerType string, customerID, accountID uuid.UUID) Owner {
	if ownerType == cp.OwnerTypeSMPPAccount {
		return Owner{Type: ownerType, ID: accountID, CustomerID: customerID, AccountID: &accountID}
	}
	return Owner{Type: cp.OwnerTypeCustomer, ID: customerID, CustomerID: customerID, AccountID: &accountID}
}
