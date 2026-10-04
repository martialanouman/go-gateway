package billing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

const eventRelayBatch = 100

// EventOutbox is the durable queue of billing transitions (step-400); *postgres.BillingRepo satisfies it.
type EventOutbox interface {
	RelayBillingEvents(ctx context.Context, limit int, publish func(context.Context, []cp.BillingEvent) error) (int, error)
	OldestPendingBillingEvent(ctx context.Context) (time.Time, bool, error)
}

// EventProducer is the durable Kafka producer the relay publishes with.
type EventProducer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// EventRelay moves queued billing transitions from the outbox to billing.events. A transition reaches the
// topic at least once: a crash between the produce and the outbox delete, or two replicas reading the same
// row, publish it again.
type EventRelay struct {
	store    EventOutbox
	producer EventProducer
	logger   *slog.Logger
}

// NewEventRelay builds a relay over the outbox.
func NewEventRelay(store EventOutbox, producer EventProducer, logger *slog.Logger) *EventRelay {
	return &EventRelay{store: store, producer: producer, logger: logger}
}

// DrainOnce relays a bounded pass. A failed relay keeps its events queued for the next pass.
func (r *EventRelay) DrainOnce(ctx context.Context) {
	drainBatches(ctx, r.logger, "events relay", eventRelayBatch, func(ctx context.Context, limit int) (int64, error) {
		n, err := r.store.RelayBillingEvents(ctx, limit, r.publish)
		return int64(n), err
	})
}

// OutboxLag reads the age of the oldest queued event, for a gauge evaluated at scrape time: a produce in
// flight is unbounded on a degraded broker (kafka.Producer.Produce), so a lag set at the end of a relay pass
// would freeze while the pass hangs. NaN on a read error, which a healthy 0 must not hide.
func OutboxLag(store EventOutbox, timeout time.Duration) func() float64 {
	return func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		oldest, pending, err := store.OldestPendingBillingEvent(ctx)
		if err != nil {
			return math.NaN()
		}
		if !pending {
			return 0
		}
		return max(0, time.Since(oldest).Seconds())
	}
}

func (r *EventRelay) publish(ctx context.Context, events []cp.BillingEvent) error {
	for _, ev := range events {
		value, err := json.Marshal(floorEventRecord{
			V: 1, EventID: ev.ID.String(), Event: "mo_balance_floor_reached",
			CustomerID: ev.CustomerID.String(), OwnerType: ev.OwnerType, OwnerID: ev.OwnerID.String(),
			Direction: directionMO, BalanceAfter: ev.BalanceAfter, Floor: ev.Floor, OccurredAt: ev.CreatedAt.UTC(),
		})
		if err != nil {
			return fmt.Errorf("billing: encode billing event %s: %w", ev.ID, err)
		}
		if err := r.producer.Produce(ctx, kafka.Record{
			Topic:   kafka.TopicBillingEvents,
			Key:     []byte(ev.OwnerType + ":" + ev.OwnerID.String()),
			Value:   value,
			Headers: []kafka.Header{{Key: kafka.HeaderCustomerID, Value: []byte(ev.CustomerID.String())}},
		}); err != nil {
			return err
		}
	}
	return nil
}

// floorEventRecord is the billing.events wire format, v1. It carries the state at the transition, not just
// the transition, so a consumer replaying an old offset can tell a stale crossing from a fresh one.
type floorEventRecord struct {
	V            int       `json:"v"`
	EventID      string    `json:"event_id"`
	Event        string    `json:"event"`
	CustomerID   string    `json:"customer_id"`
	OwnerType    string    `json:"owner_type"`
	OwnerID      string    `json:"owner_id"`
	Direction    string    `json:"direction"`
	BalanceAfter int       `json:"balance_after"`
	Floor        int       `json:"floor"`
	OccurredAt   time.Time `json:"occurred_at"`
}
