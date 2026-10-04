package billing_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// queueFloorCrossing records one mo_charge that crossed the floor and returns its outbox event id.
func queueFloorCrossing(t *testing.T, repo *postgres.BillingRepo) (customerID, eventID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	pool := pgtest.Pool(t)
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ($1) RETURNING id`, "relay-"+uuid.NewString()).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	messageID := uuid.New()
	balanceAfter, floor := -12, -10
	if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMO,
		CustomerID: customerID, MessageID: &messageID, EntryType: cp.EntryMOCharge, Credits: -4,
		BalanceAfter: &balanceAfter, MOFloorReached: &floor,
	}); err != nil {
		t.Fatalf("record mo_charge: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT id FROM control_plane.billing_events_outbox WHERE customer_id = $1`, customerID).Scan(&eventID); err != nil {
		t.Fatalf("read queued event: %v", err)
	}
	return customerID, eventID
}

func outboxHolds(t *testing.T, eventID uuid.UUID) bool {
	t.Helper()
	var n int
	if err := pgtest.Pool(t).QueryRow(context.Background(),
		`SELECT count(*) FROM control_plane.billing_events_outbox WHERE id = $1`, eventID).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n == 1
}

// TestEventRelayLandsTheCrossingOnBillingEvents runs the real outbox, the real durable producer and a real
// broker: the fakes cannot tell whether the topic exists or whether the record survives the round trip.
func TestEventRelayLandsTheCrossingOnBillingEvents(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second, ProduceTimeout: 10 * time.Second}
	repo := postgres.NewBillingRepo(pgtest.Pool(t))
	customerID, eventID := queueFloorCrossing(t, repo)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(producer.Close)
	billing.NewEventRelay(repo, producer, quiet()).DrainOnce(context.Background())

	if outboxHolds(t, eventID) {
		t.Fatal("the event is still queued after a relay pass against a live broker")
	}

	consumer, err := kafka.NewConsumer(cfg, "billing-events-test-"+uuid.NewString(), kafka.TopicBillingEvents)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := make(chan kafka.Record, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, rec kafka.Record) error {
			var ev struct {
				EventID string `json:"event_id"`
			}
			if json.Unmarshal(rec.Value, &ev) == nil && ev.EventID == eventID.String() {
				select {
				case got <- rec:
				default:
				}
			}
			return nil
		})
	}()

	select {
	case rec := <-got:
		if want := "customer:" + customerID.String(); string(rec.Key) != want {
			t.Errorf("key = %q, want %q", rec.Key, want)
		}
	case <-ctx.Done():
		t.Fatalf("event %s never reached %s", eventID, kafka.TopicBillingEvents)
	}
}

// TestEventRelayKeepsTheCrossingWhileKafkaIsDown: an unreachable broker is a produce error, never a lost
// event — the row waits for the next pass and the lag shows it.
func TestEventRelayKeepsTheCrossingWhileKafkaIsDown(t *testing.T) {
	cfg := config.Kafka{Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, ProduceTimeout: 2 * time.Second}
	repo := postgres.NewBillingRepo(pgtest.Pool(t))
	_, eventID := queueFloorCrossing(t, repo)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(producer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	billing.NewEventRelay(repo, producer, quiet()).DrainOnce(ctx)

	if ctx.Err() != nil {
		t.Fatal("the relay pass did not return within its produce bound")
	}
	if !outboxHolds(t, eventID) {
		t.Error("the event left the outbox although no broker acknowledged it")
	}
	if lag := billing.OutboxLag(repo, time.Second)(); !(lag > 0) {
		t.Errorf("lag with an event waiting = %v, want > 0", lag)
	}
}
