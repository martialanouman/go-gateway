package billing_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type fakeOutbox struct {
	queued    []cp.BillingEvent
	calls     int
	oldestErr error
}

func (f *fakeOutbox) RelayBillingEvents(ctx context.Context, limit int, publish func(context.Context, []cp.BillingEvent) error) (int, error) {
	f.calls++
	batch := f.queued[:min(limit, len(f.queued))]
	if len(batch) == 0 {
		return 0, nil
	}
	if err := publish(ctx, batch); err != nil {
		return 0, err
	}
	f.queued = f.queued[len(batch):]
	return len(batch), nil
}

func (f *fakeOutbox) OldestPendingBillingEvent(context.Context) (time.Time, bool, error) {
	if f.oldestErr != nil {
		return time.Time{}, false, f.oldestErr
	}
	if len(f.queued) == 0 {
		return time.Time{}, false, nil
	}
	return f.queued[0].CreatedAt, true, nil
}

type recordingProducer struct {
	records []kafka.Record
	err     error
}

func (p *recordingProducer) Produce(_ context.Context, rec kafka.Record) error {
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, rec)
	return nil
}

func floorEvent(at time.Time) cp.BillingEvent {
	return cp.BillingEvent{
		ID: uuid.New(), OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: uuid.New(), CustomerID: uuid.New(),
		BalanceAfter: -12, Floor: -10, CreatedAt: at,
	}
}

func TestEventRelayPublishesTheTransitionAndItsState(t *testing.T) {
	// pgx hands timestamptz back in the local zone: the wire value must still be UTC.
	at := time.Date(2026, 10, 4, 14, 30, 0, 123_000_000, time.FixedZone("UTC+2", 2*3600))
	ev := floorEvent(at)
	store := &fakeOutbox{queued: []cp.BillingEvent{ev, floorEvent(at)}}
	producer := &recordingProducer{}

	billing.NewEventRelay(store, producer, quiet()).DrainOnce(context.Background())

	if len(producer.records) != 2 {
		t.Fatalf("records = %d, want 2: the outbox deletes the whole batch it handed over", len(producer.records))
	}
	rec := producer.records[0]
	if rec.Topic != kafka.TopicBillingEvents {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicBillingEvents)
	}
	if want := "smpp_account:" + ev.OwnerID.String(); string(rec.Key) != want {
		t.Errorf("key = %q, want %q: one owner's transitions stay on one partition", rec.Key, want)
	}
	if len(rec.Headers) != 1 || rec.Headers[0].Key != kafka.HeaderCustomerID || string(rec.Headers[0].Value) != ev.CustomerID.String() {
		t.Errorf("headers = %+v, want only customer_id=%s", rec.Headers, ev.CustomerID)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Value, &got); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	want := map[string]any{
		"v": 1.0, "event_id": ev.ID.String(), "event": "mo_balance_floor_reached",
		"customer_id": ev.CustomerID.String(), "owner_type": "smpp_account", "owner_id": ev.OwnerID.String(),
		"direction": "mo", "balance_after": -12.0, "floor": -10.0, "occurred_at": "2026-10-04T12:30:00.123Z",
	}
	if len(got) != len(want) {
		t.Errorf("fields = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if len(store.queued) != 0 {
		t.Errorf("queued after a published relay = %d, want 0", len(store.queued))
	}
}

func TestEventRelayDrainsABacklogWithinABoundedPass(t *testing.T) {
	queue := func(n int) *fakeOutbox {
		queued := make([]cp.BillingEvent, n)
		for i := range queued {
			queued[i] = floorEvent(time.Now())
		}
		return &fakeOutbox{queued: queued}
	}
	backlog := queue(203)
	billing.NewEventRelay(backlog, &recordingProducer{}, quiet()).DrainOnce(context.Background())
	if backlog.calls != 3 || len(backlog.queued) != 0 {
		t.Errorf("relay calls = %d with %d left, want 3 calls and none left", backlog.calls, len(backlog.queued))
	}

	flood := queue(1001)
	billing.NewEventRelay(flood, &recordingProducer{}, quiet()).DrainOnce(context.Background())
	if flood.calls != 10 || len(flood.queued) != 1 {
		t.Errorf("relay calls = %d with %d left, want 10 calls and 1 left: a pass is bounded", flood.calls, len(flood.queued))
	}
}

func TestEventRelayKeepsTheEventWhenKafkaFails(t *testing.T) {
	store := &fakeOutbox{queued: []cp.BillingEvent{floorEvent(time.Now())}}
	billing.NewEventRelay(store, &recordingProducer{err: errors.New("broker down")}, quiet()).DrainOnce(context.Background())
	if len(store.queued) != 1 {
		t.Errorf("queued after a failed produce = %d, want 1", len(store.queued))
	}
}

// TestOutboxLagIsReadAtScrapeTime: a produce in flight is unbounded on a degraded broker, so a lag the
// relay pass set at its end would freeze while the pass hangs. Read at scrape, it keeps growing.
func TestOutboxLagIsReadAtScrapeTime(t *testing.T) {
	if got := billing.OutboxLag(&fakeOutbox{}, time.Second)(); got != 0 {
		t.Errorf("lag of an empty outbox = %v, want 0", got)
	}
	stuck := &fakeOutbox{queued: []cp.BillingEvent{floorEvent(time.Now().Add(-90 * time.Second))}}
	if got := billing.OutboxLag(stuck, time.Second)(); got < 90 {
		t.Errorf("lag with a 90 s old event = %v, want ≥ 90", got)
	}
	if got := billing.OutboxLag(&fakeOutbox{oldestErr: errors.New("postgres down")}, time.Second)(); !math.IsNaN(got) {
		t.Errorf("lag on a read error = %v, want NaN: 0 would read as a healthy relay", got)
	}
}
