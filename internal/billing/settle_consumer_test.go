package billing_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// syncSettler records settlements concurrently and fails the messages it is told to.
type syncSettler struct {
	mu       sync.Mutex
	captured []uuid.UUID
	released []uuid.UUID
	owners   map[uuid.UUID]billing.Owner
	failFor  map[uuid.UUID]error
}

func (s *syncSettler) record(list *[]uuid.UUID, owner billing.Owner, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failFor[id]; err != nil {
		return err
	}
	if s.owners == nil {
		s.owners = map[uuid.UUID]billing.Owner{}
	}
	s.owners[id] = owner
	*list = append(*list, id)
	return nil
}

func (s *syncSettler) Capture(_ context.Context, owner billing.Owner, id uuid.UUID) (int, error) {
	return 1, s.record(&s.captured, owner, id)
}

func (s *syncSettler) Release(_ context.Context, owner billing.Owner, id uuid.UUID) error {
	return s.record(&s.released, owner, id)
}

// batchOnce hands its records to the handler as one poll batch and keeps the per-record results.
type batchOnce struct {
	recs    []kafka.Record
	results []error
}

func (b *batchOnce) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	b.results = handle(ctx, b.recs)
	return nil
}

func outcomeRecord(t *testing.T, status string, billable bool, ownerType string) (kafka.Record, pipeline.OutcomeMT) {
	t.Helper()
	ev := pipeline.OutcomeMT{
		MessageID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(), ConnectorID: uuid.New(),
		SegmentSeq: 1, SegmentCount: 1, SubmittedAt: time.Now().UTC(), Status: status,
		Billable: billable, OwnerType: ownerType,
	}
	rec, err := pipeline.EncodeOutcome(ev)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec, ev
}

// TestSettleConsumerSettlesFromTheOutcome: billing-svc, not the pool, settles a sent message (step-287d).
// enroute captures and failed releases, against the balance the reservation was made on; a message with no
// reservation costs no billing call.
func TestSettleConsumerSettlesFromTheOutcome(t *testing.T) {
	sent, sentEv := outcomeRecord(t, "enroute", true, cp.OwnerTypeSMPPAccount)
	refused, refusedEv := outcomeRecord(t, "failed", true, cp.OwnerTypeCustomer)
	free, _ := outcomeRecord(t, "enroute", false, "")
	settler := &syncSettler{}
	cons := &batchOnce{recs: []kafka.Record{sent, refused, free}}

	if err := billing.NewSettleConsumer(cons, settler, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(settler.captured) != 1 || settler.captured[0] != sentEv.MessageID {
		t.Errorf("captured %v, want only %s", settler.captured, sentEv.MessageID)
	}
	if len(settler.released) != 1 || settler.released[0] != refusedEv.MessageID {
		t.Errorf("released %v, want only %s", settler.released, refusedEv.MessageID)
	}
	if got := settler.owners[sentEv.MessageID]; got.Type != cp.OwnerTypeSMPPAccount || got.ID != sentEv.AccountID ||
		got.CustomerID != sentEv.CustomerID {
		t.Errorf("capture owner = %+v, want the smpp_account balance of %s", got, sentEv.AccountID)
	}
	if got := settler.owners[refusedEv.MessageID]; got.Type != cp.OwnerTypeCustomer || got.ID != refusedEv.CustomerID ||
		got.AccountID == nil || *got.AccountID != refusedEv.AccountID {
		t.Errorf("release owner = %+v, want the customer balance of %s, account kept", got, refusedEv.CustomerID)
	}
	for i, err := range cons.results {
		if err != nil {
			t.Errorf("record %d failed: %v", i, err)
		}
	}
}

// TestSettleConsumerReplaysAFailedSettlement: a billing fault fails its own record, which is replayed on
// this group's offsets — never on mt.routed, so it can never re-send an SMS.
func TestSettleConsumerReplaysAFailedSettlement(t *testing.T) {
	ok, _ := outcomeRecord(t, "enroute", true, cp.OwnerTypeCustomer)
	bad, badEv := outcomeRecord(t, "enroute", true, cp.OwnerTypeCustomer)
	settler := &syncSettler{failFor: map[uuid.UUID]error{badEv.MessageID: errors.New("redis down")}}
	cons := &batchOnce{recs: []kafka.Record{ok, bad}}

	_ = billing.NewSettleConsumer(cons, settler, nil).Run(context.Background())
	if cons.results[0] != nil || cons.results[1] == nil {
		t.Errorf("results = %v, want only the failed settlement to be replayed", cons.results)
	}
}

type settleTimes struct {
	mu sync.Mutex
	n  map[string]int
}

func (s *settleTimes) Observe(action string, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == nil {
		s.n = map[string]int{}
	}
	s.n[action]++
}

// TestSettleConsumerTimesEachSettlement: the consumer settled ~85 messages/s for 275 published (step-287
// run 2); each settlement is timed by action, lock and durable write included (step-287e).
func TestSettleConsumerTimesEachSettlement(t *testing.T) {
	sent, _ := outcomeRecord(t, "enroute", true, cp.OwnerTypeCustomer)
	refused, _ := outcomeRecord(t, "failed", true, cp.OwnerTypeCustomer)
	times := &settleTimes{}
	cons := &batchOnce{recs: []kafka.Record{sent, refused}}
	if err := billing.NewSettleConsumer(cons, &syncSettler{}, nil, billing.WithSettleTimes(times)).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if times.n["capture"] != 1 || times.n["release"] != 1 {
		t.Errorf("timed %v, want one capture and one release", times.n)
	}
}
