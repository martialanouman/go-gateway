package router_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"

	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// windowReserver holds each reserve for a per-message delay and records the peak number in flight: the reserve
// is the wait step-285c overlaps inside a lane.
type windowReserver struct {
	mu       sync.Mutex
	delay    map[uuid.UUID]time.Duration
	calls    int
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (s *windowReserver) Reserve(_ context.Context, _, _, messageID uuid.UUID, _ int) (bool, string, error) {
	cur := s.inFlight.Add(1)
	for old := s.peak.Load(); cur > old && !s.peak.CompareAndSwap(old, cur); old = s.peak.Load() {
	}
	s.mu.Lock()
	s.calls++
	d, ok := s.delay[messageID]
	s.mu.Unlock()
	if !ok {
		d = 5 * time.Millisecond
	}
	time.Sleep(d)
	s.inFlight.Add(-1)
	return false, "", nil
}

func onePartition(t *testing.T, n int) ([]pipeline.InboundMT, []kafka.Record) {
	t.Helper()
	ins := make([]pipeline.InboundMT, n)
	recs := make([]kafka.Record, n)
	for i := range n {
		ins[i] = inbound("+2250700000000")
		recs[i] = onPartition(t, 0, int64(i), ins[i])
	}
	return ins, recs
}

// TestALaneOverlapsUpToItsWindowOfReserves: within ONE partition, reserves run concurrently, and never more
// than the window at once (step-285c). Before it, a lane reserved one message after the other and a
// customer's debit was capped at lanes ÷ reserve latency.
func TestALaneOverlapsUpToItsWindowOfReserves(t *testing.T) {
	_, recs := onePartition(t, 3*router.LaneWindow)
	res := &windowReserver{}
	prod := &fakeProducer{}
	cons := &oneBatchConsumer{records: recs}
	if err := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, res, prod, &fakeCDR{}, cons).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(prod.produced); got != len(recs) {
		t.Fatalf("produced %d records, want %d", got, len(recs))
	}
	if peak := res.peak.Load(); peak != router.LaneWindow {
		t.Errorf("peak concurrent reserves in one lane = %d, want the window %d", peak, router.LaneWindow)
	}
}

// TestALanePublishesInOffsetOrderWhenReservesFinishOutOfOrder: the first message reserves slowest, so the
// whole window finishes before it; publication still follows the offsets.
func TestALanePublishesInOffsetOrderWhenReservesFinishOutOfOrder(t *testing.T) {
	ins, recs := onePartition(t, router.LaneWindow)
	res := &windowReserver{delay: map[uuid.UUID]time.Duration{}}
	for i, in := range ins {
		res.delay[in.MessageID] = time.Duration(len(ins)-i) * 5 * time.Millisecond
	}
	prod := &orderingProducer{delay: map[string]time.Duration{}}
	cons := &oneBatchConsumer{records: recs}
	if err := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, res, prod, &fakeCDR{}, cons).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.order) != len(ins) {
		t.Fatalf("produced %d records, want %d", len(prod.order), len(ins))
	}
	for i, in := range ins {
		if prod.order[i] != string(in.MessageID[:]) {
			t.Fatalf("position %d published out of offset order", i)
		}
	}
}

// TestAFailedLaneStopsReservingAboveIt: once a lane fails, it launches nothing new. Only the window already in
// flight was reserved; the rest of the partition waits for redelivery, unreserved. The router.process span of
// every record already through the pipeline still ends, marked failed: an unended one would leave its stage
// spans exported without their parent.
func TestAFailedLaneStopsReservingAboveIt(t *testing.T) {
	ins, recs := onePartition(t, 3*router.LaneWindow)
	res := &windowReserver{}
	prod := &failingProducer{failID: string(ins[0].MessageID[:])}
	cons := &oneBatchConsumer{records: recs}
	spans := otelrec.New(t)
	tracer := observability.Tracer(spans.Provider(), "router")
	r := router.New(router.Deps{
		Consumer: cons, Producer: prod, CDR: &fakeCDR{}, Tracer: tracer,
		Pipeline: pipeline.New(pipeline.Deps{
			Tracer: tracer, Resolver: stubResolver{conn: uuid.New()}, SenderIDs: allowAllSenderIDs{},
			OptOut: allowAllOptOut{}, Antispam: allowAllAntispam{}, Credit: res,
		}),
	})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ended := 0
	for _, sp := range spans.Ended() {
		if sp.Name() != "router.process" {
			continue
		}
		ended++
		if sp.Status().Code != codes.Error {
			t.Errorf("a router.process span of the failed window ended as %v, want Error", sp.Status().Code)
		}
	}
	if ended != router.LaneWindow {
		t.Errorf("ended router.process spans = %d, want %d (the failure and every record staged above it)", ended, router.LaneWindow)
	}
	if len(prod.produced) != 0 {
		t.Errorf("published %d records above the failure, want none", len(prod.produced))
	}
	if res.calls != router.LaneWindow {
		t.Errorf("reserves = %d, want the window %d already in flight and nothing launched after the failure", res.calls, router.LaneWindow)
	}
	for i, err := range cons.results[1:] {
		if err == nil {
			t.Errorf("record %d above the failure reported as handled", i+1)
		}
	}
}
