package router_test

import (
	"context"
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

// laneWindow is the design's figure, written out rather than read from the router: a test that follows the
// constant would stay green at 1, the behaviour step-285c replaced.
const laneWindow = 8

// windowReserver holds the first laneWindow reserves until all of them have arrived (or a guard elapses), so
// the peak it records is the window itself and not whatever overlap the scheduler happened to allow. Past the
// barrier, every reserve takes its per-message delay.
type windowReserver struct {
	delay    map[uuid.UUID]time.Duration // written before Run, only read during it
	calls    atomic.Int32
	inFlight atomic.Int32
	peak     atomic.Int32
	full     chan struct{}
}

func newWindowReserver() *windowReserver {
	return &windowReserver{delay: map[uuid.UUID]time.Duration{}, full: make(chan struct{})}
}

func (s *windowReserver) Reserve(_ context.Context, _, _, messageID uuid.UUID, _ int) (bool, string, error) {
	cur := s.inFlight.Add(1)
	for old := s.peak.Load(); cur > old && !s.peak.CompareAndSwap(old, cur); old = s.peak.Load() {
	}
	if s.calls.Add(1) == laneWindow {
		close(s.full)
	}
	select {
	case <-s.full:
	case <-time.After(2 * time.Second):
	}
	time.Sleep(s.delay[messageID])
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
	_, recs := onePartition(t, 3*laneWindow)
	res := newWindowReserver()
	prod := &fakeProducer{}
	cons := &oneBatchConsumer{records: recs}
	if err := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, res, prod, &fakeCDR{}, cons).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(prod.produced); got != len(recs) {
		t.Fatalf("produced %d records, want %d", got, len(recs))
	}
	if peak := res.peak.Load(); peak != laneWindow {
		t.Errorf("peak concurrent reserves in one lane = %d, want the window %d", peak, laneWindow)
	}
}

// TestALanePublishesInOffsetOrderWhenReservesFinishOutOfOrder: the first message reserves slowest, so the
// whole window finishes before it; publication still follows the offsets.
func TestALanePublishesInOffsetOrderWhenReservesFinishOutOfOrder(t *testing.T) {
	ins, recs := onePartition(t, laneWindow)
	res := newWindowReserver()
	for i, in := range ins {
		res.delay[in.MessageID] = time.Duration(len(ins)-i) * 5 * time.Millisecond
	}
	prod := &fakeProducer{}
	cons := &oneBatchConsumer{records: recs}
	if err := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, res, prod, &fakeCDR{}, cons).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Without it the order below would hold trivially on a lane that reserves one message at a time.
	if peak := res.peak.Load(); peak <= 1 {
		t.Fatalf("peak concurrent reserves = %d: the reserves never overlapped, so they could not finish out of order", peak)
	}
	if len(prod.produced) != len(ins) {
		t.Fatalf("produced %d records, want %d", len(prod.produced), len(ins))
	}
	for i, in := range ins {
		if string(prod.produced[i].Key) != string(in.MessageID[:]) {
			t.Fatalf("position %d published out of offset order", i)
		}
	}
}

// TestAFailedLaneStopsReservingAboveIt: the produce of offset 3 fails. What precedes it is published; nothing
// above it is, and the lane launches nothing new after it. The
// router.process span of every record already through the pipeline still ends, marked failed: an unended one
// would leave its stage spans exported without their parent.
func TestAFailedLaneStopsReservingAboveIt(t *testing.T) {
	const failAt = 3
	ins, recs := onePartition(t, 3*laneWindow)
	res := newWindowReserver()
	prod := &failingProducer{failID: string(ins[failAt].MessageID[:])}
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

	if len(prod.produced) != failAt {
		t.Errorf("published %d records, want the %d below the failure", len(prod.produced), failAt)
	}
	for i, id := range prod.produced {
		if id != string(ins[i].MessageID[:]) {
			t.Errorf("published record %d is not offset %d", i, i)
		}
	}
	// One launch per publication below the failure, on top of the initial window.
	if got := int(res.calls.Load()); got != laneWindow+failAt {
		t.Errorf("reserves = %d, want %d: nothing may be launched after the failure", got, laneWindow+failAt)
	}
	for i, err := range cons.results {
		if (i < failAt) != (err == nil) {
			t.Errorf("record %d result = %v: only the records below the failure are handled", i, err)
		}
	}

	ended, failed := 0, 0
	for _, sp := range spans.Ended() {
		if sp.Name() == "router.process" {
			ended++
			if sp.Status().Code == codes.Error {
				failed++
			}
		}
	}
	if ended != laneWindow+failAt || failed != laneWindow {
		t.Errorf("router.process spans ended = %d (failed %d), want %d (failed %d): the failure and every record staged above it",
			ended, failed, laneWindow+failAt, laneWindow)
	}
}
