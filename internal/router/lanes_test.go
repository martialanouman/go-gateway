package router_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// This file covers the router's batch fan-out (step-201d PR2): the consume loop runs ONE goroutine per
// Kafka partition present in a poll batch, instead of processing every record on a single goroutine.
//
// The lane is the partition, deliberately, and TestAFaultNeverStrandsAProducedRecordAboveIt is the test
// that says why. Sharding by account key would have parallelised too, but a fault in one lane would then
// leave records that OTHER lanes had already published sitting above it, uncommitted and therefore
// destined to be republished — turning a fault into ~a poll of duplicate SMS per partition. With one
// lane per partition, a fault stops the only goroutine that could have touched the records above it, so
// nothing already published is ever redelivered.

// oneBatchConsumer hands every record over in a single poll batch — so the lanes really do run at the
// same time — and keeps the per-record verdict, which is what the halt property is read from.
//
// It deliberately does NOT mirror kafka.RunBatch's error propagation: these tests assert on results,
// record by record, and RunBatch's own behaviour is covered in the kafka package.
type oneBatchConsumer struct {
	records []kafka.Record
	results []error
}

func (c *oneBatchConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	c.results = handle(ctx, c.records)
	return nil
}

// onPartition encodes an inbound message onto a chosen partition and offset.
//
// The partition is CHOSEN, never left to chance: a lane the fixture forgot to fill is a lane the test
// silently stops exercising, and the assertion would then pass for the wrong reason.
func onPartition(t *testing.T, partition int32, offset int64, in pipeline.InboundMT) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode mt.inbound: %v", err)
	}
	rec.Partition = partition
	rec.Offset = offset
	return rec
}

// blockingProducer records the peak number of Produce calls in flight at once. The sleep is
// load-bearing: without a delay inside Produce the calls finish faster than they overlap, and a
// concurrent implementation would still peak at 1.
type blockingProducer struct {
	mu       sync.Mutex
	produced []kafka.Record
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (p *blockingProducer) Produce(_ context.Context, rec kafka.Record) error {
	cur := p.inFlight.Add(1)
	for {
		old := p.peak.Load()
		if cur <= old || p.peak.CompareAndSwap(old, cur) {
			break
		}
	}
	time.Sleep(3 * time.Millisecond) // stands in for the acks=all broker round-trip
	p.mu.Lock()
	p.produced = append(p.produced, rec)
	p.mu.Unlock()
	p.inFlight.Add(-1)
	return nil
}

// TestRouterLanesRaiseConcurrency: records spread over four partitions are produced concurrently, and
// records confined to one partition are not.
//
// The one-partition arm is not decoration. It proves the counter CAN report 1 — without it, a peak of 4
// on the other arm could be an artefact of the fixture rather than a property of the router.
func TestRouterLanesRaiseConcurrency(t *testing.T) {
	const perPartition = 5

	for _, tc := range []struct {
		name       string
		partitions int32
		wantPeak   func(int32) bool
		explain    string
	}{
		{"one partition", 1, func(peak int32) bool { return peak == 1 }, "a single partition is a single lane, so nothing may overlap"},
		{"four partitions", 4, func(peak int32) bool { return peak > 1 }, "four partitions are four lanes, so produces must overlap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs := make([]kafka.Record, 0, int(tc.partitions)*perPartition)
			for p := range tc.partitions {
				for i := range perPartition {
					recs = append(recs, onPartition(t, p, int64(i), inbound("+2250700000000")))
				}
			}
			prod := &blockingProducer{}
			cons := &oneBatchConsumer{records: recs}
			r := newRouter(t, stubResolver{conn: uuid.New()}, prod, &fakeCDR{}, cons)

			if err := r.Run(context.Background()); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := len(prod.produced); got != int(tc.partitions)*perPartition {
				t.Fatalf("produced %d records, want %d — the batch was not fully processed",
					got, int(tc.partitions)*perPartition)
			}
			if peak := prod.peak.Load(); !tc.wantPeak(peak) {
				t.Errorf("peak concurrent Produce = %d: %s", peak, tc.explain)
			}
		})
	}
}

// orderingProducer records what it was handed, and sleeps LONGER for the records that come first.
//
// That is what makes the ordering assertion deterministic instead of probabilistic: under an
// implementation that processes a partition's records in parallel, the last record of the partition
// finishes before the first one ALWAYS, not merely often.
type orderingProducer struct {
	mu       sync.Mutex
	order    []string
	delay    map[string]time.Duration
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (p *orderingProducer) Produce(_ context.Context, rec kafka.Record) error {
	cur := p.inFlight.Add(1)
	for {
		old := p.peak.Load()
		if cur <= old || p.peak.CompareAndSwap(old, cur) {
			break
		}
	}
	id := string(rec.Key)
	p.mu.Lock()
	d := p.delay[id]
	p.mu.Unlock()
	time.Sleep(d)
	p.mu.Lock()
	p.order = append(p.order, id)
	p.mu.Unlock()
	p.inFlight.Add(-1)
	return nil
}

// TestRouterKeepsAPartitionInOrderUnderLoad: a partition's records are published in offset order, while
// another partition's records run alongside them.
//
// The second half of that sentence is asserted too. Without it the test would pass on a fully sequential
// router and prove nothing about the fan-out it is meant to guard.
func TestRouterKeepsAPartitionInOrderUnderLoad(t *testing.T) {
	const n = 5

	recs := make([]kafka.Record, 0, 2*n)
	ordered := make([]string, 0, n)
	delay := make(map[string]time.Duration, 2*n)
	for i := range n {
		in := inbound("+2250700000000")
		recs = append(recs, onPartition(t, 0, int64(i), in))
		// mt.routed is keyed by message id, so the producer sees the message id, not the account.
		id := string(in.MessageID[:])
		ordered = append(ordered, id)
		delay[id] = time.Duration(n-i) * 8 * time.Millisecond // 40ms, 32ms, … 8ms

		other := inbound("+2250700000001")
		recs = append(recs, onPartition(t, 1, int64(i), other))
		delay[string(other.MessageID[:])] = time.Millisecond
	}

	prod := &orderingProducer{delay: delay}
	cons := &oneBatchConsumer{records: recs}
	r := newRouter(t, stubResolver{conn: uuid.New()}, prod, &fakeCDR{}, cons)
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The subsequence belonging to partition 0 must be exactly the order it was submitted in.
	inP0 := make(map[string]bool, n)
	for _, id := range ordered {
		inP0[id] = true
	}
	var got []string
	for _, id := range prod.order {
		if inP0[id] {
			got = append(got, id)
		}
	}
	if len(got) != n {
		t.Fatalf("partition 0 produced %d records, want %d", len(got), n)
	}
	for i := range got {
		if got[i] != ordered[i] {
			t.Fatalf("partition 0 came out in position %d out of order: a lane must process its records "+
				"sequentially, in offset order", i)
		}
	}
	// Without this the test would pass on a fully sequential router and prove nothing about the fan-out it
	// exists to guard: the ordering above is trivially true when nothing runs at the same time.
	if peak := prod.peak.Load(); peak <= 1 {
		t.Errorf("peak concurrent Produce = %d: the two lanes never overlapped, so the ordering assertion "+
			"above was checked on a sequential run and proved nothing", peak)
	}
}

// failingProducer fails one chosen message id, and records every message it was ever asked to publish.
//
// The id is chosen rather than counted: the lanes run concurrently, so "fail the second call" would pick
// a different record from run to run and the test would flake.
type failingProducer struct {
	mu       sync.Mutex
	failID   string
	produced []string
}

var errProduceRefused = errors.New("router_test: producer refused")

func (p *failingProducer) Produce(_ context.Context, rec kafka.Record) error {
	id := string(rec.Key)
	if id == p.failID {
		return errProduceRefused
	}
	p.mu.Lock()
	p.produced = append(p.produced, id)
	p.mu.Unlock()
	return nil
}

// TestAFaultNeverStrandsAProducedRecordAboveIt is the property the lane choice exists for.
//
// For every partition, no record whose offset is ABOVE that partition's first failure may have reached
// the producer. Those records are not committable (committablePrefix refuses them), so anything already
// published there would be republished on redelivery — a duplicate SMS on a subscriber's handset, which
// ADR-0012 bounds and this design refuses to widen.
//
// It holds because the lane IS the partition: the goroutine that failed is the only one that could have
// touched the records above it, and it stopped. It would NOT hold under a lane keyed by account.
func TestAFaultNeverStrandsAProducedRecordAboveIt(t *testing.T) {
	// Partition 0: three records, the MIDDLE one fails. Partition 1: two records, both fine.
	p0 := []pipeline.InboundMT{inbound("+2250700000000"), inbound("+2250700000001"), inbound("+2250700000002")}
	p1 := []pipeline.InboundMT{inbound("+2250700000003"), inbound("+2250700000004")}

	recs := make([]kafka.Record, 0, len(p0)+len(p1))
	for i, in := range p0 {
		recs = append(recs, onPartition(t, 0, int64(i), in))
	}
	for i, in := range p1 {
		recs = append(recs, onPartition(t, 1, int64(i), in))
	}

	prod := &failingProducer{failID: string(p0[1].MessageID[:])}
	cons := &oneBatchConsumer{records: recs}
	r := newRouter(t, stubResolver{conn: uuid.New()}, prod, &fakeCDR{}, cons)

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	published := make(map[string]bool, len(prod.produced))
	for _, id := range prod.produced {
		published[id] = true
	}
	// The record above the failure, in the SAME partition, must never have been published.
	if published[string(p0[2].MessageID[:])] {
		t.Error("a record above the failure in its own partition was published: it is not committable, so " +
			"redelivery will publish it a second time — a duplicated SMS (ADR-0012)")
	}
	// The record below the failure was legitimately published and will commit.
	if !published[string(p0[0].MessageID[:])] {
		t.Error("the record below the failure was not published: a fault must not hold back work that " +
			"precedes it")
	}
	// A sibling partition is untouched by another partition's fault.
	for _, in := range p1 {
		if !published[string(in.MessageID[:])] {
			t.Errorf("partition 1 record %s was not published: a fault in partition 0 must not stop a "+
				"sibling lane", in.MessageID)
		}
	}

	// And the verdict the consumer got back must let committablePrefix do the rest.
	if len(cons.results) != len(recs) {
		t.Fatalf("handler returned %d results for %d records: the BatchHandler contract requires one per "+
			"record", len(cons.results), len(recs))
	}
	if cons.results[1] == nil {
		t.Error("the failing record was reported as handled: its offset would commit and the message would " +
			"be lost")
	}
	if cons.results[2] == nil {
		t.Error("the record above the failure was reported as handled: committablePrefix would let its " +
			"offset commit and skip the failure underneath it")
	}
	for i := 3; i < len(recs); i++ {
		if cons.results[i] != nil {
			t.Errorf("partition 1 record %d reported a fault it never had", i-3)
		}
	}
}
