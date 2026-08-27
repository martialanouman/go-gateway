//go:build loadref

package e2e_test

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
)

// poolCeilingPartitions is fixed, and it is not a free choice. The pool's fan-out is per POLL BATCH —
// one goroutine per shard present in the batch — and the batch is bounded by FetchMaxPartitionBytes
// (56 KiB, ADR-0012) PER PARTITION. Fewer partitions means smaller batches means fewer shards present,
// so a sweep that moved it would change the fan-out it is trying to attribute to the bind count.
//
// Eight is the router's knee from step-201e, which keeps the two benches readable side by side.
const poolCeilingPartitions = 8

// measurePoolCeiling runs one palier: prefill a private mt.routed topic, run the connector pool alone
// over it against a fake SMSC for `hold`, and report the rate with the facts needed to read it. The rate
// is returned as well as logged so the caller can cross the two sweeps at their shared configuration.
//
// "Alone" means no router, no REST, no injector — not "without its own stores". The pool's post-send
// path is part of what it costs: mt.outcome is produced synchronously with acks=all on every message
// (step-201c), and dropping it would publish the throughput of a pool that records nothing.
//
// Two production stages ARE left out, and the cost of each is named rather than assumed:
//
//   - DLRMap (the Redis correlation write, §1.11) is the caller's choice. Passed nil it is left out, which
//     is what the reference run does too — so the palier is comparable to the 2 400/s it published, and
//     it is an UPPER bound on production, which pays one Redis round-trip per acknowledged submit_sm on
//     this path. Passed a store it is wired, timed, and checked: putsMatchSubmits refuses a palier that
//     held a store and never reached it, which is what recordDLRMapping does on a non-ROK response.
//   - Billing is nil. Billing is opt-in (M9) and invariant (c) requires a disabled deployment to make
//     zero network calls, so a no-op settler is a faithful billing-off pod, not a shortcut.
//
// The breaker is NOT stubbed away. With Deps.Breaker nil the pool builds no local breaker at all: the
// palier would be faster than production AND breakerHeld would have nothing to read.
func measurePoolCeiling(t *testing.T, brokers []string, bed poolBed, binds, window int, hold time.Duration, dlr *countingDLRMap) float64 {
	t.Helper()

	label := poolLabel(binds, window)
	topic, connectorID := bed.topic, bed.connectorID

	cfg := refKafkaConfig(brokers)
	cons := newConsumer(t, cfg, "loadref-pool-ceiling-"+uuid.NewString(), topic)
	inner, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(inner.Close)

	prod := newCountingProducer(inner)
	counted := &countingConsumer{inner: cons}
	tracer := observability.Tracer(noop.NewTracerProvider(), "loadref-pool-ceiling")

	// New rather than Start: Start wires the peer's logger to t.Logf, and tearing a saturated peer down
	// prints one "broken pipe" line per in-flight response — hundreds of lines burying the figures.
	peer, err := fakesmsc.New(fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	if err != nil {
		t.Fatalf("peer: %v", err)
	}
	t.Cleanup(peer.Close)

	breakers := &watchingAggregator{}
	// Assigned through a branch rather than inline: a (*countingDLRMap)(nil) stored in the Deps.DLRMap
	// interface is a NON-nil interface holding a nil pointer, so New would keep it instead of defaulting
	// to its no-op and the first acknowledged submit would panic inside the send path.
	deps := connectorpool.Deps{
		Consumer:    counted,
		Producer:    prod,
		ConnectorID: connectorID,
		Bind: connectorpool.BindConfig{
			Addr: peer.Addr(), SystemID: "gateway", Password: "pw",
			DialTimeout: 5 * time.Second, ResponseTimeout: 10 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3,
			WindowSize: window, BindPoolSize: binds,
		},
		Breaker:      breakers,
		BreakerGauge: metrics.NewCatalog(),
		Metrics:      metrics.NewCatalog(),
		Tracer:       tracer,
	}
	if dlr != nil {
		deps.DLRMap = dlr
	}
	pool := connectorpool.New(deps)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- pool.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-runErr; err != nil {
			t.Errorf("%s: pool run: %v", label, err)
		}
	}()

	// The binds must be up before the clock opens, or the window measures a dial. Waiting on the first
	// outcome would not catch it: that is already past the bind.
	waitBound(t, peer, binds)
	waitUntilSubmitting(t, prod, counted)

	first := lagOf(t, cons, topic)
	brokerBefore, brokerErr := scrapeBroker(t)
	start := time.Now()
	base, baseNanos, baseBuckets := prod.snapshot()
	baseBatches, baseRecords, baseLanes := counted.snapshot()
	basePuts, baseDLRNanos, baseDLRBuckets := dlr.snapshot()
	baseSubmits := peer.SubmitsByConn()

	select {
	case err := <-runErr:
		runErr <- err // give the deferred read something to drain
		t.Fatalf("%s: the pool stopped inside the window: %v", label, err)
	case <-time.After(hold):
	}

	elapsed := time.Since(start)
	produced, nanos, buckets := prod.snapshot()
	done := produced - base
	brokerAfter, afterErr := scrapeBroker(t)
	// Read while the group is still alive: kadm seeds a group's lag from its members, so a reading taken
	// after cancel() describes a group that has left.
	last := lagOf(t, cons, topic)
	batches, recs, lanes := counted.snapshot()
	// The store is read before the peer here because it is read before the peer at the other end of the
	// window too. What matters is not which comes first but that the ORDER is the same at both ends: the
	// two deltas then cover spans offset by the same read gap, and the residue between them stays a
	// handful of messages instead of accumulating. It does not vanish — the first run read 64 194 writes
	// against 64 189 submits — which is why putsMatchSubmits bands it symmetrically rather than ordering it.
	endPuts, endDLRNanos, endDLRBuckets := dlr.snapshot()
	puts, dlrNanos := endPuts-basePuts, endDLRNanos-baseDLRNanos
	dlrBuckets := deltaBuckets(baseDLRBuckets, endDLRBuckets)
	submits := subtractSubmits(baseSubmits, peer.SubmitsByConn())
	reports, worst := breakers.worst()

	if err := backlogHeld(first, last); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if err := shardBalance(submits, binds); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if err := breakerHeld(reports, worst); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if dlr != nil {
		var carried int64
		for _, n := range submits {
			carried += n
		}
		if err := putsMatchSubmits(int64(puts), carried); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}

	rate := float64(done) / elapsed.Seconds()
	t.Logf("pool alone: %s -> %8.0f submit_sm/s · %s · peer %.0f/s · %s · prefill %.0f rec/s",
		label, rate,
		crossCheck(rate, first, last, elapsed),
		calibratePeer(t, binds),
		laneShape(batches-baseBatches, recs-baseRecords, lanes-baseLanes, poolCeilingPartitions),
		bed.prefillRate)
	t.Logf("            %s    %s", label, brokerReport(brokerBefore, brokerAfter, brokerErr, afterErr))
	t.Logf("            %s    %s", label,
		stageLatency(refProduceStage, refProduceExcludes, done, nanos-baseNanos, deltaBuckets(baseBuckets, buckets), elapsed, done))
	if dlr != nil {
		t.Logf("            %s    %s", label, stageLatency(refDLRStage, refDLRExcludes, puts, dlrNanos, dlrBuckets, elapsed, done))
	}

	if rate == 0 {
		t.Fatalf("the pool moved nothing at %s", label)
	}
	return rate
}

// countingDLRMap decorates a DLR correlation store with the counter and the timer the fidelity palier
// reads, on the pattern of countingProducer: it decorates rather than replaces, so the palier measures
// the production store and not a stand-in for it.
//
// The counter is the load-bearing half. recordDLRMapping returns early on a non-ROK submit_sm_resp and
// on a response carrying no smsc message id, so a palier can hold a real store, dial it, and never call
// it — at which point the "with" and "without" configurations are the same configuration and their
// delta is host noise wearing Redis's name. putsMatchSubmits is what refuses that, and it needs this
// count.
//
// It counts SUCCESSES only, for countingProducer's reason: a store failing under load is a finding, not
// a latency, and averaging failed round trips into the mean would make a broken Redis look fast.
type countingDLRMap struct {
	inner   connectorpool.DLRMap
	ok      atomic.Uint64
	nanos   atomic.Uint64
	buckets []atomic.Uint64
}

func newCountingDLRMap(inner connectorpool.DLRMap) *countingDLRMap {
	return &countingDLRMap{inner: inner, buckets: make([]atomic.Uint64, len(produceBounds)+1)}
}

func (m *countingDLRMap) Put(ctx context.Context, smscMsgID string, r pipeline.RoutedMT) error {
	started := time.Now()
	if err := m.inner.Put(ctx, smscMsgID, r); err != nil {
		return err
	}
	took := time.Since(started)
	m.ok.Add(1)
	m.nanos.Add(uint64(took))
	m.buckets[produceBucket(took)].Add(1)
	return nil
}

// snapshot reads the three counters together, and answers for a nil receiver.
//
// The nil case is not defensiveness: measurePoolCeiling brackets its window with one block of opening
// readings and one of closing ones, and those two blocks are read side by side when a palier looks
// wrong. Wrapping two of the lines in a condition to serve the storeless paliers would break that
// symmetry for a value the guard and the log already skip.
func (m *countingDLRMap) snapshot() (count, nanos uint64, buckets []uint64) {
	if m == nil {
		return 0, 0, nil
	}
	buckets = make([]uint64, len(m.buckets))
	for i := range m.buckets {
		buckets[i] = m.buckets[i].Load()
	}
	return m.ok.Load(), m.nanos.Load(), buckets
}

// waitUntilSubmitting blocks until the pool has produced its first mt.outcome, and names WHY if it
// never does.
//
// The router's waitUntilConsuming offers two causes — the group did not join, or the topic is empty —
// and neither fits the pool's most likely failure. A prefill whose ConnectorID does not match is
// skipped AND COMMITTED (connectorpool.go filters before the send), so the group joins, drains the
// whole backlog at full speed, and submits nothing. Reporting that as "the group did not join" sends
// the reader to Kafka for a fault that lives in the fixture.
//
// The two are told apart by what the consumer saw: records delivered with no outcome produced is a
// pool that skipped them, and it can only be the connector filter.
func waitUntilSubmitting(t *testing.T, prod *countingProducer, counted *countingConsumer) {
	t.Helper()
	deadline := time.Now().Add(ceilingJoinDeadline)
	for time.Now().Before(deadline) {
		if prod.ok.Load() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, records, _ := counted.snapshot(); records > 0 {
		t.Fatalf("the pool was delivered %d records in %v and produced no outcome for any of them: it is "+
			"skipping-and-committing, which is what it does when the prefill's ConnectorID does not match Deps.ConnectorID",
			records, ceilingJoinDeadline)
	}
	t.Fatalf("nothing was delivered in %v — the group did not join, or the topic is empty", ceilingJoinDeadline)
}

// poolLabel names a palier by both levers, so two rows can never be confused for one another in a log
// read weeks later — and so a sweep that moved one lever cannot be filed under the other.
func poolLabel(binds, window int) string {
	return fmt.Sprintf("%2d binds w%-3d", binds, window)
}

// newPoolBed builds the one backlog every palier of the sweep reads.
//
// One topic for the whole sweep, not one per palier. The router bench needs a fresh topic each time
// because it VARIES the partition count; here partitions are fixed and only the pool's own two levers
// move, so a single prefill serves every palier — each one joins with a fresh group id and re-reads the
// same records from offset zero. That is nine times less disk on a single-node broker, nine prefills
// saved, and — the part that matters for the curve — an IDENTICAL fixture under every point rather than
// nine independently sampled ones.
//
// The ids are balanced across maxBinds shards, which balances every smaller power of two for free:
// FNV(id) % 16 uniform means FNV(id) % 8 uniform, because each residue mod 8 collects exactly two
// residues mod 16. The sweep's bind counts are all powers of two for that reason. Nothing downstream
// trusts this arithmetic — shardBalance reads what the peer counted.
func newPoolBed(t *testing.T, brokers []string, records, maxBinds int) poolBed {
	t.Helper()

	// The pool filters on ConnectorID and SKIPS-AND-COMMITS what does not match, so a prefill carrying
	// the wrong one drains the backlog at full speed and submits nothing — a flattering consumer rate
	// with an empty peer. One id, used by both ends.
	bed := poolBed{connectorID: uuid.New()}
	records -= records % maxBinds
	ids := balancedShardIDs(t, records, maxBinds)

	bed.topic = newCeilingTopic(t, brokers, "routed", poolCeilingPartitions)
	bed.prefillRate = prefill(t, brokers, bed.topic, records, func(i int) (kafka.Record, error) {
		return pipeline.EncodeRouted(routedForPool(ids[i], bed.connectorID))
	})

	// The routed key is the message id, so the partition is murmur2(id) and the shard is FNV(id): two
	// independent geometries over the same bytes. The ids are chosen for the shard, so this reads where
	// they happened to land partition-wise rather than predicting it.
	if err := prefillBalance(endOffsets(t, brokers, bed.topic), poolCeilingPartitions); err != nil {
		t.Fatalf("prefill: %v", err)
	}
	return bed
}

// poolBed is the backlog and the connector identity every palier of a sweep shares.
type poolBed struct {
	topic       string
	connectorID uuid.UUID
	prefillRate float64
}

// balancedShardIDs returns count message ids spread exactly evenly across `shards` bind shards, using
// the pool's own key hash (FNV-32a of the id bytes % shards, mirroring shardIndex).
//
// It is copied from internal/connectorpool's own test rather than shared, the way the router bench
// copies its pipeline stubs: a helper reaching across a package boundary to a test file is a coupling
// neither package declares. The copy is only ever a FIXTURE — shardBalance reads what the peer counted,
// so a copy that drifted from shardIndex produces an unbalanced prefill and the guard says so. Nothing
// downstream trusts this hash.
func balancedShardIDs(t *testing.T, count, shards int) []uuid.UUID {
	t.Helper()
	if count <= 0 || count%shards != 0 {
		t.Fatalf("balancedShardIDs: count %d is not a positive multiple of shards %d", count, shards)
	}
	per := count / shards
	filled := make([]int, shards)
	ids := make([]uuid.UUID, 0, count)
	for len(ids) < count {
		id := uuid.New()
		h := fnv.New32a()
		_, _ = h.Write(id[:])
		s := int(h.Sum32() % uint32(shards))
		if filled[s] < per {
			filled[s]++
			ids = append(ids, id)
		}
	}
	return ids
}

// routedForPool is the record the router publishes after a successful pipeline pass, addressed to THIS
// bench's connector.
//
// It differs from routedBench on the two fields a prefill may not leave to chance: the message id
// carries the shard geometry, and the connector id decides whether the pool processes the record or
// skips-and-commits it. Everything else is the reference run's own routed record.
func routedForPool(id, connectorID uuid.UUID) pipeline.RoutedMT {
	const body = "Your one time code is 424242. It expires in ten minutes. Do not share it with anyone, our staff will never ask you for it."
	return pipeline.RoutedMT{
		MessageID:    id,
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		From:         refSenderID,
		To:           "2250700000000",
		Body:         msg.NewBodyString(body),
		Encoding:     "gsm7",
		ConnectorID:  connectorID,
		SegmentSeq:   1,
		SegmentCount: 1,
		SubmittedAt:  time.Now().UTC(),
	}
}

// subtractSubmits turns two per-bind readings into what the window carried. The peer counts for its
// whole life, and the binds are dialled before the clock opens, so the opening reading is rarely zero.
func subtractSubmits(before, after []int64) []int64 {
	out := make([]int64, len(after))
	for i := range after {
		if i < len(before) {
			out[i] = after[i] - before[i]
			continue
		}
		out[i] = after[i]
	}
	return out
}

// watchingAggregator is the in-process stand-in for the Redis breaker aggregate, plus the reading
// breakerHeld needs. It echoes each bind's own state back — which is what a single-pod aggregate would
// compute anyway — and remembers the worst it was ever shown.
//
// Worst, not last: a breaker that opened and recovered inside the window still cut the send path, and a
// closing reading would report the recovery rather than the cut.
type watchingAggregator struct {
	mu      sync.Mutex
	n       int
	highest breaker.State
}

func (a *watchingAggregator) Report(_ context.Context, _ string, _ int, s breaker.State) (breaker.State, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	if severityOf(s) > severityOf(a.highest) {
		a.highest = s
	}
	return s, nil
}

func (a *watchingAggregator) worst() (int, breaker.State) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n, a.highest
}

// severityOf orders the states by how much they say the send path was refused. It does not rely on the
// iota order of breaker.State: Closed is the zero value and the constants are ordered for readability,
// not severity, so a reordering there must not silently reorder this.
func severityOf(s breaker.State) int {
	switch s {
	case breaker.Open:
		return 2
	case breaker.HalfOpen:
		return 1
	default:
		return 0
	}
}
