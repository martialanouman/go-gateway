package e2e_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
)

// minPrefillShare is the fraction of the mean below which a partition is treated as starved.
//
// It is deliberately generous: the point is not to police the hash's uniformity but to catch the lane
// that will run dry mid-window. franz-go's default partitioner spreads a handful of keys unevenly by
// construction, and a tight band would fail runs that measure perfectly well.
const minPrefillShare = 0.5

// minShardShare is the fraction of the mean below which a bind is treated as starved. It is the shard
// analogue of minPrefillShare and deliberately as generous: FNV over a few thousand ids is not perfectly
// flat, and the point is to catch the bind that idles, not to police the hash.
const minShardShare = 0.5

// crossCheck renders the same throughput derived from the backlog instead of the producer, because two
// independent readings that agree are a measurement and one reading is an assertion.
//
// The topic is static for the window — nothing produces to it while the consumer drains it — so the
// backlog consumed and the records published must match. They are counted at different layers (the
// consumer's committed offsets against the producer's acknowledgements), so a wide gap means one of the
// two is not counting what its name says.
func crossCheck(rate float64, first, last map[int32]int64, window time.Duration) string {
	var drained int64
	for p, n := range first {
		drained += n - last[p]
	}
	if window <= 0 || drained <= 0 {
		return "no backlog delta to cross-check against"
	}
	byLag := float64(drained) / window.Seconds()
	gap := 100 * (byLag - rate) / rate
	out := fmt.Sprintf("backlog says %.0f msg/s (%+.1f%%)", byLag, gap)
	if gap > 5 || gap < -5 {
		return out + " — the two sources disagree, this palier is not quotable"
	}
	return out
}

// shardBalance reports whether every bind carried a comparable share of the window's submits.
//
// The pool fans a poll batch out by FNV32a(MessageID) % len(binds) (step-124), a geometry independent of
// the Kafka partition: a prefill balanced across partitions can still leave binds idle. A palier whose
// binds did not all work is a smaller pool wearing a larger label, and it fails quietly — a lower rate,
// no error.
//
// counts are the submits the PEER observed per connection, never a recomputation of the shard hash. A
// guard that re-derived the shard from the ids would agree with a prefill built from the same copied
// hash however far both had drifted from the pool's own shardIndex: it would confirm the copy rather
// than the geometry, and pass under any convention.
func shardBalance(counts []int64, binds int) error {
	if len(counts) == 0 {
		return fmt.Errorf("the peer counted no connection: nothing was observed, which is not the same as a fan-out that worked")
	}
	if len(counts) != binds {
		return fmt.Errorf("the peer served %d connections against %d binds configured: the curve would be drawn "+
			"against a bind count that never carried records", len(counts), binds)
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	mean := float64(total) / float64(binds)
	for i, n := range counts {
		if n == 0 {
			return fmt.Errorf("bind %d carried no submit at all: the shard geometry left it idle, so this palier "+
				"measured %d binds and reports %d", i, binds-1, binds)
		}
		if float64(n) < minShardShare*mean {
			return fmt.Errorf("bind %d carried %d submits against a mean of %.0f: it starves before the window "+
				"closes and drags the palier down without saying so", i, n, mean)
		}
	}
	return nil
}

// breakerHeld reports whether the send path stayed open for the whole window.
//
// A breaker that trips mid-window refuses submits until its cooldown elapses, and the palier then
// divides the work done before the cut by the whole window: a lower number, no error, no sign of why.
// That is a throughput reading of an outage, and it must fail rather than print.
//
// reports is not decoration. breaker.Closed is the ZERO value of breaker.State, so a window that
// observed nothing — a heartbeat that never fired, a spy never wired — carries the same value as one
// that stayed healthy. Without this check the guard would pass its own absence.
func breakerHeld(reports int, worst breaker.State) error {
	if reports == 0 {
		return fmt.Errorf("no breaker state was reported during the window: Closed is the zero value, so an "+
			"unobserved window is indistinguishable from a healthy one — check the heartbeat against the hold (%d readings)", reports)
	}
	if worst != breaker.Closed {
		return fmt.Errorf("the breaker reached %q during the window: the send path was refused for at least a "+
			"cooldown, so this palier measured a cut and not a ceiling", worst)
	}
	return nil
}

// backlogHeld reports whether every partition still held work when the window closed.
//
// A ceiling measured over an exhausted backlog is not a ceiling: past the last record the router idles,
// and the rate divides real work by a window that includes the idling. The failure is silent — it
// produces a lower number, not an error — which is why it is checked rather than assumed.
//
// It refuses PER PARTITION and never on the total. One lane running dry costs the palier its share of
// the throughput while the total backlog still looks healthy, so a sum would report the run as sound.
func backlogHeld(first, last map[int32]int64) error {
	if len(first) == 0 {
		return fmt.Errorf("no backlog reading was taken: nothing was measured, which is not the same as a backlog that held")
	}
	for _, p := range sortedPartitions(first) {
		remaining, ok := last[p]
		if !ok {
			return fmt.Errorf("partition %d was assigned at the start of the window and gone at the end: "+
				"the assignment moved under the measurement and the two readings are not comparable", p)
		}
		if remaining <= 0 {
			return fmt.Errorf("partition %d drained during the window (%d records at the start, %d at the end): "+
				"this palier measured the end of a queue, not a ceiling — raise REF_PREFILL above %d",
				p, first[p], remaining, first[p])
		}
	}
	return nil
}

// prefillBalance reports whether the prefill landed on every partition in comparable amounts.
//
// The account ids are picked against franz-go's partitioner so that one lands on each partition, but
// that is an optimisation, not a guarantee: kafka.NewProducer configures no RecordPartitioner, so the
// default decides, and a version bump could move it without a single test going red. This reads the
// end offsets and observes where the records actually went.
//
// A starved partition does not fail the run outright the way a drained one does — it drags the palier
// down and says nothing, which is worse.
func prefillBalance(endOffsets map[int32]int64, partitions int) error {
	if len(endOffsets) == 0 {
		return fmt.Errorf("the prefill wrote nothing")
	}
	if len(endOffsets) != partitions {
		return fmt.Errorf("the prefill reached %d partitions of %d: the curve would be drawn against a lane count that never carried records",
			len(endOffsets), partitions)
	}
	var total int64
	for _, n := range endOffsets {
		total += n
	}
	mean := float64(total) / float64(partitions)
	for _, p := range sortedPartitions(endOffsets) {
		if float64(endOffsets[p]) < minPrefillShare*mean {
			return fmt.Errorf("partition %d holds %d records against a mean of %.0f: its lane would idle "+
				"before the window closes and drag the palier down without saying so",
				p, endOffsets[p], mean)
		}
	}
	return nil
}

// laneShape renders how many lanes the router actually opened, against how many it could have.
//
// handleBatch opens one goroutine per partition PRESENT IN THE POLL BATCH, and the batch is bounded by
// FetchMaxPartitionBytes (56 KiB, ADR-0012) and FetchMaxBytes — not by the topic. So a sweep that
// labels its rows with the partition count is asserting something it has not measured: a batch
// spanning 2 partitions out of 16 is a 2-lane measurement wearing a 16-lane label.
func laneShape(batches, records, lanesSum uint64, partitions int) string {
	if batches == 0 {
		return "no poll batch observed — the lane count of this palier is unknown"
	}
	lanes := float64(lanesSum) / float64(batches)
	out := fmt.Sprintf("%.1f lanes per batch of %d possible, %.0f records per batch",
		lanes, partitions, float64(records)/float64(batches))
	if lanes < 0.95*float64(partitions) {
		return out + " — the curve is drawn against the observed lanes, which fall short of the partition count"
	}
	return out
}

// produceBounds are the upper edges of the produce histogram, log2-spaced.
//
// It is a package-level value, computed once, and not a function: produceBucket runs on every single
// produce of a palier — ~300 000 of them at sixteen lanes — and rebuilding the slice there allocated
// 144 bytes per observation, some 43 MB per palier, inside the very path the histogram exists to
// measure without disturbing.
//
// The whole bucketing convention lives in this file, outside the loadref build tag: the writer
// (produceBucket) and the readers (p99Bucket, produceLatency) have to agree on what a bucket means, and
// a convention split across a build tag cannot be exercised by the ordinary suite at all.
//
// The range is chosen to bracket the answer rather than to confirm it. step-201d put the blocked time
// near 819 µs BY SUBTRACTION; bounds that started there could only agree with it. From 16 µs the
// measurement can say "the produce is cheap and the cost is elsewhere", and up to 2 s it can say "the
// broker stalls", which are the two findings that would change what step-207 provisions.
var produceBounds = func() []time.Duration {
	out := make([]time.Duration, 0, 18)
	for d := 16 * time.Microsecond; d <= 2*time.Second; d *= 2 {
		out = append(out, d)
	}
	return out
}()

// produceBucket is the index d falls in: bucket i holds (bounds[i-1], bounds[i]], and the last one
// everything above the top edge. p99Bucket reads the same convention back — TestProduceBucketsAgreeWith
// TheirReading is what keeps the two from drifting.
func produceBucket(d time.Duration) int {
	for i, b := range produceBounds {
		if d <= b {
			return i
		}
	}
	return len(produceBounds)
}

// deltaBuckets subtracts the opening reading from the closing one, so a palier reports its own window
// and not everything since the producer was built.
//
// Both readings come from one countingProducer.snapshot, sized by the same len(p.buckets), so their
// lengths are equal by construction. The indexing is deliberately blind: were that invariant ever
// broken, a panic naming the line is the right outcome. A length guard here would turn a real defect
// into a plausible number published without a word — the failure mode this whole file exists to avoid.
func deltaBuckets(before, after []uint64) []uint64 {
	out := make([]uint64, len(after))
	for i := range after {
		out[i] = after[i] - before[i]
	}
	return out
}

// produceLatency renders what the synchronous acks=all produce cost, measured rather than subtracted.
//
// It is the answer to a question PR1 could only reach by arithmetic: the router's throughput per lane
// falls 70% between 1 and 16 lanes, and the per-record cost has to be paid somewhere. The mean here is
// what one Produce took; the budget is the window divided by what came out; the share is which of the
// two dominates.
//
// It refuses to divide rather than print a plausible zero, and it names what it does NOT cover — the
// decode, Pipeline.Process and the encode are outside this timer, so the share is a floor on the
// per-message budget and never the whole of it.
//
// buckets are per-class counts (one increment per observation), not the cumulative form a Prometheus
// exposition uses: the hot path pays one atomic add instead of one per bucket above the value.
func produceLatency(count, sumNanos uint64, buckets []uint64, window time.Duration, produced uint64) string {
	if count == 0 {
		return "no produce observed in this window"
	}
	mean := time.Duration(sumNanos / count)
	out := fmt.Sprintf("%d produces, mean %v, p99 %s", count, mean.Round(time.Microsecond), p99Interval(buckets))
	if produced == 0 || window <= 0 {
		return out + " (no output in the window: no budget to compare it against)"
	}
	budget := window / time.Duration(produced)
	share := 100 * float64(mean) / float64(budget)
	out = fmt.Sprintf("%s · budget %v/message · the produce alone is %.0f%% of it "+
		"(decode, Pipeline.Process and encode are NOT counted here)",
		out, budget.Round(time.Microsecond), share)
	if share <= 100 {
		return out
	}
	// Over 100% is not an arithmetic slip and must not read as one. The budget is the WHOLE router's
	// output rate inverted, while the mean is what ONE produce took: a share above 100% is exactly what
	// concurrent lanes buy, and its size says how many produces are in flight at once.
	return fmt.Sprintf("%s — above 100%% because the produces overlap: ~%.1f of them are in flight at "+
		"any instant, which is what the lanes bought", out, share/100)
}

// p99Interval renders the 99th percentile as the interval between two bucket edges.
//
// Never a single value: the edges are a factor of two apart, so interpolating inside one carries up to
// 100% of error while reading exactly like a measurement (the lesson gatewaymetrics states at length).
func p99Interval(buckets []uint64) string {
	i := p99Bucket(buckets)
	switch {
	case i < 0:
		return "unknown"
	case i == 0:
		return fmt.Sprintf("at most %v", produceBounds[0])
	case i > len(produceBounds)-1:
		return fmt.Sprintf("over %v", produceBounds[len(produceBounds)-1])
	default:
		return fmt.Sprintf("in (%v, %v]", produceBounds[i-1], produceBounds[i])
	}
}

// p99Bucket is the index of the bucket the 99th percentile falls in, or -1 when nothing was observed.
//
// It is separate from the rendering because that is the only way to test it against produceBucket
// exactly: an assertion on the rendered string would pass against a reader off by one bucket.
//
// The target is floored at one observation. uint64(1 * 0.99) rounds to zero, and a reader stopping at
// "seen >= 0" would answer with the first bucket whether or not anything landed in it.
func p99Bucket(buckets []uint64) int {
	var total uint64
	for _, n := range buckets {
		total += n
	}
	if total == 0 {
		return -1
	}
	target := max(uint64(float64(total)*0.99), 1)
	var seen uint64
	for i, n := range buckets {
		seen += n
		if seen >= target {
			return i
		}
	}
	return -1
}

// TestProduceBucketsAgreeWithTheirReading is the guard the first version of this code did not have.
//
// The bucketing convention — bucket i holds (bounds[i-1], bounds[i]] — is upheld by two functions:
// produceBucket writes, p99Bucket reads. They must agree exactly, and nothing forced them to: the
// writer used to live behind the loadref build tag and the reader outside it, so the ordinary suite
// could not exercise the round trip at all. One of them switching < for <= would move every quantile by
// a full bucket, silently.
//
// It asserts that the chosen bucket CONTAINS the observation. Comparing produceBucket against a reader
// fed by produceBucket itself would be circular — it would only prove that an index survives a round
// trip, which holds under any convention, including a wrong one.
func TestProduceBucketsAgreeWithTheirReading(t *testing.T) {
	bounds := produceBounds
	for _, d := range []time.Duration{
		time.Nanosecond,             // below the first edge
		bounds[0],                   // exactly on it — the boundary < and <= disagree about
		bounds[0] + time.Nanosecond, // just past it
		500 * time.Microsecond,      // mid-scale
		bounds[len(bounds)-1],       // exactly on the top edge
		2 * bounds[len(bounds)-1],   // past the top: the overflow bucket
	} {
		i := produceBucket(d)

		// The convention p99Interval renders: bucket i is (bounds[i-1], bounds[i]].
		if i > 0 && d <= bounds[i-1] {
			t.Errorf("%v landed in bucket %d, whose interval starts above it at %v", d, i, bounds[i-1])
		}
		if i < len(bounds) && d > bounds[i] {
			t.Errorf("%v landed in bucket %d, whose interval ends below it at %v", d, i, bounds[i])
		}
		if i == len(bounds) && d <= bounds[len(bounds)-1] {
			t.Errorf("%v went to the overflow bucket although it is under the top edge %v",
				d, bounds[len(bounds)-1])
		}

		// And the reader lands on the same bucket when that is the only one carrying anything.
		buckets := make([]uint64, len(bounds)+1)
		buckets[i] = 100
		if got := p99Bucket(buckets); got != i {
			t.Errorf("%v was written to bucket %d and read back from %d", d, i, got)
		}
	}
}

// TestProduceBucketAllocatesNothing guards the one property no functional assertion can reach.
//
// produceBucket runs on every produce of a palier — ~300 000 at sixteen lanes. The first version
// rebuilt the bounds slice on each call: 144 bytes an observation, ~43 MB a palier, allocated inside
// the path whose whole purpose is to time that path without disturbing it. The measurement survived it
// (~30 ns against a 188-507 µs produce), but an instrument that adds garbage collection to what it
// measures is one regression away from mattering.
func TestProduceBucketAllocatesNothing(t *testing.T) {
	if n := testing.AllocsPerRun(100, func() { produceBucket(500 * time.Microsecond) }); n != 0 {
		t.Errorf("produceBucket allocates %v times per call, and it runs once per produce", n)
	}
}

// TestP99BucketIgnoresEmptyLeadingBuckets: with a single observation the 99th-percentile target rounds
// down to zero, and a reader that stops at "seen >= target" would name the first bucket — empty — as
// the answer. Marginal at the bench's hundreds of thousands of samples, wrong at any scale.
func TestP99BucketIgnoresEmptyLeadingBuckets(t *testing.T) {
	buckets := make([]uint64, len(produceBounds)+1)
	buckets[3] = 1

	if got := p99Bucket(buckets); got != 3 {
		t.Errorf("a lone observation in bucket 3 must read back from 3, got %d", got)
	}
}

// TestDeltaBucketsSubtractsTheOpeningReading pins the direction of the subtraction.
//
// Reversed, it underflows: these are unsigned counters, so `before - after` does not yield a negative
// number that someone would notice — it yields a value near 2^64 that lands in the histogram and reads
// as a produce that took several centuries.
func TestDeltaBucketsSubtractsTheOpeningReading(t *testing.T) {
	got := deltaBuckets([]uint64{1, 2, 0}, []uint64{5, 9, 4})
	want := []uint64{4, 7, 4}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deltaBuckets = %v, want %v", got, want)
		}
	}
}

// TestProduceLatencyRefusesToDivide: an empty histogram means the produce path was never exercised,
// which is a finding. A mean of zero reads as "the produce is free", which is its opposite.
func TestProduceLatencyRefusesToDivide(t *testing.T) {
	got := produceLatency(0, 0, make([]uint64, len(produceBounds)+1), 10*time.Second, 50000)
	if strings.Contains(got, "µs") || strings.Contains(got, "%") {
		t.Errorf("an empty histogram must not be rendered as a latency or a share: %s", got)
	}
}

// TestProduceLatencySplitsTheBudget pins the arithmetic the whole point of D3 rests on: what share of a
// message's wall time is the synchronous acks=all produce.
//
// The two counts are deliberately DIFFERENT — 50 000 produces for 25 000 messages, the shape a
// two-segment message gives — because they divide different things and a test where they are equal
// cannot tell one from the other. The mean is per produce (41 s / 50 000 = 820 µs); the budget is per
// message (10 s / 25 000 = 400 µs); the share is 205%, i.e. the produce alone costs twice what a
// message may spend.
func TestProduceLatencySplitsTheBudget(t *testing.T) {
	buckets := make([]uint64, len(produceBounds)+1)
	buckets[0] = 50000
	got := produceLatency(50000, uint64(41*time.Second), buckets, 10*time.Second, 25000)

	if !strings.Contains(got, "820µs") {
		t.Errorf("41s over 50000 produces is a mean of 820µs per produce, got: %s", got)
	}
	if !strings.Contains(got, "400µs") {
		t.Errorf("10s over 25000 messages is a 400µs budget per message, got: %s", got)
	}
	if !strings.Contains(got, "205") {
		t.Errorf("820µs against a 400µs budget is 205%% of it, got: %s", got)
	}
	// A share over 100% is what concurrent lanes buy, not an arithmetic slip — the real 16-lane palier
	// reads 1514%, and a reader who takes that for a bug stops reading there.
	if !strings.Contains(got, "in flight") {
		t.Errorf("a share above 100%% must say it means overlapping produces, got: %s", got)
	}

	// At or below 100% there is nothing to explain, and the sentence would be noise.
	quiet := produceLatency(50000, uint64(5*time.Second), buckets, 10*time.Second, 50000)
	if strings.Contains(quiet, "in flight") {
		t.Errorf("a share under 100%% needs no overlap clause, got: %s", quiet)
	}
}

// TestProduceLatencyNamesWhatItExcludes: the figure is a share of the per-message budget, not the whole
// of it. Without the clause a reader takes it for the message's cost and stops looking.
func TestProduceLatencyNamesWhatItExcludes(t *testing.T) {
	buckets := make([]uint64, len(produceBounds)+1)
	buckets[0] = 1000
	got := produceLatency(1000, uint64(time.Second), buckets, 10*time.Second, 1000)
	if !strings.Contains(got, "NOT counted") {
		t.Errorf("the figure must say what it leaves out (decode, pipeline, encode), got: %s", got)
	}
}

// TestProduceLatencyBoundsTheQuantile: on log-spaced buckets a quantile is an interval, never a value.
// Interpolating inside a bucket whose edges are a factor of two apart carries up to 100% of error while
// reading exactly like a measurement — the lesson gatewaymetrics already paid for.
func TestProduceLatencyBoundsTheQuantile(t *testing.T) {
	bounds := produceBounds
	buckets := make([]uint64, len(bounds)+1)
	// 990 observations in [512µs, 1ms), 10 above it: the 99th percentile of 1000 falls in the first.
	lower := indexOfBound(t, bounds, 512*time.Microsecond)
	buckets[lower+1] = 990
	buckets[lower+2] = 10

	got := produceLatency(1000, uint64(990*512*time.Microsecond), buckets, 10*time.Second, 1000)

	// Both edges of the bucket the 99th percentile falls in, read from the bounds themselves so the
	// assertion survives a change of scale.
	low, high := bounds[lower].String(), bounds[lower+1].String()
	if !strings.Contains(got, low) || !strings.Contains(got, high) {
		t.Errorf("the p99 must be rendered as the interval (%s, %s], got: %s", low, high, got)
	}
	// An interval, not a value: a single figure inside a bucket whose edges are a factor of two apart
	// would be an interpolation reading like a measurement.
	if !strings.Contains(got, "p99 in (") {
		t.Errorf("the p99 must be an interval, got: %s", got)
	}
}

// indexOfBound finds a bound by value, so the test above states the edge it means rather than an index
// that would silently move the day the bounds change.
func indexOfBound(t *testing.T, bounds []time.Duration, want time.Duration) int {
	t.Helper()
	for i, b := range bounds {
		if b == want {
			return i
		}
	}
	t.Fatalf("no %v bound in %v", want, bounds)
	return 0
}

// sortedPartitions keeps every refusal deterministic: with a map's iteration order, the partition named
// in an error would change from run to run and two identical failures would read as two different ones.
func sortedPartitions(m map[int32]int64) []int32 {
	out := make([]int32, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestBacklogGuardRefusesADrainedLane pins the guard the router-only ceiling rests on.
//
// A ceiling measured over a backlog that ran out is not a ceiling: past the last record the router is
// idle, and the rate divides real work by a window that includes the idling. It reads as a perfectly
// plausible number, which is exactly why it has to fail rather than print.
func TestBacklogGuardRefusesADrainedLane(t *testing.T) {
	first := map[int32]int64{0: 10000, 1: 10000, 2: 10000, 3: 10000}

	// Every partition still holds work: the window closed before the backlog did.
	if err := backlogHeld(first, map[int32]int64{0: 4000, 1: 3800, 2: 4100, 3: 3900}); err != nil {
		t.Errorf("a backlog that held on every partition must pass: %v", err)
	}

	// One lane ran dry. The total is still 11 900 records, which is why summing would miss it.
	err := backlogHeld(first, map[int32]int64{0: 4000, 1: 3800, 2: 4100, 3: 0})
	if err == nil {
		t.Fatal("a partition that drained must fail the palier, however healthy the total looks")
	}
	if !strings.Contains(err.Error(), "3") {
		t.Errorf("the error must name the partition that drained, got: %v", err)
	}

	// No reading at all is not "the backlog held" — it is "nothing was measured".
	if err := backlogHeld(map[int32]int64{}, map[int32]int64{0: 10}); err == nil {
		t.Error("an empty first reading must fail: no observation is not a passing observation")
	}

	// A partition that vanishes between the two readings means the assignment moved under the
	// measurement, so the two ends are not comparable.
	if err := backlogHeld(first, map[int32]int64{0: 4000, 1: 3800, 2: 4100}); err == nil {
		t.Error("a partition present at the start and missing at the end must fail")
	}
}

// TestPrefillBalanceRefusesALopsidedBacklog pins the guard that observes where the prefill landed.
//
// The account ids are picked against franz-go's partitioner, but that is an optimisation: the default
// partitioner is not configured by kafka.NewProducer, so a version bump could move it without a single
// test going red. This guard does not predict the hash — it reads the end offsets and refuses a
// backlog that would starve a lane before the window closes.
func TestPrefillBalanceRefusesALopsidedBacklog(t *testing.T) {
	if err := prefillBalance(map[int32]int64{0: 8000, 1: 8000, 2: 8000, 3: 8000}, 4); err != nil {
		t.Errorf("an evenly spread prefill must pass: %v", err)
	}

	// p3 holds 1.5% of the mean. It drains almost immediately and its lane idles for the rest of the
	// window, dragging the palier down with no sign of why.
	err := prefillBalance(map[int32]int64{0: 8000, 1: 8000, 2: 8000, 3: 120}, 4)
	if err == nil {
		t.Fatal("a partition holding a fraction of the mean must fail: its lane will idle")
	}
	if !strings.Contains(err.Error(), "3") {
		t.Errorf("the error must name the starved partition, got: %v", err)
	}

	// Fewer partitions carrying records than the topic has: the sweep would draw the curve against a
	// lane count that never existed.
	if err := prefillBalance(map[int32]int64{0: 8000, 1: 8000, 2: 8000}, 4); err == nil {
		t.Error("a topic with 4 partitions and 3 loaded must fail")
	}

	if err := prefillBalance(map[int32]int64{}, 4); err == nil {
		t.Error("an empty prefill must fail")
	}
}

// TestLaneShapeNamesTheObservedLaneCount: the curve is drawn against the lanes that actually opened,
// never against the partition count that was asked for.
//
// handleBatch opens one goroutine per partition PRESENT IN THE POLL BATCH, and the batch is bounded by
// FetchMaxPartitionBytes and FetchMaxBytes. A batch spanning 2 partitions out of 16 is a 2-lane
// measurement wearing a 16-lane label.
func TestLaneShapeNamesTheObservedLaneCount(t *testing.T) {
	got := laneShape(1000, 128000, 3200, 16)
	if !strings.Contains(got, "3.2") {
		t.Errorf("3200 lanes over 1000 batches is 3.2 lanes per batch, got: %s", got)
	}
	if !strings.Contains(got, "16") {
		t.Errorf("the figure must name the partition count it fell short of, got: %s", got)
	}

	// All 16 partitions in every batch: the curve is drawn against what was asked for, and the reader
	// needs to be told so rather than left to compare two numbers.
	got = laneShape(1000, 128000, 16000, 16)
	if strings.Contains(got, "short") {
		t.Errorf("a batch spanning every partition must not warn about falling short, got: %s", got)
	}

	// No batch means no lane observation. A mean of zero lanes would read as "the fan-out did not
	// happen", which is a different finding from "nothing was measured".
	if got := laneShape(0, 0, 0, 16); strings.Contains(got, "per batch") {
		t.Errorf("no batch must not be rendered as a lane mean: %s", got)
	}
}

// TestShardBalanceRefusesAnIdleBind pins the guard the pool ceiling needs and the router never did.
//
// The pool fans a poll batch out by FNV32a(MessageID) % len(binds) — a geometry that has nothing to do
// with the Kafka partition. A prefill whose ids land unevenly leaves binds idle, and the palier then
// measures fewer binds than its label claims, which is the exact shape of the lane trap step-201e hit.
//
// It reads what the PEER counted per connection, never a recomputation of the hash. A guard that
// re-derived the shard from the ids would agree with a prefill built from the same copied hash however
// far both had drifted from the pool's own shardIndex — it would confirm the copy, not the geometry.
func TestShardBalanceRefusesAnIdleBind(t *testing.T) {
	if err := shardBalance([]int64{4000, 3900, 4100, 4000}, 4); err != nil {
		t.Errorf("four binds carrying comparable traffic must pass: %v", err)
	}

	// One bind never carried a submit. The total is a healthy 12 000, which is why a sum would miss it
	// and why the palier would publish a four-bind figure that three binds produced.
	err := shardBalance([]int64{4000, 3900, 4100, 0}, 4)
	if err == nil {
		t.Fatal("a bind that carried nothing must fail the palier, however healthy the total looks")
	}
	if !strings.Contains(err.Error(), "3") {
		t.Errorf("the error must name the idle bind, got: %v", err)
	}

	// A bind at 2% of the mean is not idle, so the zero check misses it — and it drags the palier down
	// without saying so, exactly like a starved partition.
	if err := shardBalance([]int64{4000, 3900, 4100, 80}, 4); err == nil {
		t.Error("a bind carrying a fraction of the mean must fail: it starves without saying so")
	}

	// Every bind at zero is the ONLY case the starvation check cannot reach: the mean is zero too, so
	// no count is below half of it and the guard would report a fan-out that never sent a single
	// submit. It is also the bench's most dangerous failure — a prefill whose ConnectorID does not
	// match is skipped-and-committed, which drains the backlog at full speed and submits nothing.
	err = shardBalance([]int64{0, 0, 0, 0}, 4)
	if err == nil {
		t.Fatal("four silent binds must fail: a zero mean puts every count above half of it")
	}
	if !strings.Contains(err.Error(), "no submit") {
		t.Errorf("the error must say the binds carried nothing, not that they starved, got: %v", err)
	}

	// The peer saw fewer connections than the pool was configured for: some bind never dialled, and the
	// curve would be drawn against a bind count that never carried anything.
	if err := shardBalance([]int64{4000, 3900, 4100}, 4); err == nil {
		t.Error("fewer connections at the peer than binds configured must fail")
	}

	if err := shardBalance(nil, 4); err == nil {
		t.Error("no observation is not a passing observation")
	}
}

// TestBreakerHeldRefusesAPalierThatOpened pins the guard that separates a throughput from an outage.
//
// A breaker that opens mid-window cuts the send path, and the palier then divides the work it managed
// before the cut by the whole window: a lower number, no error, no sign of why.
//
// The reading count is not decoration. breaker.Closed is the ZERO value of breaker.State, so a window
// that observed nothing at all carries the same value as a window that stayed healthy — "no evidence"
// would read as "healthy" and the guard would pass its own absence.
func TestBreakerHeldRefusesAPalierThatOpened(t *testing.T) {
	if err := breakerHeld(5, breaker.Closed); err != nil {
		t.Errorf("a breaker that stayed closed across five readings must pass: %v", err)
	}

	if err := breakerHeld(5, breaker.Open); err == nil {
		t.Fatal("a breaker that opened must fail: the palier measured a cut, not a ceiling")
	}

	// Half-open is recovery from an open episode: the send path was refused for the cooldown, so the
	// window still holds a cut whatever the state at the end.
	if err := breakerHeld(5, breaker.HalfOpen); err == nil {
		t.Error("a breaker probing its way back must fail too: the cut already happened")
	}

	if err := breakerHeld(0, breaker.Closed); err == nil {
		t.Error("zero readings must fail: Closed is the zero value, so no evidence would read as healthy")
	}
}

// TestCrossCheckFlagsDisagreeingSources pins the renderer that turns one reading into a measurement.
//
// It moved out of the loadref build tag when the pool bench became its second caller: a convention the
// ordinary suite cannot compile is a convention no test can exercise, which is the argument this file's
// header already makes for the bucketing helpers.
func TestCrossCheckFlagsDisagreeingSources(t *testing.T) {
	first := map[int32]int64{0: 10000, 1: 10000}
	last := map[int32]int64{0: 5000, 1: 5000}

	// 10 000 records drained over 10s is 1 000/s. The producer says the same, so the palier is quotable.
	got := crossCheck(1000, first, last, 10*time.Second)
	if strings.Contains(got, "disagree") {
		t.Errorf("two sources within the band must not be flagged, got: %s", got)
	}

	// The producer claims 2 000/s while the backlog only gave up 1 000/s. One of the two is not counting
	// what its name says, and neither number may be published.
	got = crossCheck(2000, first, last, 10*time.Second)
	if !strings.Contains(got, "disagree") {
		t.Errorf("a 50%% gap must be flagged as unquotable, got: %s", got)
	}

	// No backlog delta is not agreement: there is simply nothing to check against.
	if got := crossCheck(1000, first, first, 10*time.Second); strings.Contains(got, "%") {
		t.Errorf("no delta must not be rendered as a percentage agreement, got: %s", got)
	}
}
