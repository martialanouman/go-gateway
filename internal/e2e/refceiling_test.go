package e2e_test

import (
	"fmt"
	"math"
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

// putsSamplingSlack is how far the two counters may differ before the difference stops being the
// boundary between them and starts being a fault.
//
// It is SYMMETRIC, and the first version of this guard was not. "The write follows the response, so it
// cannot outnumber it" is true of the two counters at one instant and false of what the palier actually
// compares: two DELTAS, each the difference of two readings taken microseconds apart on a live path.
// The first run measured 64 194 writes against 64 189 submits and failed a palier that was sound. The
// readings are taken in the same order at both ends of the window, which makes the two spans nearly
// equal and the residue small — but nothing makes it zero, or gives it a sign.
//
// One percent of a palier that moves hundreds of thousands of records is thousands of messages: two
// orders of magnitude above that residue, and still an order below the smallest real fault. A counter
// wired to segments instead of messages reads 30% high; a store that is never reached reads 100% low.
const putsSamplingSlack = 0.01

// putsMatchSubmits reports whether the DLR store was actually on the path the palier measured.
//
// It exists because recordDLRMapping returns EARLY on a non-ROK submit_sm_resp and on a response
// carrying no smsc message id: both are ordinary production behaviour, and both mean a wired store is
// never called. A fidelity palier can therefore hold a real Redis client, dial it, and pay nothing —
// at which point the "with" and "without" configurations are the same configuration, their delta is
// host noise, and the noise gets published under Redis's name.
//
// Nothing else in this bench can see that: the rate is plausible, the breaker is closed, the producer
// and the backlog agree. The only evidence is the store's own counter.
func putsMatchSubmits(puts, submits int64) error {
	if submits <= 0 {
		return fmt.Errorf("the peer counted no submit in the window: there was nothing for the DLR store to "+
			"record, so its silence proves nothing about whether it is wired (%d writes)", puts)
	}
	if puts == 0 {
		return fmt.Errorf("the DLR store was never called across %d submits: recordDLRMapping skips a non-ROK "+
			"response and one carrying no smsc message id, so this palier paid no Redis at all and its delta "+
			"against the storeless one is host noise", submits)
	}
	ratio := float64(puts) / float64(submits)
	if ratio > 1+putsSamplingSlack {
		return fmt.Errorf("the DLR store recorded %d writes against %d submits (%.0f%%): more writes than the "+
			"traffic they are derived from is not a sampling boundary at this size — one of the two counters is "+
			"not counting what its name says", puts, submits, 100*ratio)
	}
	if ratio < 1-putsSamplingSlack {
		return fmt.Errorf("the DLR store recorded %d writes for %d submits (%.0f%%): the palier paid Redis for "+
			"part of its traffic, so any delta read from it understates the cost by the rest",
			puts, submits, 100*ratio)
	}
	return nil
}

// fidelityDelta renders what wiring the real DLR store cost the pool, and refuses to name a figure the
// readings cannot support.
//
// The delta between two configurations is only meaningful against the spread of the readings it is
// drawn from. step-201f PR1 measured ONE configuration twice inside a single run and read 12 573/s then
// 14 972/s — 19% apart on a lever the same bench had shown inert. A fidelity palier that ran three
// windows without the store, then three with, and subtracted the means would attribute that drift to
// Redis with a straight face, and the resulting number would go into step-207's sizing.
//
// So the pairs must be INTERLEAVED by the caller, and the caller must ALTERNATE which member runs first:
// interleaving puts the two members minutes apart instead of half an hour, and alternating stops a host
// that drifts monotonically from penalising whichever side always ran second — a systematic error, which
// is the kind an average does not remove. The spread within a side is then this host's own variance over
// the same span the delta is measured across, and the only local estimate of the noise available.
//
// A delta under that spread is reported as unreadable rather than as a cost. Two pairs is the floor; one
// bounds no spread at all, so any delta would clear a noise estimate of zero.
//
// It returns an ERROR, not a verdict, when the stored side comes out ahead by more than the scatter: an
// added synchronous write cannot raise throughput, so that reading says the two sides were not measured
// under the same conditions, and a caller left green over it would publish nothing and notice nothing.
func fidelityDelta(without, with []float64) (string, error) {
	if len(without) != len(with) {
		return "", fmt.Errorf("%d readings without the store against %d with: the pairing is what defends "+
			"against drift, and unpaired sides were not measured across the same span", len(without), len(with))
	}
	if len(without) < 2 {
		return "", fmt.Errorf("%d pair(s) measured: a single pair bounds no spread, so any delta would clear a "+
			"noise estimate of zero — run at least two interleaved pairs", len(without))
	}
	meanW, spreadW, err := meanAndSpread(without, "without the store")
	if err != nil {
		return "", err
	}
	meanD, spreadD, err := meanAndSpread(with, "with the store")
	if err != nil {
		return "", err
	}

	noise := max(spreadW, spreadD)
	cost := (meanW - meanD) / meanW
	out := fmt.Sprintf("%d interleaved pairs · %.0f/s without the DLR store, %.0f/s with · spread within a "+
		"side %.0f%%", len(without), meanW, meanD, 100*noise)

	switch {
	case math.Abs(cost) <= noise:
		return fmt.Sprintf("%s · the %.0f%% delta is under the spread of the readings it is drawn from: this "+
			"bench cannot put a figure on the DLR write", out, 100*math.Abs(cost)), nil
	case cost < 0:
		// An error, not a verdict. A synchronous write added to every message cannot RAISE throughput, so a
		// reading that says it did was not taken under one set of conditions — and the run it belongs to
		// bounds nothing. Rendering it as a sentence would leave the caller green over an unusable
		// measurement, which is the one thing this file exists to refuse. sweepsAgree treats the same class
		// the same way.
		return "", fmt.Errorf("%s · the palier ran %.0f%% FASTER with the store wired, past its own %.0f%% "+
			"scatter: no added write can do that, so the two sides were not measured under the same "+
			"conditions and neither bounds the other — re-run the pairs", out, -100*cost, 100*noise)
	default:
		return fmt.Sprintf("%s · the store costs %.0f%% of the throughput", out, 100*cost), nil
	}
}

// meanAndSpread is the mean of a side and how far its own readings spread around it, as a fraction of
// the mean.
//
// Peak-to-peak rather than a standard deviation: three readings are too few for a deviation to describe
// anything, and the question asked of it is "could the delta be one more of these readings", which the
// full range answers directly and a deviation only after a distributional assumption this bench has no
// grounds for.
func meanAndSpread(rates []float64, side string) (mean, spread float64, err error) {
	lo, hi, sum := rates[0], rates[0], 0.0
	for _, r := range rates {
		if r <= 0 {
			return 0, 0, fmt.Errorf("a palier %s read %.0f/s: a window that moved nothing is a broken palier, "+
				"not a slow one, and averaging it in manufactures a delta", side, r)
		}
		lo, hi, sum = min(lo, r), max(hi, r), sum+r
	}
	mean = sum / float64(len(rates))
	return mean, (hi - lo) / mean, nil
}

// maxSweepGap is the fraction by which two readings of the same configuration may differ before the
// curves drawn through them stop meaning anything.
//
// It is wider than crossCheck's ±5% because it compares two windows minutes apart rather than two
// counters over one window: the host's own drift rides on top. It is still narrower than the smallest
// step a two-lever sweep is read for — a lever that moves throughput by less than this is a lever the
// bench cannot see, and saying so is the point.
const maxSweepGap = 0.10

// sweepsAgree reports whether the two sweeps measured the same configuration to the same number.
//
// It is the between-palier counterpart of crossCheck. Every other guard in this bench compares two
// readings of the SAME window, so a host that slowed down between the two sweeps satisfies all of them
// and still bends both curves — the crossing point is the only place that drift becomes visible.
func sweepsAgree(atA, atB float64) error {
	if atA <= 0 || atB <= 0 {
		return fmt.Errorf("a crossing reading was zero (%.0f and %.0f): a palier that moved nothing agrees "+
			"with nothing, and the ratio would come from a division by it", atA, atB)
	}
	gap := (atB - atA) / atA
	if gap < 0 {
		gap = -gap
	}
	if gap > maxSweepGap {
		return fmt.Errorf("the two sweeps read their shared configuration as %.0f/s and %.0f/s, a %.0f%% gap: "+
			"the host moved between them, so the slope of either curve is worth less than the spread of one of "+
			"its own points — lengthen the window (REF_CAL_HOLD) before reading either", atA, atB, 100*gap)
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

// refProduceStage and refProduceExcludes name the acks=all produce for stageLatency, once, for the two
// benches that time it. The exclusion clause is the honest half: the timer brackets the Produce call and
// nothing else, so the share it renders is a FLOOR on the per-message budget and never the whole of it.
const (
	refProduceStage    = "produces"
	refProduceExcludes = "decode, Pipeline.Process and encode are NOT counted here"
)

// refDLRStage and refDLRExcludes name the DLR correlation write for stageLatency (step-201f PR2).
//
// Its exclusion clause carries more weight than the produce's, because the DLR timer is the closest
// this bench gets to the fiche's D2 — chronométrer le submit_sm in situ — and it is NOT that. There is
// no seam around bind.Submit: it is concrete, unexported, and reached from one call site inside the
// send path, so timing it would mean adding an interface to connectorpool.Deps, which the fiche forbids
// twice. What the two timers give instead is a decomposition: budget minus produce minus DLR write is a
// residual that CONTAINS the submit_sm round trip, and a ceiling on it is not a measurement of it.
const (
	refDLRStage    = "DLR writes"
	refDLRExcludes = "the submit_sm round trip and the mt.outcome produce are NOT counted here"
)

// stageLatency renders what one per-message stage cost, measured rather than subtracted.
//
// It is the answer to a question PR1 could only reach by arithmetic: the router's throughput per lane
// falls 70% between 1 and 16 lanes, and the per-record cost has to be paid somewhere. The mean here is
// what one occurrence of the stage took; the budget is the window divided by what came out; the share is
// which of the two dominates.
//
// It refuses to divide rather than print a plausible zero, and `excludes` names what the timer does NOT
// cover — a share of a budget is not the budget, and a reader who takes it for the message's whole cost
// stops looking. It is a parameter because the same arithmetic now serves two stages: the router's and
// the pool's acks=all produce, and the pool's DLR correlation write (step-201f PR2). A second renderer
// would have been the same code with a different noun, and the two would have drifted.
//
// stage is the plural noun the count is rendered with ("produces", "DLR writes").
//
// buckets are per-class counts (one increment per observation), not the cumulative form a Prometheus
// exposition uses: the hot path pays one atomic add instead of one per bucket above the value.
func stageLatency(stage, excludes string, count, sumNanos uint64, buckets []uint64, window time.Duration, messages uint64) string {
	if count == 0 {
		return "no " + stage + " observed in this window"
	}
	mean := time.Duration(sumNanos / count)
	out := fmt.Sprintf("%d %s, mean %v, p99 %s", count, stage, mean.Round(time.Microsecond), p99Interval(buckets))
	if messages == 0 || window <= 0 {
		return out + " (no output in the window: no budget to compare it against)"
	}
	budget := window / time.Duration(messages)
	share := 100 * float64(mean) / float64(budget)
	out = fmt.Sprintf("%s · budget %v/message · they are %.0f%% of it (%s)", out, budget.Round(time.Microsecond), share, excludes)
	if share <= 100 {
		return out
	}
	// Over 100% is not an arithmetic slip and must not read as one. The budget is the WHOLE stage's
	// output rate inverted, while the mean is what ONE occurrence took: a share above 100% is exactly
	// what concurrency buys, and its size says how many are in flight at once.
	return fmt.Sprintf("%s — above 100%% because they overlap: ~%.1f in flight at any instant", out, share/100)
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

// TestStageLatencyRefusesToDivide: an empty histogram means the produce path was never exercised,
// which is a finding. A mean of zero reads as "the produce is free", which is its opposite.
func TestStageLatencyRefusesToDivide(t *testing.T) {
	got := stageLatency(refProduceStage, refProduceExcludes, 0, 0, make([]uint64, len(produceBounds)+1), 10*time.Second, 50000)
	if strings.Contains(got, "µs") || strings.Contains(got, "%") {
		t.Errorf("an empty histogram must not be rendered as a latency or a share: %s", got)
	}
}

// TestStageLatencySplitsTheBudget pins the arithmetic the whole point of D3 rests on: what share of a
// message's wall time is the synchronous acks=all produce.
//
// The two counts are deliberately DIFFERENT — 50 000 produces for 25 000 messages, the shape a
// two-segment message gives — because they divide different things and a test where they are equal
// cannot tell one from the other. The mean is per produce (41 s / 50 000 = 820 µs); the budget is per
// message (10 s / 25 000 = 400 µs); the share is 205%, i.e. the produce alone costs twice what a
// message may spend.
func TestStageLatencySplitsTheBudget(t *testing.T) {
	buckets := make([]uint64, len(produceBounds)+1)
	buckets[0] = 50000
	got := stageLatency(refProduceStage, refProduceExcludes, 50000, uint64(41*time.Second), buckets, 10*time.Second, 25000)

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
	quiet := stageLatency(refProduceStage, refProduceExcludes, 50000, uint64(5*time.Second), buckets, 10*time.Second, 50000)
	if strings.Contains(quiet, "in flight") {
		t.Errorf("a share under 100%% needs no overlap clause, got: %s", quiet)
	}
}

// TestStageLatencyNamesWhatItExcludes: the figure is a share of the per-message budget, not the whole
// of it. Without the clause a reader takes it for the message's cost and stops looking.
//
// It is checked with the DLR stage's own constants rather than the produce's, because the clause became
// a PARAMETER when the renderer grew its second caller: what this guards is that a caller's clause
// reaches the output at all, which is what a third caller passing "" would break. Those constants are
// the second caller's real wording, and this is the only place the ordinary suite can reach them — the
// caller itself lives behind the loadref tag.
func TestStageLatencyNamesWhatItExcludes(t *testing.T) {
	buckets := make([]uint64, len(produceBounds)+1)
	buckets[0] = 1000
	got := stageLatency(refDLRStage, refDLRExcludes, 1000, uint64(time.Second), buckets, 10*time.Second, 1000)
	if !strings.Contains(got, "NOT counted") {
		t.Errorf("the figure must say what the timer leaves out (here: the submit_sm round trip and the "+
			"mt.outcome produce), got: %s", got)
	}
}

// TestStageLatencyBoundsTheQuantile: on log-spaced buckets a quantile is an interval, never a value.
// Interpolating inside a bucket whose edges are a factor of two apart carries up to 100% of error while
// reading exactly like a measurement — the lesson gatewaymetrics already paid for.
func TestStageLatencyBoundsTheQuantile(t *testing.T) {
	bounds := produceBounds
	buckets := make([]uint64, len(bounds)+1)
	// 990 observations in [512µs, 1ms), 10 above it: the 99th percentile of 1000 falls in the first.
	lower := indexOfBound(t, bounds, 512*time.Microsecond)
	buckets[lower+1] = 990
	buckets[lower+2] = 10

	got := stageLatency(refProduceStage, refProduceExcludes, 1000, uint64(990*512*time.Microsecond), buckets, 10*time.Second, 1000)

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

// TestSweepsAgreeOnTheirCrossingPoint pins the guard between two sweeps, where crossCheck guards within
// one palier.
//
// A two-lever sweep measures the same configuration twice — once in each lever's curve — and the pair
// is the only evidence that the two curves were drawn under the same conditions. Nothing else in the
// bench can see host drift: every palier's internal cross-check compares two readings of the SAME
// window, so a host that slowed down between the sweeps satisfies all of them and still bends both
// curves.
//
// The band is wider than crossCheck's ±5% on purpose. That one compares two counters over one window;
// this compares two windows minutes apart, which carries the host's own variance on top.
func TestSweepsAgreeOnTheirCrossingPoint(t *testing.T) {
	if err := sweepsAgree(19710, 19969); err != nil {
		t.Errorf("two readings within a percent must pass: %v", err)
	}

	// The reading actually observed on the first full run: 16 888 against 19 710 is 17%, so the slope of
	// either curve is worth less than the gap between two readings of one of its own points.
	err := sweepsAgree(16888, 19710)
	if err == nil {
		t.Fatal("a 17% gap on the same configuration must fail: neither curve is readable through it")
	}
	if !strings.Contains(err.Error(), "%") {
		t.Errorf("the error must quantify the gap so a reader can judge it, got: %v", err)
	}

	// Order must not decide the verdict: the same pair swapped is the same disagreement.
	if err := sweepsAgree(19710, 16888); err == nil {
		t.Error("the guard must be symmetric: swapping the two readings is the same gap")
	}

	// A palier that moved nothing is not agreement with anything, and dividing by it would render a
	// verdict from an infinity.
	if err := sweepsAgree(0, 19710); err == nil {
		t.Error("a zero reading must fail rather than divide")
	}
}

// TestPutsMatchSubmitsRefusesASilentNoop pins the guard without which the fidelity palier could compare
// a configuration against ITSELF and publish the difference as the cost of Redis.
//
// recordDLRMapping returns early on a non-ROK submit_sm_resp and on a response carrying no smsc message
// id (connectorpool.go). Both are ordinary production behaviour and both mean the wired store is never
// called — so a palier can hold a real Redis client, dial it, and pay nothing. The two paliers would
// then be identical, their delta would be host noise, and the noise would be published under Redis's
// name. Nothing else in the bench can see it: the rate is plausible, the breaker is closed, the two
// sources agree.
func TestPutsMatchSubmitsRefusesASilentNoop(t *testing.T) {
	if err := putsMatchSubmits(100000, 100000); err != nil {
		t.Errorf("a store called once per submit must pass: %v", err)
	}

	// The reading is taken from two counters sampled a few microseconds apart, with submits in flight
	// between them: a handful of submits with no Put yet is the boundary, not a fault.
	if err := putsMatchSubmits(99995, 100000); err != nil {
		t.Errorf("a handful of submits in flight at the sampling boundary must pass: %v", err)
	}

	err := putsMatchSubmits(0, 100000)
	if err == nil {
		t.Fatal("a store that recorded nothing under 100 000 submits must fail: the palier paid no Redis at all")
	}
	if !strings.Contains(err.Error(), "never called") {
		t.Errorf("the error must say the store was never reached, not that it lagged, got: %v", err)
	}

	// Half the submits recorded is not a sampling boundary; it is a palier that paid Redis for half its
	// traffic and would report half the cost.
	if err := putsMatchSubmits(50000, 100000); err == nil {
		t.Error("a store reached for half the submits must fail: the delta would be half the cost")
	}

	// The excess side carries the same boundary as the shortfall, and the first version of this guard
	// did not: it refused any excess outright, on the argument that a write follows the response it is
	// derived from. True of the two counters at one instant; false of the deltas the palier compares.
	// The first run read 64 194 writes against 64 189 submits and failed a sound palier on five messages.
	if err := putsMatchSubmits(100001, 100000); err != nil {
		t.Errorf("a handful of writes over at the sampling boundary must pass: %v", err)
	}

	// A counter wired to segments rather than messages reads about 30% high, which is what the excess
	// side is actually there to catch — two orders of magnitude above the boundary.
	if err := putsMatchSubmits(130000, 100000); err == nil {
		t.Error("30% more writes than submits must fail: one of the two counters is not counting what it names")
	}

	if err := putsMatchSubmits(0, 0); err == nil {
		t.Error("a window with no submit at all must fail rather than pass vacuously")
	}
}

// TestFidelityDeltaRefusesASinglePair pins the arithmetic that decides whether this bench is ALLOWED to
// name a cost for the DLR write.
//
// step-201f PR1 measured the same configuration twice in one run and read 12 573/s then 14 972/s — 19%
// apart, on an inert lever. A fidelity palier that ran three paliers without Redis, then three with, and
// subtracted the means would attribute that drift to Redis with a straight face. So the delta is only
// readable against the spread of the readings it is drawn from, and one pair bounds no spread at all.
func TestFidelityDeltaRefusesASinglePair(t *testing.T) {
	if _, err := fidelityDelta([]float64{12000}, []float64{9000}); err == nil {
		t.Fatal("one pair must fail: with no spread to compare against, any delta reads as significant")
	}

	if _, err := fidelityDelta(nil, nil); err == nil {
		t.Error("no pair at all must fail rather than render a verdict from nothing")
	}

	if _, err := fidelityDelta([]float64{12000, 12100}, []float64{9000}); err == nil {
		t.Error("unpaired readings must fail: the pairing is what defends against drift")
	}

	// A palier that moved nothing is not a fast configuration; it is a broken one, and it would drag a
	// mean toward zero and manufacture a cost.
	if _, err := fidelityDelta([]float64{12000, 0}, []float64{9000, 9100}); err == nil {
		t.Error("a palier that moved nothing must fail rather than be averaged in")
	}
}

// TestFidelityDeltaCallsADeltaUnderTheNoiseUnreadable is the case this host actually produces.
//
// Three tight readings on each side and a delta smaller than the spread within one side: arithmetic
// will still yield a percentage, and printing it would turn host variance into a measured cost of
// Redis. The verdict has to say the bench cannot see it.
func TestFidelityDeltaCallsADeltaUnderTheNoiseUnreadable(t *testing.T) {
	// Each side spreads ~15% within itself; the two means differ by ~3%.
	without := []float64{12000, 13800, 12500}
	with := []float64{11900, 13200, 12300}

	got, err := fidelityDelta(without, with)
	if err != nil {
		t.Fatalf("three usable pairs must render a verdict: %v", err)
	}
	if !strings.Contains(got, "under the spread") {
		t.Errorf("a delta smaller than the spread of its own readings must be called unreadable, got: %s", got)
	}

	// The noise estimate must be the WIDEST side, not the narrowest. Here the storeless side is tight
	// (~2%) and the stored side scatters (~18%), and the 8% delta falls between the two: read against the
	// tight side it is a measured cost of Redis, read against the wide one it is one more of the readings
	// the wide side already produced. A guard that took the narrower estimate would publish the figure.
	got, err = fidelityDelta([]float64{12000, 12100, 11900}, []float64{10000, 12000, 11000})
	if err != nil {
		t.Fatalf("three usable pairs must render a verdict: %v", err)
	}
	if !strings.Contains(got, "under the spread") {
		t.Errorf("a delta inside the WIDER side's own scatter must be called unreadable, got: %s", got)
	}
}

// TestFidelityDeltaNamesACostThatClearsTheNoise is the other outcome, and the one step-201b would act
// on: readings tight enough that the delta stands outside them.
func TestFidelityDeltaNamesACostThatClearsTheNoise(t *testing.T) {
	// Each side is within 2% of itself; the means are 40% apart.
	without := []float64{12000, 12100, 11900}
	with := []float64{7200, 7260, 7140}

	got, err := fidelityDelta(without, with)
	if err != nil {
		t.Fatalf("three usable pairs must render a verdict: %v", err)
	}
	if strings.Contains(got, "under the spread") {
		t.Errorf("a 40%% delta against a 2%% spread must be named as a cost, got: %s", got)
	}
	if !strings.Contains(got, "40") {
		t.Errorf("the verdict must quantify the cost so step-207 can size against it, got: %s", got)
	}

	// A configuration that ran FASTER with the store wired, by more than its own readings scatter, is not
	// a cost with a lost sign — it is arithmetically impossible. An added synchronous write per message
	// cannot raise throughput, so the two sides were not measured under the same conditions and the
	// comparison is void. That is the class sweepsAgree already refuses with an error, and it must fail
	// the run rather than render a sentence a green test invites nobody to read.
	_, err = fidelityDelta(with, without)
	if err == nil {
		t.Fatal("a palier 40% FASTER with the store wired must fail: no added write can do that, so the two " +
			"sides drifted and neither bounds the other")
	}
	if !strings.Contains(err.Error(), "FASTER") {
		t.Errorf("the error must name what is impossible about the reading, got: %v", err)
	}
}

// TestFidelityDeltaToleratesASlowSideInsideTheNoise separates the impossible reading above from the
// ordinary one that looks like it.
//
// The store side coming out marginally ahead is not a finding: it is one of the readings the host was
// already producing, and refusing it would fail every run whose delta happens to land just the wrong
// side of zero. Only an excess LARGER than the scatter is impossible.
func TestFidelityDeltaToleratesASlowSideInsideTheNoise(t *testing.T) {
	// The stored side is 1,6% ahead, inside a 14% scatter.
	got, err := fidelityDelta([]float64{12000, 13800, 12500}, []float64{12400, 13900, 12600})
	if err != nil {
		t.Fatalf("a side marginally ahead inside its own scatter must render, not fail: %v", err)
	}
	if !strings.Contains(got, "under the spread") {
		t.Errorf("a negative delta inside the scatter is still an unreadable delta, got: %s", got)
	}
}
