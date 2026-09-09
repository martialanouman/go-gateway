package e2e_test

import (
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

// The two destination blocks the bench draws from, both inside the +22507000xxxxxx range this
// repository reserves for fixtures. They are DISJOINT by construction: the non-ported literal ends in
// seven zeroes and every ported number is offset by at least one.
//
// Nothing this bench produces leaves the process — the router-only bed has no connector pool, no SMSC
// and no network egress — so widening the draw here carries none of the risk that keeps the k6 script
// pinned to a narrow block (step-410 owns that lock).
const (
	nonPortedDest = "2250700000000"
	// portedShareDen fixes the granularity of REF_PORTED_SHARE at 0.001. A rational share is what lets
	// the mix guard compute the expected ported count exactly instead of tolerating a band.
	portedShareDen = 1000
)

// l0Dest maps a prefill record index to its destination, so that exactly `share` of the records are
// ported and they touch exactly `pool` distinct numbers.
//
// Both properties are DETERMINISTIC in i, never random: the guard has to be able to compute what the
// run should have observed. Random draws would leave every outcome ratio inside a tolerance band, and a
// band is exactly what hides a seed that never bound.
//
// pool is the working set, and it is the locality dial this bench substitutes for a TTL: a 30 s window
// cannot observe a 6 h expiry, so the run FABRICATES locality by the size of the ported set and a
// FLUSHDB between paliers, rather than simulating it.
func l0Dest(i int, share float64, pool int) string {
	// One guard, not two: at share=0 num is 0 and `pos >= 0` already holds for every index, so an
	// additional `num <= 0` early return would be unreachable — and it would make the share=0 clause
	// impossible to falsify, which is how a test that guards nothing stays green.
	num := portedPerBlock(share)
	pos := i % portedShareDen
	if pos >= num {
		return nonPortedDest
	}
	if pool < 1 {
		pool = 1
	}
	// The ordinal counts PORTED occurrences, not record indices: taking i/den would advance the ported
	// number once per thousand records, and a run shorter than den*pool would touch a single number
	// while claiming a pool of thousands.
	ordinal := (i/portedShareDen)*num + pos
	return fmt.Sprintf("2250700%06d", 1+ordinal%pool)
}

// canonicalMSISDN is the CHECK the exact_routes table enforces
// (migrations/0004_exact_routes_msisdn_canonical.up.sql): digits only, no leading "+", no zero first
// digit. A seed that misses it is rejected by Postgres — but a seed that is merely INCONSISTENT with
// what the router looks up is not, and that is the failure this file exists to make impossible.
var canonicalMSISDN = regexp.MustCompile(`^[1-9][0-9]+$`)

// portedPerBlock is how many of each block of portedShareDen records are ported. It is the ONE place the
// share is rounded, so l0Dest and portedSet cannot round it differently.
func portedPerBlock(share float64) int { return int(math.Round(share * portedShareDen)) }

// portedSet enumerates the distinct ported numbers l0Dest draws, in first-draw order — the exact set the
// seed must write into exact_routes.
//
// It TERMINATES by construction: a share that rounds to no ported record per block yields the empty set
// instead of a loop with no exit, which the caller then refuses. Enumerating through l0Dest rather than
// through a second formula is what keeps the seed and the lookup from disagreeing.
func portedSet(share float64, pool int) []string {
	num := portedPerBlock(share)
	if num <= 0 || pool < 1 {
		return nil
	}
	seen := make(map[string]bool, pool)
	out := make([]string, 0, pool)
	for i := 0; len(out) < pool; i++ {
		dest := l0Dest(i, share, pool)
		if dest == nonPortedDest || seen[dest] {
			continue
		}
		seen[dest] = true
		out = append(out, dest)
	}
	return out
}

// TestL0DestReproducesTheLegacyFixture is what keeps test/load/README.md readable.
//
// Every router-ceiling figure in the journal was measured against ONE destination, the literal
// inboundBench froze. share=0 must reproduce it for every index, or the sweep this bench shares a bed
// with stops being comparable to its own history.
func TestL0DestReproducesTheLegacyFixture(t *testing.T) {
	for _, i := range []int{0, 1, 999, 1000, 123456} {
		if got := l0Dest(i, 0, 0); got != "2250700000000" {
			t.Errorf("l0Dest(%d, share=0) = %q, want the legacy fixture 2250700000000", i, got)
		}
	}
}

// TestL0DestHitsTheSeededShare pins the two numbers the whole measurement is read against: how many
// records are ported, and how many DISTINCT ported numbers they touch.
//
// The second is the locality dial. A run whose ported records all land on ten numbers prices the Redis
// arm; one whose ported records never repeat prices the Postgres arm. If the draw is not exact, the
// mix guard cannot compute what it should have seen and every outcome ratio becomes unfalsifiable.
func TestL0DestHitsTheSeededShare(t *testing.T) {
	const (
		records = 1000
		pool    = 10
	)
	ported, distinct := 0, map[string]bool{}
	for i := range records {
		got := l0Dest(i, 0.3, pool)
		if got == "2250700000000" {
			continue
		}
		ported++
		distinct[got] = true
	}

	if ported != 300 {
		t.Errorf("share=0.3 over %d records produced %d ported, want exactly 300", records, ported)
	}
	if len(distinct) != pool {
		t.Errorf("pool=%d produced %d distinct ported numbers, want exactly %d — the working set IS the "+
			"locality dial, so a pool that does not bind makes pg_hit unreadable", pool, len(distinct), pool)
	}
	if distinct["2250700000000"] {
		t.Error("the ported block overlaps the non-ported literal: a record would be counted on both sides")
	}
}

// TestL0DestIsCanonicalE164 is the guard against the hollow fixture this bench is most likely to grow.
//
// The seed writes these numbers into exact_routes and the router looks them up with e164.Normalize
// output. A "+" prefix, or a number e164 rejects as invalid for its country code, gives a seed that
// LOOKS present and a Bloom that answers false for every message: 100% bloom_miss, a measurement of
// zero that reads like a result. Postgres would catch the "+" via the CHECK; nothing would catch the
// second, which is why Normalize is asserted to be the identity here and not merely to succeed.
func TestL0DestIsCanonicalE164(t *testing.T) {
	for _, i := range []int{0, 1, 7, 300, 999, 1000, 999999} {
		for _, share := range []float64{0, 0.3, 1} {
			got := l0Dest(i, share, 1000)
			if !canonicalMSISDN.MatchString(got) {
				t.Fatalf("l0Dest(%d, %v) = %q, which the exact_routes CHECK rejects", i, share, got)
			}
			norm, err := e164.Normalize(got)
			if err != nil {
				t.Fatalf("l0Dest(%d, %v) = %q: e164.Normalize rejects it (%v) — the router would never "+
					"look up what the seed wrote", i, share, got, err)
			}
			if norm != got {
				t.Fatalf("l0Dest(%d, %v) = %q but normalizes to %q: the seed and the lookup would "+
					"disagree, and the bench would read 100%% bloom_miss as a result", i, share, got, norm)
			}
		}
	}
}

// l0Outcomes is the closed vocabulary internal/routing/exact declares. It is repeated here on purpose:
// if the resolver grows an outcome, this list does not, the total stops matching, and the bench says so
// instead of shrinking every ratio by the share of the label it never learned about.
var l0Outcomes = []string{"bloom_miss", "redis_hit", "redis_error", "pg_hit", "pg_miss", "pg_error"}

// maxMixGap is how far an observed outcome may sit from the model before the palier stops being a
// reading. It is as generous as minPrefillShare's cousins for the same reason: the window is a slice of
// the prefill, so the ported count it covers is exact only to within one share block.
const maxMixGap = 0.10

// mixHolds judges the outcome mix against the model the seed makes predictable.
//
// With a cache flushed before the palier, the FIRST touch of each distinct ported number is a pg_hit and
// every later touch is a redis_hit, so:
//
//	pg_hit    = min(ported lookups, pool)      <- the Postgres arm, maximal at the cold bound
//	redis_hit = ported lookups - pg_hit        <- the Redis arm, maximal at the hot bound
//
// The non-ported side is deliberately NOT split between bloom_miss and pg_miss. The bench draws a single
// non-ported number, so a Bloom false positive on it is all-or-nothing: either no message pays Postgres
// for it, or every one of them does — the deterministic hammer ADR-0015 leaves open. Both are legitimate
// runs, so the guard holds their SUM and the renderer prints which one happened.
func mixHolds(counts map[string]uint64, messages uint64, share float64, pool int) error {
	if messages == 0 {
		return fmt.Errorf("no messages in the window: there is no mix to judge")
	}
	if n := counts["pg_error"] + counts["redis_error"]; n > 0 {
		return fmt.Errorf("%d lookup(s) failed closed (pg_error %d, redis_error %d): the palier rejected "+
			"messages rather than routing them, so its rate is not a throughput. First suspect is the pool "+
			"— MaxConns against the lane count, with Acquire waiting out DefaultLookupTimeout — and the "+
			"MaxConns this appeared at is itself the answer step-280 is after",
			n, counts["pg_error"], counts["redis_error"])
	}

	// The unknown label is caught by NAME, not by the total's tolerance band. A vocabulary that grows
	// upstream would otherwise hide inside maxMixGap for any share small enough, and every ratio below
	// would shrink by exactly the amount nobody was told about.
	for name := range counts {
		if !slices.Contains(l0Outcomes, name) {
			return fmt.Errorf("outcome %q is outside the vocabulary this bench models (%v): it would be "+
				"dropped from every ratio here, so the resolver grew a leg and the bench did not", name, l0Outcomes)
		}
	}

	var total uint64
	for _, name := range l0Outcomes {
		total += counts[name]
	}
	if gap := relGap(float64(total), float64(messages)); gap > maxMixGap {
		return fmt.Errorf("the %d observations across the known outcomes are %.0f%% off the %d messages "+
			"routed: exactly one observation per resolution is the invariant every ratio here rests on, so "+
			"either an outcome outside %v was recorded or the counter was not read across the window",
			total, 100*gap, messages, l0Outcomes)
	}

	ported := uint64(math.Round(share * float64(messages)))
	if ported == 0 {
		return nil // the share=0 palier prices the Bloom gate; there is no store leg to model.
	}
	if reached := counts["pg_hit"] + counts["redis_hit"]; reached == 0 {
		return fmt.Errorf("share=%.3f should have carried %d lookups to the store and the store was never "+
			"reached: the seed, the Bloom or the canonical form disagree, and this palier priced an L0 "+
			"stage that did nothing", share, ported)
	}

	wantPg := min(ported, uint64(max(pool, 1)))
	if gap := relGap(float64(counts["pg_hit"]), float64(wantPg)); gap > maxMixGap {
		return fmt.Errorf("pg_hit %d against the %d expected (min of %d ported lookups and a pool of %d): "+
			"a read-through cache cannot miss more distinct numbers than it holds, so a high reading means "+
			"the cache was flushed under the palier and a low one means it was not flushed before it",
			counts["pg_hit"], wantPg, ported, pool)
	}
	if gap := relGap(float64(counts["redis_hit"]), float64(ported-wantPg)); gap > maxMixGap {
		return fmt.Errorf("redis_hit %d against the %d expected (%d ported lookups less the %d first "+
			"touches): the ported draw did not repeat the way the pool says it should",
			counts["redis_hit"], ported-wantPg, ported, wantPg)
	}
	return nil
}

// relGap is the relative distance between an observation and its model, and it answers 0 when both are
// zero — the hot bound legitimately expects no redis_hit at all, and dividing there would reject it.
func relGap(got, want float64) float64 {
	if want == 0 {
		if got == 0 {
			return 0
		}
		return 1
	}
	return math.Abs(got-want) / want
}

// TestMixHoldsRefusesARunThatNeverReachedTheStore is the putsMatchSubmits of this bench.
//
// A seed can be present, a Bloom can be built, and the resolver can still answer bloom_miss for every
// message — a canonicalisation drift, a filter built before the rows landed, a share that rounded to
// zero. The palier would then publish a throughput measured with the L0 stage doing nothing at all,
// which is indistinguishable from the "without" side and would price the stage at zero.
func TestMixHoldsRefusesARunThatNeverReachedTheStore(t *testing.T) {
	counts := map[string]uint64{"bloom_miss": 100000}
	err := mixHolds(counts, 100000, 0.3, 1000)
	if err == nil {
		t.Fatal("a run where share=0.3 and the store was never read must not be accepted: it prices the " +
			"L0 stage at zero while looking like a measurement")
	}
	if !strings.Contains(err.Error(), "never") {
		t.Errorf("the error must say the store was never reached, got: %v", err)
	}
}

// TestMixHoldsRefusesAMixThatDoesNotTotalTheMessages carries resolver.go's "exactly one observation per
// resolution" all the way to the bench.
//
// It is also what stops an outcome the bench does not know about from vanishing: an unrecognised label
// must break the total, not be quietly dropped, or a vocabulary that grows upstream would silently
// shrink every ratio computed here.
func TestMixHoldsRefusesAMixThatDoesNotTotalTheMessages(t *testing.T) {
	short := map[string]uint64{"bloom_miss": 70000, "pg_hit": 1000, "redis_hit": 20000}
	if err := mixHolds(short, 100000, 0.3, 1000); err == nil {
		t.Error("a mix summing to 91% of the messages must be refused: one observation per resolution is " +
			"the invariant every ratio below rests on")
	}

	unknown := map[string]uint64{"bloom_miss": 70000, "pg_hit": 1000, "redis_hit": 29000, "teleported": 9000}
	if err := mixHolds(unknown, 109000, 0.3, 1000); err == nil {
		t.Error("an outcome the bench does not know must break the total rather than disappear")
	}
}

// TestMixHoldsJudgesBothEndpoints pins the model the whole reading rests on: with a flushed cache the
// FIRST touch of each distinct ported number is a pg_hit and every later touch is a redis_hit, so
// pg_hit is min(ported lookups, pool) at both ends of the dial.
func TestMixHoldsJudgesBothEndpoints(t *testing.T) {
	// Cold bound: pool (100000) outruns the ported lookups (30000), so every ported record pays Postgres.
	cold := map[string]uint64{"bloom_miss": 70000, "pg_hit": 30000}
	if err := mixHolds(cold, 100000, 0.3, 100000); err != nil {
		t.Errorf("the cold bound is the Postgres arm this bench exists to price: %v", err)
	}

	// Hot bound: 1000 distinct numbers under 30000 ported lookups, so 1000 pg_hit and the rest served
	// from Redis.
	hot := map[string]uint64{"bloom_miss": 70000, "pg_hit": 1000, "redis_hit": 29000}
	if err := mixHolds(hot, 100000, 0.3, 1000); err != nil {
		t.Errorf("the hot bound is the Redis arm: %v", err)
	}

	// The two swapped is not a reading, it is a bench that flushed when it should not have (or did not
	// when it should): 29000 cold misses against a pool of 1000 is arithmetically impossible.
	swapped := map[string]uint64{"bloom_miss": 70000, "pg_hit": 29000, "redis_hit": 1000}
	if err := mixHolds(swapped, 100000, 0.3, 1000); err == nil {
		t.Error("pg_hit above the pool size is impossible with a read-through cache: it must be refused, " +
			"not published as a Postgres cost")
	}
}

// TestMixHoldsRefusesAnErrorMix names the pool saturation before the CDR guard speaks.
//
// A saturated pgx pool surfaces as pg_error (Acquire waits up to DefaultLookupTimeout, then the lookup
// fails closed), the message is rejected, and the palier dies far away on "N messages rejected". That
// diagnosis is the ANSWER to step-280's second quantity, so it must be named where it happens.
func TestMixHoldsRefusesAnErrorMix(t *testing.T) {
	counts := map[string]uint64{"bloom_miss": 70000, "pg_hit": 900, "redis_hit": 29000, "pg_error": 100}
	err := mixHolds(counts, 100000, 0.3, 1000)
	if err == nil {
		t.Fatal("a palier with pg_error must not be published as a throughput")
	}
	if !strings.Contains(err.Error(), "MaxConns") {
		t.Errorf("the error must point at the pool as the first suspect, got: %v", err)
	}
}

// poolPressure renders what the L0 lookups asked of the pgx pool over the window.
//
// Every figure is a DELTA across the window (Stat counters are cumulative from the pool's birth, and the
// seed ran through the same pool before the palier opened), and every figure is per message, because
// that is the only form that transposes off this host: the representative environment will have a
// different rate and the same work per message.
//
// emptyAcquires is the one that matters. It is the count of Acquire calls that found no free connection
// and had to wait, and it is the leading edge of the failure mode step-280 names: the wait runs up to
// DefaultLookupTimeout, the lookup then fails closed, the record is redelivered, and the redelivery
// makes the same lookup against the same saturated pool.
func poolPressure(acquires, emptyAcquires int64, acquireWait time.Duration, messages uint64, maxConns int32, lanes int) string {
	if messages == 0 || acquires == 0 {
		return fmt.Sprintf("unreadable: %d acquisitions over %d messages — the L0 lookups never reached "+
			"the pool, so there is no pressure to report", acquires, messages)
	}
	perMsg := float64(acquires) / float64(messages)
	meanWait := time.Duration(int64(acquireWait) / acquires)
	out := fmt.Sprintf("%d acquisitions over %d messages (%.2f/message), mean wait %v, against MaxConns=%d "+
		"for %d lanes", acquires, messages, perMsg, meanWait.Round(time.Microsecond), maxConns, lanes)

	if emptyAcquires == 0 {
		return out + " · the pool never emptied over the window: this MaxConns was not the constraint here"
	}
	return fmt.Sprintf("%s · %d of them found the pool empty and waited (%.1f%%): this is the starvation "+
		"edge — past it Acquire waits out DefaultLookupTimeout, the lookup fails closed and the redelivery "+
		"makes the same lookup against the same pool", out, emptyAcquires,
		100*float64(emptyAcquires)/float64(acquires))
}

// cacheFootprint prices the exactroute keys THIS palier added, in bytes per key.
//
// It is the one figure here that transposes unchanged: step-250e could only estimate ~150-200 bytes and
// derived 1.3 to 10 GB on the Redis shared with the billing balances, and that estimate is what sizes
// maxmemory. Both readings are bracketed around the window because the instance holds whatever the
// previous palier left; only the difference belongs to this one.
func cacheFootprint(keysBefore, keysAfter, memBefore, memAfter int64) string {
	added := keysAfter - keysBefore
	if added <= 0 {
		return fmt.Sprintf("unreadable: %d keys before, %d after — this palier added none, so the memory "+
			"delta prices nothing", keysBefore, keysAfter)
	}
	perKey := (memAfter - memBefore) / added
	return fmt.Sprintf("%d keys added, %d B more used_memory: %d B per exactroute key (used_memory %d -> "+
		"%d; only the difference belongs to this palier)", added, memAfter-memBefore, perKey, memBefore, memAfter)
}

// TestPoolPressureNamesTheStarvation is step-280's second quantity, made observable.
//
// MaxConns is 10 by default and a router pod can own 12 Kafka lanes: Little's law says the margin
// disappears somewhere, and the failure mode is vicious — Acquire waits, the lookup fails closed, the
// record is redelivered, and the redelivery makes the same lookup against the same saturated pool. A
// figure that does not distinguish "the pool never emptied" from "it emptied N times" leaves that
// invisible until production finds it.
func TestPoolPressureNamesTheStarvation(t *testing.T) {
	starved := poolPressure(30000, 4200, 6*time.Second, 100000, 10, 12)
	if !strings.Contains(starved, "4200") {
		t.Errorf("a pool that emptied 4200 times must say so: %s", starved)
	}
	if !strings.Contains(starved, "10") || !strings.Contains(starved, "12") {
		t.Errorf("the figure is unreadable without MaxConns against the lane count: %s", starved)
	}

	healthy := poolPressure(30000, 0, 30*time.Millisecond, 100000, 24, 12)
	if strings.Contains(healthy, "starv") && !strings.Contains(healthy, "never") {
		t.Errorf("a pool that never emptied must be reported as such, not as starvation: %s", healthy)
	}
	if !strings.Contains(healthy, "never") {
		t.Errorf("silence on an empty-acquire count of zero reads as an unmeasured pool: %s", healthy)
	}
}

// TestPoolPressureRefusesToDivide mirrors TestStageLatencyRefusesToDivide: no messages means the
// resolver never ran, which is a fact worth reading, not a NaN worth misreading.
func TestPoolPressureRefusesToDivide(t *testing.T) {
	// The artefact this guards is a PANIC, not a NaN: both divisors are integers, so removing the guard
	// takes the palier down with "integer divide by zero" instead of printing a plausible figure. Asserting
	// on NaN here would be a clause that can never fire.
	got := poolPressure(0, 0, 0, 0, 10, 12)
	if !strings.Contains(got, "unreadable") {
		t.Errorf("an empty window must say it is unreadable: %s", got)
	}
}

// TestCacheFootprintPricesOnlyTheKeysItAdded turns step-280's third quantity from arithmetic into a
// constant.
//
// step-250e could only estimate "~150-200 bytes per key" and derived 1.3 to 10 GB on the Redis shared
// with the billing balances. Bytes per key is the one figure of this bench that transposes unchanged to
// the representative environment — provided it prices the keys THIS palier added and not the whole
// instance.
func TestCacheFootprintPricesOnlyTheKeysItAdded(t *testing.T) {
	// 1000 keys added, 180 KB more memory: 180 bytes per key, whatever else the instance held.
	// Anchored on the separator, not on the digits alone: pricing the ABSOLUTE memory would render
	// "4180 B per exactroute key", and a bare Contains("180 B") would swallow it — the substring trap
	// that turns this guard into decoration.
	got := cacheFootprint(50, 1050, 4_000_000, 4_180_000)
	if !strings.Contains(got, ": 180 B per exactroute key") {
		t.Errorf("the delta prices 180 bytes per key: %s", got)
	}

	if got := cacheFootprint(1050, 1050, 4_000_000, 4_180_000); !strings.Contains(got, "unreadable") {
		t.Errorf("a palier that added no key prices nothing and must say so: %s", got)
	}
}

// TestFidelityDeltaNamesItsSubject: the renderer is shared by two benches now, and a verdict that names
// the wrong one is worse than no verdict — it would file the cost of the L0 stage under the DLR write.
func TestFidelityDeltaNamesItsSubject(t *testing.T) {
	const subject = "the L0 stage"

	// The verdict branch.
	got, err := fidelityDelta([]float64{12000, 12100, 11900}, []float64{9000, 9100, 8900}, subject)
	if err != nil {
		t.Fatalf("three clean pairs must yield a verdict: %v", err)
	}
	if !strings.Contains(got, subject) {
		t.Errorf("the verdict must name what it priced: %s", got)
	}

	// The under-spread branch — a result, not a failure, and it must say what it could not price.
	got, err = fidelityDelta([]float64{12000, 13800, 12500}, []float64{12400, 13900, 12600}, subject)
	if err != nil {
		t.Fatalf("an unreadable delta is a result, not an error: %v", err)
	}
	if !strings.Contains(got, subject) {
		t.Errorf("the unreadable verdict must name what it could not price: %s", got)
	}

	// The error branch: the "with" side faster than the "without" side cannot happen for an added stage.
	_, err = fidelityDelta([]float64{9000, 9100, 8900}, []float64{12000, 12100, 11900}, subject)
	if err == nil {
		t.Fatal("a side that ran faster with the stage wired must be refused")
	}
	if !strings.Contains(err.Error(), subject) {
		t.Errorf("the refusal must name the subject too: %v", err)
	}
}

// countingLookups is the palier's own meter for the L0 resolver, on the pattern of countingProducer and
// countingCDR: atomics read from inside the window, nothing retained, no HTTP scrape competing for the
// host the palier is measuring.
//
// It satisfies exact.LookupMeter. The production wiring feeds the same observations into the Prometheus
// catalog (cmd/router-svc/wiring.go); this bench reads them straight because a counter vector would have
// to be scraped or reached through testutil, and both cost more than a map of atomics.
type countingLookups struct {
	mu sync.Mutex
	n  map[string]uint64
}

func newCountingLookups() *countingLookups { return &countingLookups{n: map[string]uint64{}} }

func (c *countingLookups) Observe(outcome string) {
	c.mu.Lock()
	c.n[outcome]++
	c.mu.Unlock()
}

func (c *countingLookups) snapshot() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.n)
}

// subtractMix is the window's own observations. The seed and the preflight both resolve through the same
// meter before the window opens, so the absolute counts would carry work the palier never did.
func subtractMix(before, after map[string]uint64) map[string]uint64 {
	if after == nil {
		return nil
	}
	out := make(map[string]uint64, len(after))
	for name, n := range after {
		out[name] = n - before[name]
	}
	return out
}

// TestSubtractMixKeepsOnlyTheWindow pins the direction of the subtraction, the way
// TestDeltaBucketsSubtractsTheOpeningReading does for the produce histogram.
//
// The seed and the preflight resolve through the same meter before the window opens. Reading the
// absolute counts would credit the palier with lookups it never made — and it would do so in the
// direction that makes the L0 stage look busier, which is the direction nobody questions.
func TestSubtractMixKeepsOnlyTheWindow(t *testing.T) {
	before := map[string]uint64{"bloom_miss": 100, "pg_hit": 7}
	after := map[string]uint64{"bloom_miss": 700, "pg_hit": 57, "redis_hit": 9}

	got := subtractMix(before, after)
	want := map[string]uint64{"bloom_miss": 600, "pg_hit": 50, "redis_hit": 9}
	for name, n := range want {
		if got[name] != n {
			t.Errorf("%s = %d after subtracting the opening reading, want %d", name, got[name], n)
		}
	}
	if subtractMix(nil, nil) != nil {
		t.Error("a palier with no probe must yield no mix, not an empty one a guard would judge")
	}
}

// TestPortedSetTerminatesAndMatchesTheDraw kills a whole class of hang.
//
// The seed used to enumerate the ported numbers by calling l0Dest until it had collected `pool` of them.
// That loop terminates only if l0Dest ever RETURNS a ported number — and a share under 0.0005 rounds to
// zero records per block while still clearing the share > 0 guard, so REF_PORTED_SHARE=0.0004 span the
// loop forever and the bench hung until the test timeout with no diagnosis at all.
//
// The enumeration is pure now, so termination is provable here rather than observable after forty
// minutes, and the empty answer is what the caller refuses.
func TestPortedSetTerminatesAndMatchesTheDraw(t *testing.T) {
	if got := portedSet(0.0004, 10); len(got) != 0 {
		t.Errorf("share=0.0004 rounds to no ported record per block: the set must be empty, got %d — a "+
			"non-empty answer here means the enumeration cannot terminate", len(got))
	}

	set := portedSet(0.3, 10)
	if len(set) != 10 {
		t.Fatalf("pool=10 must enumerate exactly 10 distinct numbers, got %d", len(set))
	}

	// Every member must be one l0Dest actually draws, and every number l0Dest draws must be a member:
	// a seed enumerated by a second formula is a seed that can disagree with the lookup, and the
	// disagreement reads as a clean 100% bloom_miss.
	member := make(map[string]bool, len(set))
	for _, msisdn := range set {
		member[msisdn] = true
	}
	for i := range 5000 {
		got := l0Dest(i, 0.3, 10)
		if got == nonPortedDest {
			continue
		}
		if !member[got] {
			t.Fatalf("l0Dest draws %s at index %d, which the seed would never write", got, i)
		}
	}
}
