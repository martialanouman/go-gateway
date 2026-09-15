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
// It TERMINATES over the whole domain, and the domain is 0 < num <= portedShareDen — both ends, not just
// the low one:
//
//   - num == 0 (a share under 0.0005) draws no ported record at all, so the loop has no exit.
//   - num > portedShareDen (a share above 1, which is what `PORTED_SHARE=30` means when the percentage is
//     typed where the fraction belongs) caps pos at portedShareDen-1 while advancing the ordinal by num
//     per block, so the ordinals become the strided set [num*k, num*k+999] instead of covering N. Whether
//     its residues cover `pool` then depends on gcd(num, pool) — PORTED_SHARE=30 with a pool of 100 000
//     reaches one residue in ten and never terminates, while a coprime stride would. The guard refuses
//     the whole half-plane rather than compute that: a share is a FRACTION, and above 1 it is a typo
//     whichever way the arithmetic falls.
//
// Outside that domain it refuses, and the refusal names WHICH lever is out of range: the guard folds
// three conditions, and a caller re-explaining them in one sentence necessarily explains two of them
// wrong. Enumerating through l0Dest rather than through a second formula is what keeps the seed and the
// lookup from disagreeing.
func portedSet(share float64, pool int) ([]string, error) {
	num := portedPerBlock(share)
	if num <= 0 || num > portedShareDen {
		return nil, fmt.Errorf("REF_PORTED_SHARE=%v is outside the domain this bench can draw: it is a "+
			"FRACTION, so it must land in [%v, 1]. Below that it rounds to no ported record per block and "+
			"the bench would price an L0 stage that never ran; above 1 the draw strides instead of "+
			"covering N, and whether it ever reaches the pool depends on gcd(num, pool) — a share is a "+
			"fraction either way", share, 1.0/portedShareDen)
	}
	if pool < 1 {
		return nil, fmt.Errorf("REF_PORTED_POOL=%d: the working set is what the draw covers, what the "+
			"seed writes into exact_routes and what the mix guard expects as pg_hit, so it must hold at "+
			"least one number", pool)
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
	return out, nil
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
//
// The pool of 0 is in the sweep for a second reason: portedSet REFUSES that working set, so nothing in
// the bench reaches l0Dest with it — which left its `pool < 1` clamp as a branch no test covered, and an
// uncovered clamp is one edit away from an integer division by zero in a pure function two benches call.
func TestL0DestIsCanonicalE164(t *testing.T) {
	for _, i := range []int{0, 1, 7, 300, 999, 1000, 999999} {
		for _, share := range []float64{0, 0.3, 1} {
			for _, pool := range []int{0, 1, 1000} {
				assertCanonicalDraw(t, i, share, pool)
			}
		}
	}
}

func assertCanonicalDraw(t *testing.T, i int, share float64, pool int) {
	t.Helper()
	got := l0Dest(i, share, pool)
	if !canonicalMSISDN.MatchString(got) {
		t.Fatalf("l0Dest(%d, %v, pool=%d) = %q, which the exact_routes CHECK rejects", i, share, pool, got)
	}
	norm, err := e164.Normalize(got)
	if err != nil {
		t.Fatalf("l0Dest(%d, %v, pool=%d) = %q: e164.Normalize rejects it (%v) — the router would never "+
			"look up what the seed wrote", i, share, pool, got, err)
	}
	if norm != got {
		t.Fatalf("l0Dest(%d, %v, pool=%d) = %q but normalizes to %q: the seed and the lookup would "+
			"disagree, and the bench would read 100%% bloom_miss as a result", i, share, pool, got, norm)
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

// The non-ported side is deliberately NOT split between bloom_miss and pg_miss. The router bench draws a
// single non-ported number, so a Bloom false positive on it is all-or-nothing: either no message pays
// Postgres for it, or every one of them does — the deterministic hammer ADR-0015 leaves open. Both are
// legitimate runs, so the guard holds their SUM and the renderer prints which one happened.
// mixCarriesItsShare judges what an outcome mix says INDEPENDENTLY of how warm the cache was: no lookup
// failed closed, no outcome outside the vocabulary, exactly one observation per resolution, and exactly
// the ported share of the traffic reaching the store.
//
// It is the half of the model that survives a HOT window, and the split exists because the other half
// does not. [mixHolds] predicts the pg_hit/redis_hit split from a cache flushed before the palier, which
// the router-only bench provides — FLUSHDB, then the whole consumption scored. The full-stack reference
// run provides neither: it holds a 20 s warmup, never flushes, and cycles its ring several times before
// the window opens, so every ported number is already cached. Handing it the cold model made it fail
// DETERMINISTICALLY at any share above zero — pg_hit ~0 against a model expecting the whole pool — with a
// message accusing the run of not flushing a cache it was never written to flush.
//
// `ported` is the caller's, because the two benches draw differently: the router bench seeds a prefill
// and its share is num/den, while the reference run samples Dest over a RING whose truncation shifts the
// real share (see portedInWindow). A share computed here would be right for one of them only.
func mixCarriesItsShare(counts map[string]uint64, messages, ported uint64) error {
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

	// The store leg, held on the COUNT that reached it rather than on how it was served. At share=0 this
	// asserts that nothing reached the store at all, which is the Bloom-gate palier's own claim — the
	// early `return nil` it used to take skipped the three checks above on the very shape the reference
	// run publishes by default.
	reached := counts["pg_hit"] + counts["redis_hit"]
	// Nothing at all is a wiring failure, not a drift, and it keeps its own sentence: a seed that never
	// bound, a Bloom built before the rows landed, a canonicalisation that disagrees. Folding it into the
	// relative gap below would report "100% off" for a run that priced an L0 stage doing nothing.
	if ported > 0 && reached == 0 {
		return fmt.Errorf("the draw carries %d lookups to the store and the store was never reached: the "+
			"seed, the Bloom or the canonical form disagree, and this window priced an L0 stage that did "+
			"nothing", ported)
	}
	if gap := relGap(float64(reached), float64(ported)); gap > maxMixGap {
		return fmt.Errorf("%d lookups reached the store against the %d the draw carries there (%.0f%% off): "+
			"either more traffic was ported than the ring draws, or part of the ported draw stopped at the "+
			"Bloom gate", reached, ported, 100*gap)
	}
	return nil
}

// mixHolds adds the COLD-window model to [mixCarriesItsShare]: with a cache flushed before the palier,
// the FIRST touch of each distinct ported number is a pg_hit and every later touch is a redis_hit, so
//
//	pg_hit    = min(ported lookups, pool)      <- the Postgres arm, maximal at the cold bound
//	redis_hit = ported lookups - pg_hit        <- the Redis arm, maximal at the hot bound
//
// Only a caller that FLUSHES may use it. A warm window makes it fail by construction.
func mixHolds(counts map[string]uint64, messages uint64, share float64, pool int) error {
	// Through portedPerBlock, not through a second rounding of share: the draw seeds num/den of every
	// block, so a model built on the raw share disagrees with the fixture by up to half a block.
	ported := uint64(portedPerBlock(share)) * messages / portedShareDen
	if err := mixCarriesItsShare(counts, messages, ported); err != nil {
		return err
	}
	if ported == 0 {
		return nil // the share=0 palier prices the Bloom gate; there is no store leg to split.
	}

	wantPg := min(ported, uint64(max(pool, 1)))
	if gap := relGap(float64(counts["pg_hit"]), float64(wantPg)); gap > maxMixGap {
		return fmt.Errorf("pg_hit %d against the %d expected (min of %d ported lookups and a pool of %d): "+
			"a high reading means either the cache was flushed under the palier, or two lanes raced the "+
			"same cold number — exact.Resolver has no singleflight, so a first touch is not strictly once "+
			"per number; a low one means the cache was not flushed before the palier",
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
// zero — the COLD bound legitimately expects no redis_hit at all (pool >= ported lookups makes every
// ported record a first touch), and dividing there would reject it. The hot bound is the opposite end:
// it expects mostly redis_hit, with pg_hit capped at the pool.
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
// Every figure is a DELTA across the window and per message, because that is the only form that
// transposes off this host.
//
// # None of pgxpool's counters measures a starvation on its own
//
// AcquireDuration is the total over ALL acquires, fast path included: a mean drawn from it is a mean
// acquire LATENCY, and it is dominated by the fast path: the runs here recorded 1 to 12 ms of
// empty-acquire wait spread over ~130 000 acquires, which contributes single-digit to double-digit
// NANOSECONDS to that mean. It is not that long waits vanish; it is that the mean answers a different
// question than the one asked, and cannot be read as "nobody waited".
//
// EmptyAcquireCount is not a queue count either — puddle increments it whenever a connection has to be
// CONSTRUCTED, including when the pool had room and nobody waited.
//
// EmptyAcquireWaitTime does not rescue it: on that same construction path puddle adds the whole connect
// and authentication time to it (puddle/pool.go, the branch that calls initResourceValue then
// increments emptyAcquireCount and emptyAcquireWaitTime together). A pool warming from MinConns to
// MaxConns therefore reports several "empty acquires" and tens of milliseconds of "wait" with nobody
// ever queued. Reading a non-zero wait as a starvation is the second version of this bench's mistake,
// in the opposite direction from the first.
//
// # What IS sound: a floor, not an equality
//
// At most newConns of the empty acquires can be a construction, so
//
//	contention floor = max(0, emptyAcquires - newConns)
//
// bounds from below the acquires that queued behind a genuinely full pool. It stays a floor rather than
// an equality because pgxpool also constructs OUTSIDE Acquire (createIdleResources feeds newConnsCount
// without touching emptyAcquireCount), which can only make it more conservative. The wait, symmetric,
// is an UPPER bound: it carries whatever connect time the window contained.
//
// # MaxConns bounds a concurrency, not a rate
//
// The router runs one goroutine per partition in a batch and each lane processes its records
// SEQUENTIALLY (internal/router/router.go, handleBatch), so one pod can never offer the pool more than
// `lanes` simultaneous acquires however fast it consumes. Comparing MaxConns to a request rate invites
// Little's law on a queue that is not shaped that way; the reading names the offered ceiling instead.
func poolPressure(acquires, emptyAcquires, newConns int64, emptyWait time.Duration, messages uint64,
	maxConns int32, lanes int,
) string {
	// Two ways in, two different facts: one sentence covering both would have to explain the missing
	// denominator by an absence of lookups it can see did happen.
	if acquires == 0 {
		return fmt.Sprintf("unreadable: no acquisition over %d messages — the L0 lookups never reached "+
			"the pool, so there is no pressure to report", messages)
	}
	if messages == 0 {
		return fmt.Sprintf("unreadable: %d acquisitions over no message — the window routed nothing, so "+
			"there is nothing to divide them by", acquires)
	}
	out := fmt.Sprintf("%d acquisitions over %d messages (%.2f/message), against MaxConns=%d for %d lanes "+
		"— a lane is sequential, so this pod can offer the pool at most %d acquires at once",
		acquires, messages, float64(acquires)/float64(messages), maxConns, lanes, lanes)

	floor := max(0, emptyAcquires-newConns)
	if floor == 0 {
		return fmt.Sprintf("%s · %d acquisitions found no idle connection and %d connections were built, "+
			"so the count is entirely covered: nothing here establishes a caller queueing, and nothing "+
			"establishes the %v of recorded wait was setup either — newConns also counts constructions "+
			"made outside Acquire, which consume the floor without ever having made anyone wait",
			out, emptyAcquires, newConns, roundWait(emptyWait))
	}
	// The TOTAL is the bound, not the total over the floor. One caller stuck behind a cold read and
	// thirty-two at fifteen microseconds sum exactly like thirty-three at the mean, so a per-caller
	// figure divided out of a sum is a mean whatever it is labelled — the fault this renderer charges
	// AcquireDuration with two paragraphs up.
	return fmt.Sprintf("%s · %d acquisitions found no idle connection and %d connections were built, so "+
		"at least %d of them queued behind a full pool — 1 in %d acquisitions. They waited %v in total, "+
		"so no single wait exceeded %v (their distribution is unknown, and the mean of %v is not a bound "+
		"on any one of them). This is the starvation edge: past it Acquire waits out "+
		"DefaultLookupTimeout, the lookup fails closed and the redelivery makes the same lookup against "+
		"the same pool",
		out, emptyAcquires, newConns, floor, acquires/floor, roundWait(emptyWait), roundWait(emptyWait),
		roundWait(emptyWait/time.Duration(floor)))
}

// roundWait rounds a wait to a precision it can actually carry. Rounding to the millisecond beside
// figures in microseconds prints "0s in total, 60µs each", which contradicts itself on the same line.
func roundWait(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(time.Microsecond)
	}
	return d.Round(time.Millisecond)
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
	grew := memAfter - memBefore
	if grew <= 0 {
		return fmt.Sprintf("unreadable: %d keys added but used_memory went from %d to %d — an instance "+
			"figure that fell while keys were written prices nothing (a client disconnected, a buffer was "+
			"released), and the integer division would render it as a negative cost per key",
			added, memBefore, memAfter)
	}
	perKey := grew / added
	return fmt.Sprintf("%d keys added, %d B more used_memory: %d B per exactroute key (used_memory %d -> "+
		"%d; only the difference belongs to this palier)", added, memAfter-memBefore, perKey, memBefore, memAfter)
}

// TestPoolPressureRefusesToCallConstructionAStarvation is the correction of a correction.
//
// The first version read AcquireDuration and EmptyAcquireCount and concluded no starvation; both are
// incapable of showing one. The second read EmptyAcquireWaitTime and called any non-zero wait a
// starvation — and that is wrong in the other direction, because puddle adds the CONNECT time to
// emptyAcquireWaitTime on the construction path (puddle/pool.go: emptyAcquireCount and
// emptyAcquireWaitTime are both incremented after initResourceValue returns). A pool warming from
// MinConns to MaxConns therefore reports several empty acquires and tens of milliseconds of "wait"
// while nobody ever queued.
//
// The only sound discrimination is the COUNT: at most newConns of the empty acquires can be a
// construction, so emptyAcquires - newConns is a floor on the acquires that queued behind a full pool.
// It is a floor and not an equality because pgxpool also constructs OUTSIDE Acquire
// (createIdleResources feeds newConnsCount without touching emptyAcquireCount), which can only make the
// floor more conservative.
func TestPoolPressureRefusesToCallConstructionAStarvation(t *testing.T) {
	// A pool warming under the first palier: seven empty acquires, seven constructions, and the connect
	// time recorded as wait. Nobody queued. (This bench opens at MinConns=2, so its own warm-up is eight;
	// seven is simply a reachable shape, not a transcript of a run.)
	warming := poolPressure(134183, 7, 7, 35*time.Millisecond, 446810, 10, 12)
	if strings.Contains(warming, "starvation") {
		t.Errorf("seven empty acquires fully covered by seven constructions is a pool warming up, not a "+
			"starvation: %s", warming)
	}
	if !strings.Contains(warming, "nothing here establishes") {
		t.Errorf("floor zero means the counters cannot show a queue — not that the wait WAS setup: "+
			"newConns also counts constructions made outside Acquire, which consume the floor without "+
			"having produced any wait at all. The branch must say what it cannot establish: %s", warming)
	}
	if strings.Contains(warming, "queued behind a full pool") {
		t.Errorf("a floor of zero must not be rendered as callers queueing: %s", warming)
	}
}

// TestPoolPressureNamesTheContentionFloor: the real runs had newConns = 0, so every empty acquire is a
// caller that queued — and that is the only reason the journal's conclusion stands.
func TestPoolPressureNamesTheContentionFloor(t *testing.T) {
	// The shape of /tmp/l0-cold3.log: 33 empty acquires, none of them a construction.
	got := poolPressure(143430, 33, 0, 12*time.Millisecond, 477397, 10, 12)
	if !strings.Contains(got, "at least 33") {
		t.Errorf("33 empty acquires against 0 constructions is a floor of 33 callers that queued: %s", got)
	}
	// The rarity is expressed as one-in-N and not as a percentage, because 33/143430 rounds to "0.0%",
	// which reads as "none" — the opposite of the finding. Asserting the ABSENCE of "0.0%" would be
	// unfalsifiable now that the renderer emits no percentage at all; asserting the ratio is what bites.
	if !strings.Contains(got, "1 in 4346") {
		t.Errorf("the rarity must be readable: %s", got)
	}
	// The TOTAL is the only per-caller upper bound these counters give: one caller stuck 11ms behind a
	// cold read and thirty-two at 15µs sum exactly like thirty-three at 365µs. Dividing by the floor and
	// calling the result "at most" is the same average the first version printed as "on average",
	// relabelled — the very fault this renderer's own godoc charges AcquireDuration with.
	if strings.Contains(got, "each at most") {
		t.Errorf("a mean relabelled as a per-caller bound is the fault this renderer exists to avoid: %s", got)
	}
	if !strings.Contains(got, "no single wait exceeded 12ms") {
		t.Errorf("the total is the bound that holds for one caller, and it must be stated as such: %s", got)
	}
}

// TestPoolPressureNamesTheOfferedConcurrency: MaxConns is a ceiling on a CONCURRENCY, and the router can
// only ever offer it one acquire per lane — handleBatch runs one goroutine per partition and each lane
// processes its records sequentially (internal/router/router.go). A figure that compares MaxConns to a
// request RATE invites Little's law on a quantity that is not queued that way.
func TestPoolPressureNamesTheOfferedConcurrency(t *testing.T) {
	got := poolPressure(143430, 33, 0, 12*time.Millisecond, 477397, 10, 12)
	if !strings.Contains(got, "at most 12") {
		t.Errorf("the reading is unreadable without the concurrency the router can actually offer: %s", got)
	}
}

// TestPoolPressureRefusesToDivide mirrors TestStageLatencyRefusesToDivide: no messages means the
// resolver never ran, which is a fact worth reading, not a NaN worth misreading.
func TestPoolPressureRefusesToDivide(t *testing.T) {
	// The artefact is a float division by zero on the per-message figure, so removing the guard prints
	// "NaN/message" rather than panicking — the earlier integer form did panic, and this comment said so
	// after the form had changed.
	got := poolPressure(0, 0, 0, 0, 0, 10, 12)
	if !strings.Contains(got, "unreadable") {
		t.Errorf("an empty window must say it is unreadable: %s", got)
	}
	if strings.Contains(got, "NaN") {
		t.Errorf("an empty window must not render as an arithmetic artefact: %s", got)
	}

	// The OTHER way into the same branch, and the one the single condition explained wrong: acquisitions
	// with no message. "the L0 lookups never reached the pool" is then false — they reached it, and what
	// is missing is the denominator. A renderer that explains an unreadable figure by a cause its own
	// inputs rule out is the fault this file keeps finding in itself.
	if got := poolPressure(500, 3, 0, time.Millisecond, 0, 10, 12); strings.Contains(got, "never reached") {
		t.Errorf("500 acquisitions did reach the pool; it is the message count that is missing: %s", got)
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

	// used_memory is an INSTANCE figure, not a sum over keys: a client disconnecting inside the window
	// releases its buffers and the delta can go backwards while keys were written. Integer division
	// would then render "-3 B per exactroute key" as a cost.
	if got := cacheFootprint(50, 1050, 4_180_000, 4_000_000); !strings.Contains(got, "unreadable") {
		t.Errorf("a used_memory that fell while keys were written prices nothing: %s", got)
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
	// The refusal's REASON has to hold for whatever the subject is. "no added write can do that" is true
	// of the DLR store and false of L0, which is a read: a sentence that names the L0 stage and then
	// explains it by a write is a verdict nobody can act on.
	if strings.Contains(err.Error(), "write") {
		t.Errorf("the refusal explains itself by a WRITE while pricing %q, which is not one: %v", subject, err)
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

// renderMix prints the outcome mix as counts and as lookups per message.
func renderMix(mix map[string]uint64, messages uint64) string {
	if len(mix) == 0 || messages == 0 {
		return "no observation"
	}
	var total uint64
	out := ""
	for _, name := range l0Outcomes {
		n := mix[name]
		total += n
		if n == 0 {
			continue
		}
		out += fmt.Sprintf("%s %d (%.1f%%) · ", name, n, 100*float64(n)/float64(messages))
	}
	return fmt.Sprintf("%s%d lookups over %d messages (%.2f/message)",
		out, total, messages, float64(total)/float64(messages))
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
// minutes, and the refusal is what the caller propagates.
func TestPortedSetTerminatesAndMatchesTheDraw(t *testing.T) {
	if got, err := portedSet(0.0004, 10); err == nil {
		t.Errorf("share=0.0004 rounds to no ported record per block: the draw must be refused, got %d "+
			"numbers — a non-empty answer here means the enumeration cannot terminate", len(got))
	}

	// The OTHER end of the same class, and the one the first guard missed. `make load-reference
	// PORTED_SHARE=30` — the percentage typed where a fraction belongs — gives num=30000. pos is capped
	// at portedShareDen-1, so the ordinals a block yields are [30000k, 30000k+999]: a strided set whose
	// residues mod pool cover only a fraction of it, and the enumeration never collects `pool` of them.
	// The terminating domain is 0 < num <= portedShareDen, not num > 0.
	if got, err := portedSet(30, 100000); err == nil {
		t.Errorf("a share above 1 draws a strided ordinal set that cannot cover the pool: the enumeration "+
			"must refuse it, got %d numbers", len(got))
	}

	set, err := portedSet(0.3, 10)
	if err != nil {
		t.Fatalf("pool=10 at share=0.3 is inside the domain: %v", err)
	}
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

// TestRenderMixCountsWhatItDivides: the "lookups per message" figure is the end-to-end check of
// resolver.go's one-observation-per-resolution invariant, so it must be the sum of what was OBSERVED
// over the messages routed — not a share of a total the renderer assumed.
func TestRenderMixCountsWhatItDivides(t *testing.T) {
	// A mix that does NOT total the messages is the falsifying case, and it has to be here: at
	// total == messages the observed sum and the message count give the same ratio, so a renderer that
	// divided by the wrong one would read 1.00 either way. mixHolds is what refuses such a mix upstream;
	// this test asks the renderer to REPORT it rather than launder it.
	short := renderMix(map[string]uint64{"bloom_miss": 63000, "redis_hit": 26100, "pg_hit": 900}, 100000)
	if !strings.Contains(short, "(0.90/message)") {
		t.Errorf("90000 observations over 100000 messages is 0.90 per message, not a ratio of the "+
			"messages by themselves: %s", short)
	}

	got := renderMix(map[string]uint64{"bloom_miss": 70000, "redis_hit": 29000, "pg_hit": 1000}, 100000)
	if !strings.Contains(got, "(1.00/message)") {
		t.Errorf("100000 observations over 100000 messages is 1.00 per message: %s", got)
	}
	if !strings.Contains(got, "bloom_miss 70000 (70.0%)") {
		t.Errorf("each outcome must carry its own share: %s", got)
	}
	if strings.Contains(got, "redis_error") || strings.Contains(got, "pg_miss") {
		t.Errorf("an outcome never observed must not be printed as a zero row: %s", got)
	}

	// A window with no messages must not render a ratio: dividing there is how "the resolver never ran"
	// becomes "the resolver was free".
	if got := renderMix(map[string]uint64{"bloom_miss": 1}, 0); strings.Contains(got, "/message") {
		t.Errorf("an empty window must not be rendered as a per-message figure: %s", got)
	}
}

// legacyDest is the destination every row of test/load/README.md before 09/09/2026 was measured
// against: one number, for every record.
//
// It is a named function rather than a nil default in newRouterBed because the sweep runs on it, and a
// nil default would be the single path no test covers while carrying the comparability of the whole
// journal.
func legacyDest(int) string { return nonPortedDest }

// TestLegacyDestIsTheJournalsFixture is what actually proves the sweep still addresses what it always
// addressed — l0Dest does not, since the sweep never calls it.
func TestLegacyDestIsTheJournalsFixture(t *testing.T) {
	for _, i := range []int{0, 1, 999, 1000, 123456} {
		if got := legacyDest(i); got != "2250700000000" {
			t.Errorf("legacyDest(%d) = %q, want the literal every earlier row was measured against", i, got)
		}
	}
}

// TestRoundWaitKeepsSubMillisecondReadable: rounding to the millisecond beside figures in microseconds
// prints "0s in total, 60µs each" — a line that contradicts itself. The waits this bench measures are
// 826µs to 12ms, so both sides of that boundary are live.
func TestRoundWaitKeepsSubMillisecondReadable(t *testing.T) {
	if got := roundWait(826 * time.Microsecond); got != 826*time.Microsecond {
		t.Errorf("a sub-millisecond total must keep its microseconds, got %v", got)
	}
	if got := roundWait(12*time.Millisecond + 400*time.Microsecond); got != 12*time.Millisecond {
		t.Errorf("a millisecond-scale total rounds to the millisecond, got %v", got)
	}
	if got := roundWait(0); got != 0 {
		t.Errorf("zero stays zero, got %v", got)
	}
}

// TestPortedSetNamesWhichLeverIsOutOfDomain: a refusal that blames the wrong lever sends the reader to
// the knob that was already right.
//
// The single guard this file shipped with folded three conditions into one empty answer, and the caller
// re-explained it in ONE sentence — the share's. `make load-reference PORTED_POOL=0` therefore died on
// "REF_PORTED_SHARE=0.3 is outside the domain ... it must land in [0.001, 1]", naming a domain that
// contains 0.3. Which lever is out of range is the whole content of the message.
func TestPortedSetNamesWhichLeverIsOutOfDomain(t *testing.T) {
	// A pool of zero, with a share the bench draws perfectly well.
	_, err := portedSet(0.3, 0)
	if err == nil {
		t.Fatal("a working set of zero numbers seeds nothing and must be refused")
	}
	if !strings.Contains(err.Error(), "REF_PORTED_POOL") {
		t.Errorf("the refusal must name the lever that is out of range: %v", err)
	}
	if strings.Contains(err.Error(), "REF_PORTED_SHARE") {
		t.Errorf("share=0.3 is inside the domain: naming it sends the reader to the knob that was "+
			"right: %v", err)
	}

	// Both ends of the share domain still name the share, and neither mentions the pool, which is sound.
	for _, share := range []float64{0.0004, 30} {
		_, err := portedSet(share, 100000)
		if err == nil {
			t.Fatalf("share=%v is outside the domain the draw terminates over and must be refused", share)
		}
		if !strings.Contains(err.Error(), "REF_PORTED_SHARE") {
			t.Errorf("share=%v: the refusal must name the share: %v", share, err)
		}
		if strings.Contains(err.Error(), "REF_PORTED_POOL") {
			t.Errorf("share=%v: a pool of 100000 is not what is out of range: %v", share, err)
		}
	}

	if _, err := portedSet(0.3, 10); err != nil {
		t.Errorf("share=0.3 with a pool of 10 is inside the domain: %v", err)
	}
}
