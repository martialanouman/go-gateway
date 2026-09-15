package e2e_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

// destBlockSize is how many numbers the +2250700xxxxxx fixture block holds. Both halves of the draw
// live in it, so the ported working set and the non-ported spread must fit side by side.
const destBlockSize = 1000000

// refDest composes the FULL-STACK reference run's destination for a payload-ring index.
//
// The router-only bench of step-270c draws its non-ported traffic from one literal, and that is right
// there: the declarative resolver those messages fall through to is prefix-based, so a million numbers
// exercise it exactly as one does. The full-stack run cannot borrow that. Its destination also drives
// the route entry and the anti-spam counter — the reason the injector pre-renders a RING of bodies at
// all (test/load/steady/inject.go) — and one number would put a whole run on one of each.
//
// So the halves are composed rather than chosen. The ported half is l0Dest UNCHANGED: one function
// draws the seed and the lookup, so they cannot disagree. Every other index spreads over
// pool+1+i%ring, and the two blocks are disjoint by ARITHMETIC rather than by convention — l0Dest's
// ported numbers live in [1, pool]. A "non-ported" destination that is ported in exact_routes would be
// an L0 hit that mixHolds cannot predict from the share, and the mix it models is the whole reading.
//
// The pool clamp mirrors l0Dest's own: below one it draws 2250700000001, so the spread has to start
// above that number and not above zero.
func refDest(i, ring int, share float64, pool int) string {
	if d := l0Dest(i, share, pool); d != nonPortedDest {
		return "+" + d
	}
	if pool < 1 {
		pool = 1
	}
	return fmt.Sprintf("+2250700%06d", pool+1+i%ring)
}

// ringCoversPool refuses a payload ring too narrow to draw the ported working set the seed writes.
//
// This is the trap step-270d exists for. newPayloads samples Dest over [0, ring) ONCE and cycles the
// pre-rendered bodies for the whole run, so the ring — never the run's length — bounds the distinct
// destinations. l0Dest draws num ported numbers per block of portedShareDen indices, so a ring of R
// reaches ceil-ish R/den * num consecutive ordinals: at the default 4 096 and a share of 0.3, that is
// 1 200 ported numbers whatever REF_PORTED_POOL says.
//
// The failure mode is the dangerous one. The run would hold its rate, the seed would look right, and
// mixHolds would read a mix of 100 % redis_hit — which is a LEGITIMATE warm bound, the one step-270c
// published. The measurement would be wrong and green. It refuses instead.
//
// Two levers, two branches, never folded: a caller re-explaining a folded guard in one sentence
// necessarily explains the other wrong (the lesson portedSet paid for).
func ringCoversPool(ring int, share float64, pool int) error {
	if ring < 1 {
		return fmt.Errorf("REF_DEST_RING=%d: the ring is how many bodies the injector pre-renders, so it "+
			"must hold at least one", ring)
	}
	num := portedPerBlock(share)

	// The domain of the two ported levers, mirrored from portedSet. refL0Shape calls nothing else, so a
	// refusal portedSet owns and this guard does not is a refusal paid after three containers and a
	// ten-second calibration — and, worse, the branches below would first phrase it in numbers that
	// contradict each other (a share of 30 reports 120 096 draws as "short of" a pool of 100 000).
	// num == 0 is NOT in here: it is the default, and it means no ported traffic rather than a typo.
	if num > portedShareDen {
		return fmt.Errorf("REF_PORTED_SHARE=%v is outside the domain the draw covers: it is a FRACTION, so "+
			"it must land in [%v, 1]. Above 1 the draw strides instead of covering N, and whether it ever "+
			"reaches the pool depends on gcd", share, 1.0/portedShareDen)
	}
	if num > 0 && pool < 1 {
		return fmt.Errorf("REF_PORTED_POOL=%d: the working set is what the draw covers, what the seed "+
			"writes into exact_routes and what the mix guard expects, so it must hold at least one number", pool)
	}

	// The block bound, and it is read on the CLAMPED pool because that is the one refDest draws from:
	// below one it spreads from 2, not from pool+1. At share=0 — the default — portedSet is never called,
	// so this is the only thing that validates REF_PORTED_POOL at all.
	//
	// The comparison is >=, not >: refDest's last value is pool+1+(ring-1) = pool+ring, and slot 0 is
	// taken by the non-ported literal, so the widest drawable number is destBlockSize-1. Accepting the
	// boundary rendered "+22507001000000" — fourteen digits, refused by e164.Normalize, answered 400 by
	// the ingress, and killed by the error clause after the whole window had run.
	if drawn := max(pool, 1) + ring; drawn >= destBlockSize {
		return fmt.Errorf("REF_PORTED_POOL=%d and REF_DEST_RING=%d draw up to %d, past the %d numbers the "+
			"+2250700xxxxxx fixture block holds: the spread would leave the block and the ingress would "+
			"refuse every number past its end", pool, ring, drawn, destBlockSize-1)
	}

	if num > 0 && ring < minRingFor(num, pool) {
		return fmt.Errorf("REF_DEST_RING=%d draws %d distinct ported numbers at REF_PORTED_SHARE=%v, "+
			"short of the REF_PORTED_POOL=%d the seed writes: the run would touch a fraction of the "+
			"working set, read a mix its guard cannot predict, and publish a Postgres read rate of nil. "+
			"Raise the ring to at least %d",
			ring, portedReach(ring, num), share, pool, minRingFor(num, pool))
	}
	return nil
}

// portedReach is how many of a ring's indices are PORTED draws, when l0Dest takes num of every
// portedShareDen. The ordinals it produces are consecutive from zero, so the count is the answer and no
// set has to be built.
//
// It is a count of draws, not of distinct numbers: those are min(portedReach, pool), and the two differ
// exactly when the ring already covers the pool — which is the case the coverage branch does not report.
func portedReach(ring, num int) int {
	return (ring/portedShareDen)*num + min(ring%portedShareDen, num)
}

// minRingFor is the narrowest ring that draws `pool` distinct ported numbers. It is the ONE place the
// covering ring is computed, so the refusal's advice cannot drift from the condition that produced it
// — the first version divided pool*den/num and named 5 000 where 4 001 covers, which is a document
// that is merely almost right and therefore obeyed wrongly.
//
// A pool that is a whole number of blocks stops at the last ported index of its last block rather than
// at the block boundary, hence the two cases.
//
// There is deliberately no pool < 1 guard, and the reason is now upstream rather than incidental: the
// caller refuses a pool below one whenever the share is drawable, so this is only ever reached with
// pool >= 1. Before that guard existed the expression yielded -700 at pool=0 and the coverage branch
// silently could not fire.
func minRingFor(num, pool int) int {
	blocks, rest := pool/num, pool%num
	if rest == 0 {
		return (blocks-1)*portedShareDen + num
	}
	return blocks*portedShareDen + rest
}

// TestRefDestKeepsTheTwoBlocksDisjoint is the property the whole composition exists for: no index the
// draw calls non-ported may land on a number the seed wrote into exact_routes.
//
// The pool must exceed the per-block ported count in at least one case, and that is the whole fixture
// rather than a detail. Removing the pool+1 offset leaves the spread at 1+i%ring, which can only reach
// [1, pool] at a SMALL index — and small indices (pos < num) are exactly the ported ones. At pool=10
// against 300 ported per block the collision is unreachable, so the property holds by the geometry of
// the fixture rather than by the code, and the test passes with the offset deleted.
func TestRefDestKeepsTheTwoBlocksDisjoint(t *testing.T) {
	for _, tc := range []struct {
		share float64
		pool  int
		ring  int
	}{
		{0.3, 1000, 4096}, // pool > num: the only shape in which the offset is falsifiable
		{0.3, 10, 4096},
		{0.3, 0, 512},
		{0.001, 1, 2048},
		{1, 5, 64},
	} {
		t.Run(fmt.Sprintf("share=%v/pool=%d", tc.share, tc.pool), func(t *testing.T) {
			ported, err := portedSet(tc.share, max(tc.pool, 1))
			if err != nil {
				t.Fatalf("portedSet: %v", err)
			}
			seeded := make(map[string]bool, len(ported))
			for _, p := range ported {
				seeded[p] = true
			}

			for i := range tc.ring {
				got := refDest(i, tc.ring, tc.share, tc.pool)
				if l0Dest(i, tc.share, tc.pool) != nonPortedDest {
					continue // this index IS ported; it is meant to be in the set
				}
				if seeded[strings.TrimPrefix(got, "+")] {
					t.Fatalf("refDest(%d) = %q is drawn as non-ported but the seed wrote it into "+
						"exact_routes: it would be an L0 hit the mix guard cannot predict from the share", i, got)
				}
			}
		})
	}
}

// TestRefDestDrawsThePortedNumbersL0DestDraws pins "no copy": the ported half must be l0Dest's own
// output, so the seed (which enumerates through l0Dest) and the lookup cannot drift apart.
//
// The pool sweep carries the trap. l0Dest's ordinal differs from the index by exactly 700*(i/den), so
// a naive second formula — 1+i%pool — is CONGRUENT to it whenever pool divides 700. At pool=100, the
// obvious round number, the two agree on all 4 096 indices and this test passes against a copy. 13 is
// there because it does not divide 700, and it is the only case that can fail.
func TestRefDestDrawsThePortedNumbersL0DestDraws(t *testing.T) {
	const (
		ring  = 4096
		share = 0.3
	)
	for _, pool := range []int{13, 100, 100000} {
		t.Run(fmt.Sprintf("pool=%d", pool), func(t *testing.T) {
			ported := 0
			for i := range ring {
				want := l0Dest(i, share, pool)
				if want == nonPortedDest {
					continue
				}
				ported++
				if got := refDest(i, ring, share, pool); got != "+"+want {
					t.Fatalf("refDest(%d) = %q, want l0Dest's %q with the E.164 plus: a second formula here "+
						"is how the seed and the lookup start disagreeing", i, got, "+"+want)
				}
			}
			if ported == 0 {
				t.Fatal("no index was ported, so the assertion above never ran")
			}
		})
	}
}

// TestRefDestSpreadsTheWholeRingWhenNothingIsPorted is the default configuration of the reference run,
// and the reason the composition exists at all: l0Dest alone returns ONE literal at share=0, which
// would collapse the run to a single destination — the opposite of what the lever buys.
func TestRefDestSpreadsTheWholeRingWhenNothingIsPorted(t *testing.T) {
	const ring = 512

	distinct := map[string]bool{}
	for i := range ring {
		distinct[refDest(i, ring, 0, 1000)] = true
	}
	if len(distinct) != ring {
		t.Errorf("a ring of %d drew %d distinct destinations at share=0, want the whole ring: l0Dest alone "+
			"draws the single non-ported literal there", ring, len(distinct))
	}
}

// TestRefDestIsCanonicalE164 holds both halves to what the pipeline and the table accept: the ingress
// takes the leading plus, exact_routes takes the digits (the CHECK of
// migrations/0004_exact_routes_msisdn_canonical.up.sql, pinned by canonicalMSISDN).
//
// The negative pool is what covers the clamp, and nothing else does. A pool of zero leaves the clamp
// unobservable — the one index whose spread would collide is index 0, which is ported at every share
// this bench can draw. A negative one makes the offset itself negative, and %06d then renders a minus
// sign into an MSISDN: "2250700-00004", which Postgres refuses and no guard here would have named.
func TestRefDestIsCanonicalE164(t *testing.T) {
	for _, share := range []float64{0, 0.3, 1} {
		for _, pool := range []int{-5, 0, 1, 1000} {
			for i := range 2048 {
				assertDrawable(t, i, 2048, share, pool)
			}
		}
	}
}

// TestRefDestStaysInsideTheBlockAtTheBoundary is the case the canonical sweep above cannot reach and
// ringCoversPool is supposed to keep out: the widest ring the guard admits.
//
// It is the falsifying case for the guard's own arithmetic. refDest draws pool+1+i, so its LAST value is
// pool+ring — one past what pool+ring <= destBlockSize allows, and the slot the non-ported literal
// already occupies is 0. At the accepted boundary the draw rendered "+22507001000000", fourteen digits,
// which e164.Normalize rejects: the ingress answers 400, steady.Criteria tolerates zero errors, and the
// run dies after the full window on a diagnosis that names HTTP rather than the lever that caused it.
func TestRefDestStaysInsideTheBlockAtTheBoundary(t *testing.T) {
	for _, tc := range []struct{ ring, pool int }{
		{destBlockSize - 2, 0}, // pool clamped to 1, so the last draw is exactly destBlockSize-1
		{destBlockSize - 2, 1},
		{900000, 99999},
		{destBlockSize / 2, destBlockSize/2 - 1},
	} {
		t.Run(fmt.Sprintf("ring=%d/pool=%d", tc.ring, tc.pool), func(t *testing.T) {
			if err := ringCoversPool(tc.ring, 0, tc.pool); err != nil {
				t.Fatalf("ringCoversPool(%d, 0, %d) = %v, want nil: this is the widest admissible shape",
					tc.ring, tc.pool, err)
			}
			// The last index is the only one that can overflow, so assert it first and by name.
			assertDrawable(t, tc.ring-1, tc.ring, 0, tc.pool)
			assertDrawable(t, 0, tc.ring, 0, tc.pool)
		})
	}
}

// assertDrawable holds a draw to what the whole chain accepts, and the e164 leg is the one that matters:
// canonicalMSISDN is ^[1-9][0-9]+$ with NO length bound, so it passes a fourteen-digit number that no
// numbering plan contains. refl0_test.go pays this lesson already — "Postgres would catch the '+' via the
// CHECK; nothing would catch the second" — and refDest is the draw that can leave the block.
func assertDrawable(t *testing.T, i, ring int, share float64, pool int) {
	t.Helper()

	got := refDest(i, ring, share, pool)
	digits, ok := strings.CutPrefix(got, "+")
	if !ok {
		t.Fatalf("refDest(%d, ring=%d, share=%v, pool=%d) = %q, want the leading plus the REST ingress takes",
			i, ring, share, pool, got)
	}
	if !canonicalMSISDN.MatchString(digits) {
		t.Fatalf("refDest(%d, ring=%d, share=%v, pool=%d) = %q: %q is not the canonical form exact_routes "+
			"accepts", i, ring, share, pool, got, digits)
	}
	norm, err := e164.Normalize(digits)
	if err != nil {
		t.Fatalf("refDest(%d, ring=%d, share=%v, pool=%d) = %q: e164.Normalize rejects it (%v) — the "+
			"ingress would answer 400 and the run would die on its error clause", i, ring, share, pool, got, err)
	}
	if norm != digits {
		t.Fatalf("refDest(%d, ring=%d, share=%v, pool=%d) = %q but normalizes to %q: the injected number "+
			"and the looked-up key would differ", i, ring, share, pool, got, norm)
	}
}

// TestRingCoversPoolRefusesTheBlockBoundary is the falsifying pair the overflow branch never had. It was
// only ever asked at 1.2x the bound, where every variant of the inequality is green — the exact reproach
// TestRingCoversPoolTurnsOnTheRingItAdvises makes to the other branch.
func TestRingCoversPoolRefusesTheBlockBoundary(t *testing.T) {
	if err := ringCoversPool(destBlockSize-1, 0, 1); err == nil {
		t.Error("ringCoversPool(ring=destBlockSize-1, pool=1) = nil: the last draw is pool+ring = " +
			"destBlockSize, one past the block")
	}
	if err := ringCoversPool(destBlockSize-2, 0, 1); err != nil {
		t.Errorf("ringCoversPool(ring=destBlockSize-2, pool=1) = %v, want nil: the last draw is exactly "+
			"destBlockSize-1, the widest number the block holds", err)
	}
}

// TestRingCoversPoolReadsTheClampedPool: refDest clamps pool below one, the guard must weigh the same
// number. At share=0 — the DEFAULT — portedSet is never called, so this branch is the only thing that
// validates REF_PORTED_POOL at all.
func TestRingCoversPoolReadsTheClampedPool(t *testing.T) {
	if err := ringCoversPool(destBlockSize-1, 0, -100000); err == nil {
		t.Error("ringCoversPool(ring=destBlockSize-1, pool=-100000) = nil: refDest clamps the pool to 1 " +
			"and draws up to 1+ring, so the guard must not credit the negative pool with room it does not buy")
	}
}

// TestPortedReachCountsTheDraw gives portedReach the assertion it never had. Every one of its uses is
// inside a message — the refusal's diagnosis, and an Errorf reached only once a test has already failed
// — so dropping its remainder term stayed green across the whole suite while the refusal told an
// operator a false count.
func TestPortedReachCountsTheDraw(t *testing.T) {
	for _, num := range []int{1, 2, 7, 300, 999, 1000} {
		for _, ring := range []int{1, 99, 300, 301, 1000, 1001, 4096} {
			want := 0
			for i := range ring {
				if i%portedShareDen < num {
					want++
				}
			}
			if got := portedReach(ring, num); got != want {
				t.Errorf("portedReach(%d, %d) = %d, want %d: the count the refusal reports must be the "+
					"draw the run actually makes", ring, num, got, want)
			}
		}
	}
}

// TestRingCoversPoolMirrorsThePortedSetDomain: refL0Shape exists to refuse BEFORE the containers start,
// and it only calls ringCoversPool. Every refusal portedSet owns but this guard does not is a refusal
// paid after three containers and a ten-second calibration — or, worse, a refusal phrased in numbers
// that contradict themselves.
func TestRingCoversPoolMirrorsThePortedSetDomain(t *testing.T) {
	// The percentage typed where the fraction belongs — the case portedSet names in full.
	err := ringCoversPool(4096, 30, 100000)
	if err == nil {
		t.Fatal("ringCoversPool(share=30) = nil: a share above 1 strides the draw, and portedSet refuses it " +
			"after the containers are up")
	}
	if !strings.Contains(err.Error(), "REF_PORTED_SHARE") {
		t.Errorf("the refusal is %q, want it to name the lever that is out of range", err)
	}
	if strings.Contains(err.Error(), "short of") {
		t.Errorf("the refusal is %q — the coverage branch, which reports 120096 numbers as 'short of' "+
			"100000 and advises a ring below the pool", err)
	}

	// A pool of zero with a drawable share: portedSet refuses it, this guard must too.
	err = ringCoversPool(4096, 0.3, 0)
	if err == nil {
		t.Fatal("ringCoversPool(share=0.3, pool=0) = nil: minRingFor yields a negative ring there, so the " +
			"coverage branch cannot fire and the refusal lands after the seed")
	}
	if !strings.Contains(err.Error(), "REF_PORTED_POOL") {
		t.Errorf("the refusal is %q, want it to name the lever that is out of range", err)
	}
}

// TestRingCoversPoolRefusesARingTooNarrowForThePool is the guard the default configuration would have
// walked straight past: 4 096 destinations against the 100 000 ported numbers step-270c seeds.
func TestRingCoversPoolRefusesARingTooNarrowForThePool(t *testing.T) {
	err := ringCoversPool(4096, 0.3, 100000)
	if err == nil {
		t.Fatal("ringCoversPool(ring=4096, share=0.3, pool=100000) = nil: the ring draws 1 200 ported " +
			"numbers, so the run would price a working set it never touched")
	}
	if !strings.Contains(err.Error(), "REF_DEST_RING") || !strings.Contains(err.Error(), "REF_PORTED_POOL") {
		t.Errorf("the refusal is %q — it must name BOTH levers, because either one is a valid way out", err)
	}
	if strings.Contains(err.Error(), "fixture block holds") {
		t.Errorf("the refusal is %q, which is the block-overflow message: two levers, two branches", err)
	}
}

// smallestCoveringRing counts what the draw ACTUALLY reaches, index by index, until it has seen the
// whole pool.
//
// It exists because the obvious expectation is circular. minRingFor is both the number the refusal
// advises and the condition ringCoversPool tests, so a test that derived its expectation from
// minRingFor would move with it: replacing the formula by pool*den/num — the wrong one this file
// shipped first — leaves such a test green, because both sides shift together. The only expectation
// that can falsify the formula is one built from l0Dest.
func smallestCoveringRing(t *testing.T, share float64, pool int) int {
	t.Helper()

	seen := make(map[string]bool, pool)
	limit := (pool + 1) * portedShareDen
	for i := range limit {
		if d := l0Dest(i, share, pool); d != nonPortedDest {
			seen[d] = true
			if len(seen) == pool {
				return i + 1
			}
		}
	}
	t.Fatalf("the draw did not reach %d distinct ported numbers in %d indices at share=%v", pool, limit, share)
	return 0
}

// TestRingCoversPoolTurnsOnTheRingItAdvises is the falsifying pair, and it holds the refusal's ADVICE
// to the same bar as its verdict: the ring it names must be the exact turn-on point, one narrower must
// refuse. A guard that only ever sees a ring ten times too small proves nothing about where it draws
// the line, and an advised number nobody checks is how "raise it to 5 000" outlives "4 001 covers".
func TestRingCoversPoolTurnsOnTheRingItAdvises(t *testing.T) {
	for _, tc := range []struct {
		share float64
		pool  int
	}{
		{0.3, 100},    // inside the first block: no block boundary crossed
		{0.3, 300},    // a whole block of ported draws — the case that stops short of the boundary
		{0.3, 301},    // one past it, so the second block is entered for a single number
		{0.3, 100000}, // step-270c's own working set
		{0.001, 5},    // the narrowest drawable share: one ported number per block
	} {
		t.Run(fmt.Sprintf("share=%v/pool=%d", tc.share, tc.pool), func(t *testing.T) {
			want := smallestCoveringRing(t, tc.share, tc.pool)

			if got := minRingFor(portedPerBlock(tc.share), tc.pool); got != want {
				t.Errorf("minRingFor advises a ring of %d, but the draw reaches the %d-number pool at %d: "+
					"the refusal would send an operator to a ring that still under-covers, or past one that "+
					"already worked", got, tc.pool, want)
			}
			if err := ringCoversPool(want, tc.share, tc.pool); err != nil {
				t.Errorf("ringCoversPool(%d, %v, %d) = %v, want nil: %d is where the draw covers the pool",
					want, tc.share, tc.pool, err, want)
			}
			if err := ringCoversPool(want-1, tc.share, tc.pool); err == nil {
				t.Errorf("ringCoversPool(%d, %v, %d) = nil, want a refusal: one index narrower draws %d "+
					"numbers, short of the pool", want-1, tc.share, tc.pool,
					portedReach(want-1, portedPerBlock(tc.share)))
			}
		})
	}
}

// TestRingCoversPoolRefusesABlockOverflow: the two halves share the +2250700xxxxxx block, so a spread
// that runs past its end wraps into the ported set.
func TestRingCoversPoolRefusesABlockOverflow(t *testing.T) {
	err := ringCoversPool(600000, 0.3, 600000)
	if err == nil {
		t.Fatal("ringCoversPool(ring=600000, pool=600000) = nil: 1 200 000 numbers do not fit in a block " +
			"of 1 000 000, so the non-ported spread wraps onto seeded numbers")
	}
	if !strings.Contains(err.Error(), "fixture block holds") {
		t.Errorf("the refusal is %q, want the block-overflow branch: a caller told to raise the ring would "+
			"make this worse", err)
	}
}

// TestRingCoversPoolIgnoresThePoolWhenNothingIsPorted pins the default run. share=0 seeds nothing, so
// no pool has to be covered and the narrow default ring is legitimate.
func TestRingCoversPoolIgnoresThePoolWhenNothingIsPorted(t *testing.T) {
	if err := ringCoversPool(4096, 0, 100000); err != nil {
		t.Errorf("ringCoversPool(4096, share=0, pool=100000) = %v, want nil: at share=0 the seed writes "+
			"nothing, so there is no working set for the ring to cover", err)
	}
}

// TestRingCoversPoolRefusesAnEmptyRing: zero divides by zero in the injector's at(), and a panic is a
// stack trace where a refusal is a sentence.
//
// It asks at share=0, and only there. At any drawable share the coverage branch refuses an empty ring
// on its own, so a case with ported traffic would pass with this guard deleted — green on the strength
// of the branch below it. share=0 is the DEFAULT of the reference run, and the one shape in which
// nothing else stands between REF_DEST_RING=0 and the panic.
func TestRingCoversPoolRefusesAnEmptyRing(t *testing.T) {
	err := ringCoversPool(0, 0, 10)
	if err == nil {
		t.Fatal("ringCoversPool(ring=0, share=0) = nil, want a refusal: the injector's at() takes seq " +
			"modulo the ring, so zero is an integer divide by zero at the first submission")
	}
	if !strings.Contains(err.Error(), "REF_DEST_RING") {
		t.Errorf("the refusal is %q, want it to name the lever an operator has to change", err)
	}
}

// portedInWindow is how many of a window's resolutions reach the store, given the ring the injector
// actually samples.
//
// It is NOT share x messages, and the difference is a property of the harness rather than a rounding.
// l0Dest places the ported draws at the HEAD of each block of portedShareDen, and the injector samples
// Dest over [0, ring) — so a ring that is not a whole number of blocks is truncated in excess. At the
// default 4 096 and a share of 0.3 the traffic is 1296/4096 = 31.6 % ported, not 30 %: a 5.5 % bias, half
// of maxMixGap eaten by an artefact of the ring, and a journal line that publishes the share it was
// asked for rather than the one it drew.
func portedInWindow(messages uint64, ring int, share float64) uint64 {
	num := portedPerBlock(share)
	if num <= 0 || ring < 1 {
		return 0
	}
	return messages * uint64(portedReach(ring, num)) / uint64(ring)
}

func TestPortedInWindowFollowsTheRingAndNotTheShare(t *testing.T) {
	// The default shape: 4 096 is four whole blocks plus 96 indices, and the 96 are all ported at a
	// share of 0.3 because the ported draws lead each block.
	if got, want := portedInWindow(4096, 4096, 0.3), uint64(1296); got != want {
		t.Errorf("portedInWindow(4096 messages, ring 4096, share 0.3) = %d, want %d: share x messages "+
			"would say 1228, and the 5.5%% gap is the ring's truncation, not rounding", got, want)
	}
	// A ring that is a whole number of blocks carries exactly the share.
	if got, want := portedInWindow(10000, 1000, 0.3), uint64(3000); got != want {
		t.Errorf("portedInWindow over a whole block = %d, want %d", got, want)
	}
	// The pathological ring the guard admits: shorter than one block, every index ported.
	if got, want := portedInWindow(1000, 300, 0.3), uint64(1000); got != want {
		t.Errorf("portedInWindow(ring 300, share 0.3) = %d, want %d: the first 300 indices of a block are "+
			"ALL ported, so the run is 100%% ported while the share says 30%%", got, want)
	}
	if got := portedInWindow(1000, 4096, 0); got != 0 {
		t.Errorf("portedInWindow at share=0 = %d, want 0", got)
	}
}

// TestMixCarriesItsShareAcceptsAWarmWindow is the defect this split exists for.
//
// The full-stack reference run holds a 20 s warmup, never FLUSHDBs, and cycles its ring several times
// before the window opens — so by then every ported number is cached, pg_hit is ~0 and redis_hit carries
// the whole ported share. That is a LEGITIMATE run: it is what production steady state looks like.
// mixHolds refuses it, because its model is written for a cache flushed before the palier, and the
// message it prints accuses the harness of not flushing.
func TestMixCarriesItsShareAcceptsAWarmWindow(t *testing.T) {
	const (
		messages = 72000
		ring     = 4096
		share    = 0.3
		pool     = 1000
	)
	ported := portedInWindow(messages, ring, share)
	warm := map[string]uint64{"bloom_miss": messages - ported, "redis_hit": ported}

	if err := mixCarriesItsShare(warm, messages, ported); err != nil {
		t.Errorf("a warm window is refused: %v", err)
	}
	if err := mixHolds(warm, messages, share, pool); err == nil {
		t.Error("mixHolds accepted a warm window: if it ever does, this split has stopped being needed " +
			"and the reference run can go back to the full model")
	}
}

// TestMixCarriesItsShareStillCatchesWhatMattersOnAWarmWindow: dropping the cold split must not drop the
// four failures that do not depend on cache warmth.
func TestMixCarriesItsShareStillCatchesWhatMattersOnAWarmWindow(t *testing.T) {
	const messages, ported = uint64(1000), uint64(300)
	sound := map[string]uint64{"bloom_miss": 700, "redis_hit": 300}

	if err := mixCarriesItsShare(sound, messages, ported); err != nil {
		t.Fatalf("the sound mix is refused: %v", err)
	}
	for name, tc := range map[string]struct {
		counts map[string]uint64
		want   string
	}{
		"a failed lookup": {
			counts: map[string]uint64{"bloom_miss": 700, "redis_hit": 299, "pg_error": 1},
			want:   "pg_error",
		},
		"an outcome the vocabulary does not hold": {
			counts: map[string]uint64{"bloom_miss": 700, "redis_hit": 300, "pg_negative_hit": 40},
			want:   "pg_negative_hit",
		},
		"fewer observations than resolutions": {
			counts: map[string]uint64{"bloom_miss": 200, "redis_hit": 300},
			want:   "observations",
		},
		"the store was never reached": {
			counts: map[string]uint64{"bloom_miss": 1000},
			want:   "300",
		},
		"the store carried more than the share": {
			counts: map[string]uint64{"bloom_miss": 400, "redis_hit": 600},
			want:   "600",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := mixCarriesItsShare(tc.counts, messages, ported)
			if err == nil {
				t.Fatalf("accepted %v", tc.counts)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal is %q, want it to name %q", err, tc.want)
			}
		})
	}
}

// TestMixCarriesItsShareRunsAtShareZero: the share=0 palier is the DEFAULT of the reference run, and it
// still has three things worth checking — no failed lookup, no unknown outcome, one observation per
// resolution. mixHolds returned nil before reaching any of them, so the run gated its call on share > 0
// and checked nothing at all on the shape it actually publishes.
func TestMixCarriesItsShareRunsAtShareZero(t *testing.T) {
	if err := mixCarriesItsShare(map[string]uint64{"bloom_miss": 1000}, 1000, 0); err != nil {
		t.Errorf("the share=0 mix is refused: %v", err)
	}
	err := mixCarriesItsShare(map[string]uint64{"bloom_miss": 999, "redis_error": 1}, 1000, 0)
	if err == nil {
		t.Error("a failed lookup at share=0 is accepted: the Bloom gate palier is the one every published " +
			"line of this run rests on")
	}
}
