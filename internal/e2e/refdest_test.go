package e2e_test

import (
	"fmt"
	"strings"
	"testing"
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
	// The block check comes FIRST, and the order is the finding rather than a preference: on a
	// configuration that both overflows and under-covers, the coverage branch's remedy — a wider ring —
	// makes the overflow worse. A guard whose advice deepens the other failure has to yield to it.
	if pool+ring > destBlockSize {
		return fmt.Errorf("REF_PORTED_POOL=%d and REF_DEST_RING=%d need %d numbers side by side, past the "+
			"%d the +2250700xxxxxx fixture block holds: the non-ported spread would wrap into the ported "+
			"set and every wrapped message would be an L0 hit the mix guard does not expect",
			pool, ring, pool+ring, destBlockSize)
	}
	if num := portedPerBlock(share); num > 0 && ring < minRingFor(num, pool) {
		return fmt.Errorf("REF_DEST_RING=%d draws %d distinct ported numbers at REF_PORTED_SHARE=%v, "+
			"short of the REF_PORTED_POOL=%d the seed writes: the run would touch a fraction of the "+
			"working set, read a 100%% redis_hit mix that mixHolds accepts as a legitimate warm bound, "+
			"and publish a Postgres read rate of nil. Raise the ring to at least %d",
			ring, portedReach(ring, num), share, pool, minRingFor(num, pool))
	}
	return nil
}

// portedReach is how many DISTINCT ported numbers a ring of `ring` indices draws when l0Dest takes num
// of every portedShareDen. The ordinals it produces are consecutive from zero, so the count is the
// answer and no set has to be built.
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
// There is deliberately no pool < 1 guard. It would be dead code: below one the expression falls under
// any ring the caller can still be holding — ring < 1 is refused before this is reached — so the
// coverage branch cannot fire either way, and a pool of zero with a drawable share is refused by
// portedSet at seed time, in the words of the lever an operator actually set.
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
				got := refDest(i, 2048, share, pool)
				digits, ok := strings.CutPrefix(got, "+")
				if !ok {
					t.Fatalf("refDest(%d, share=%v, pool=%d) = %q, want the leading plus the REST ingress takes",
						i, share, pool, got)
				}
				if !canonicalMSISDN.MatchString(digits) {
					t.Fatalf("refDest(%d, share=%v, pool=%d) = %q: %q is not the canonical form exact_routes "+
						"accepts", i, share, pool, got, digits)
				}
			}
		}
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
