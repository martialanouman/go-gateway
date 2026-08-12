//go:build loadref

package e2e_test

import (
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// sweepAWindow is the SMPP window sweep A holds fixed: the reference run's own default, so sweep A's
// bind curve is drawn in the conditions the 2 400/s figure was measured under.
const sweepAWindow = 64

// TestPoolSubmitCeiling measures what the connector pool sustains ALONE, which is the one figure the
// throughput story has never had.
//
// After step-201e the pipeline reads: ingestion ≥ 2 400/s, router 20 741/s isolated, pool→SMSC 2 400/s
// — and that last number is the only one measured with nine components sharing a laptop. The router
// APPEARED to cap at 4 702/s in the same conditions and does 20 741/s alone: a factor of 4,4 that
// belonged to the host. The pool is in exactly that position, accused on a figure measured in noise.
//
// The target is 10 400 submit_sm/s (8 000 SMS/s × 1,3 segments). The gap is ×4,3 and it is entirely in
// the outbound leg, so nothing can be sized for step-207 or claimed in step-201b until it is attributed.
//
// # What each outcome means
//
// The falsifiable question is whether the pool caps near 2 400/s once it is alone.
//
//   - It caps there again → the ceiling is the pool's, and the lever is in the bind or the SMPP window.
//     Sweep B says which: a curve flat in binds and steep in window is a window bound, the reverse is a
//     bind bound, flat in both is neither and the cost is per-message work.
//   - It climbs sharply → the 2 400/s were co-residency, like the router's 4 702/s, and the README
//     figure must be annotated rather than carried into step-201b as a component capacity.
//   - It climbs to roughly the peer's calibrated rate → the bench measured the FAKE SMSC, not the pool.
//     That is why every row carries the peer's own ceiling at the same bind count: a palier whose rate
//     approaches its peer figure is a reading of the peer wearing the pool's name.
//
// # Two levers, because one would misattribute
//
// bind_pool_size and the SMPP window are not interchangeable. Binds are TCP connections and shards of
// the fan-out; the window is how many submit_sm ride each bind unacknowledged. A one-dimensional sweep
// would credit one for what the other did — a pool at 16 binds and window 1 is sixteen synchronous
// round-trips, and its curve would read as a bind ceiling when it is a window ceiling.
//
// The two sweeps CROSS at 8 binds / window 64, deliberately. That palier is measured twice, and if the
// two readings disagree neither sweep is readable — the same argument crossCheck makes within a palier,
// applied between them.
//
// # What this bench does NOT answer
//
// Every figure here is an UPPER bound on production: DLRMap is nil, so no palier pays the Redis
// correlation write production performs on every acknowledged submit_sm. The reference run made the
// same omission silently, which is what makes its 2 400/s comparable to these rows and neither of them
// a production capacity. Measuring that delta is the next step, and until it lands no number from this
// file may be quoted as a sizing.
func TestPoolSubmitCeiling(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	hold := envDuration(t, envCalHold, 10*time.Second)
	records := int(envFloat(t, envPrefill, 150000))

	// Sweep B's bind count. REF_BIND_POOL moves it, so a knee that lands away from 8 in sweep A can be
	// re-swept without editing this file.
	atBinds := int(envFloat(t, envBindPool, 8))

	// Sweep A — the bind count, at the reference run's window.
	var crossingA float64
	for _, binds := range []int{1, 2, 4, 8, 16} {
		rate := measurePoolCeiling(t, brokers, binds, sweepAWindow, records, hold)
		if binds == atBinds {
			crossingA = rate
		}
	}

	// Sweep B — the SMPP window, at that fixed bind count.
	var crossingB float64
	for _, window := range []int{1, 10, 64, 256} {
		rate := measurePoolCeiling(t, brokers, atBinds, window, records, hold)
		if window == sweepAWindow {
			crossingB = rate
		}
	}

	// The crossing is the only place host drift between the two sweeps becomes visible: every other
	// guard compares two readings of the same window and would pass through it untouched.
	if err := sweepsAgree(crossingA, crossingB); err != nil {
		t.Errorf("%d binds w%d measured in both sweeps: %v", atBinds, sweepAWindow, err)
	}
}
