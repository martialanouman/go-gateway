//go:build loadref

package e2e_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

const (
	// envFidelityPairs is how many with/without couples the palier runs.
	//
	// Three is the floor a verdict can be read from and the ceiling a laptop finishes in one sitting:
	// six windows plus six peer calibrations plus a 1,5 M record prefill. fidelityDelta refuses fewer
	// than two outright — one pair bounds no spread, so any delta would clear a noise estimate of zero.
	envFidelityPairs = "REF_FIDELITY_PAIRS"

	// poolFidelityBinds is the configuration the pairs are measured at: the one step-201f PR1 named as
	// the sizing floor (8 binds pass the 10 400 submit_sm/s NFR target in all three runs, 4 do not). The
	// cost of the DLR write is worth knowing where step-207 will actually operate, not at the ends of a
	// curve this host could not draw.
	poolFidelityBinds = 8

	// poolFidelityPairs is envFidelityPairs' default.
	poolFidelityPairs = 3
)

// TestPoolDLRMapFidelity puts a figure on the one omission that made every number in step-201f PR1 an
// upper bound rather than a capacity.
//
// PR1 measured the pool with DLRMap nil — which is what the reference run does too, and what makes the
// two comparable. Production is not either of them: recordDLRMapping writes one Redis entry per
// ACKNOWLEDGED submit_sm, synchronously, on the send path, before the settle and before the outcome
// publish (§1.11). Every rate PR1 published belongs to a pool that skipped it. This palier wires the
// real store, at the one configuration PR1 could defend, and reads the delta.
//
// # Why the pairs are interleaved, and why the order alternates
//
// The obvious shape — three windows without the store, then three with — is the shape that would have
// produced a wrong number on this host. PR1 measured ONE configuration twice inside a single run and
// read 12 573/s then 14 972/s: 19% apart, on a lever the same run had shown inert, with four
// equivalent paliers spreading 30%. A block of three followed by a block of three would have collected
// that drift and handed it back as the cost of Redis.
//
// Interleaving puts the two members of a pair minutes apart instead of half an hour. Alternating which
// member goes first is the second half: a host that slows monotonically through the run would otherwise
// penalise whichever side always ran second, and the penalty would be systematic rather than noisy —
// which is exactly the kind of error an average does not remove.
//
// fidelityDelta then refuses to name a cost smaller than the spread of the readings it is drawn from.
// On this host that refusal is the likely outcome, and it is a finding: it says the DLR write costs
// less than what the host's own variance can hide, which is a bound step-201b can carry.
//
// # What the guard behind it is for
//
// A palier can hold a real store, dial it, and never call it: recordDLRMapping returns early on a
// non-ROK submit_sm_resp and on a response with no smsc message id. The two sides would then be the
// same configuration and their delta would be pure noise, with nothing in the rate, the breaker or the
// cross-check to say so. putsMatchSubmits inside measurePoolCeiling is what refuses it, and it is the
// reason countingDLRMap counts at all rather than only timing.
//
// # What this palier is NOT
//
// It is not the fiche's D2. Chronométrer le submit_sm in situ has no seam: bind.Submit is concrete,
// unexported and reached from one call site inside the send path, so timing it would mean adding an
// interface to connectorpool.Deps — a change to the production hot path the fiche forbids twice. What
// the run reports instead is a decomposition: the per-message budget, the produce's measured share of
// it, and the DLR write's, leaving a residual that CONTAINS the submit_sm round trip. A ceiling on it
// is not a measurement of it, and the README says so rather than the difference being quoted as one.
func TestPoolDLRMapFidelity(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	rdb := redistest.Client(t)

	hold := envDuration(t, envCalHold, poolCeilingHold)
	records := int(envFloat(t, envPrefill, poolCeilingPrefill))
	pairs := int(envFloat(t, envFidelityPairs, poolFidelityPairs))
	binds := int(envFloat(t, envBindPool, poolFidelityBinds))

	// One bed for every window, as in the sweep: each palier joins with a fresh group and re-reads from
	// offset zero, so an identical fixture sits under both members of every pair — which is the whole
	// point of pairing them.
	bed := newPoolBed(t, brokers, records, binds)
	store := dlrmap.NewRedisMap(rdb)

	without := make([]float64, 0, pairs)
	with := make([]float64, 0, pairs)
	for i := range pairs {
		run := func(dlr *countingDLRMap) float64 {
			return measurePoolCeiling(t, brokers, bed, binds, sweepAWindow, hold, dlr)
		}
		// Alternate, so neither side is always the one that ran second.
		if i%2 == 0 {
			without = append(without, run(nil))
			with = append(with, run(freshStore(t, rdb, store)))
			continue
		}
		with = append(with, run(freshStore(t, rdb, store)))
		without = append(without, run(nil))
	}

	verdict, err := fidelityDelta(without, with)
	if err != nil {
		t.Fatalf("%d binds w%d, %d pairs: %v", binds, sweepAWindow, pairs, err)
	}
	t.Logf("dlr fidelity: %d binds w%d · %s", binds, sweepAWindow, verdict)
}

// freshStore empties the DLR store, then wraps it in the counter one palier reads.
//
// Emptying it is not tidiness, it is the pairing. The bench's routed records carry no validity_period,
// so ttlForValidity falls back to maxTTL and every entry lives 72 HOURS: a palier writes one key per
// acknowledged submit_sm — some 370 000 over a thirty second window — and nothing expires inside a run.
// Without this, the third stored palier measures a Redis holding a million keys the first one never saw,
// which is exactly the thing pairing exists to rule out: an identical fixture under both members.
//
// It costs nothing the measurement can see. The flush runs before the pool is built, so it lands outside
// the window and outside the warmup, and its own duration is never divided by anything.
//
// It empties the WHOLE database, and redistest.Client hands out a container shared across the package.
// That is safe here because Go runs a package's tests sequentially unless they ask otherwise, and this
// one lives behind the loadref tag where nothing else runs alongside it. A test in this package that
// took t.Parallel() and a Redis key would break on it, loudly.
func freshStore(t *testing.T, rdb *redis.Client, store connectorpool.DLRMap) *countingDLRMap {
	t.Helper()
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("emptying the DLR store between paliers: %v", err)
	}
	return newCountingDLRMap(store)
}
