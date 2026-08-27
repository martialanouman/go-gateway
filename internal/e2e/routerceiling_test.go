//go:build loadref

package e2e_test

import (
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// envPrefill is how many mt.inbound records are written ahead of each palier.
//
// It is not a target, it is a floor with a guard behind it: the window must close before the backlog
// does, and backlogHeld fails the palier when it did not. The default holds ~15 000 msg/s for a
// ten-second window — about three times the full-stack ceiling of step-201d PR2.
const envPrefill = "REF_PREFILL"

// TestRouterConsumeCeiling isolates the router from everything that shared its host, because the
// 08/08/2026 run at 4 800 msg/s could say the router slowed down and never why — the same reason
// TestCDRWriteCeiling and TestRoutedProduceCeiling exist beside it, and it is built to their shape.
//
// It is a diagnostic, not a gate: it asserts only that the path moves and that the backlog it was given
// was never exhausted, and reports the curve.
//
// # The question
//
// step-201d PR2 measured the router at 1 692 / 2 990 / 3 422 / 4 702 msg/s over 1 / 2 / 4 / 8 lanes,
// with an injector, a REST API, a connector pool of 4 to 16 SMPP binds and a fake SMSC on the same host.
// Widening the pool from 4 to 16 binds — nobody touched the router — dropped it from 4 702 to 3 395/s.
// Here there is NO injector, NO REST, NO pool and NO peer: the topic is prefilled before the clock
// starts, and the only thing running is RunBatch -> Pipeline.Process -> Produce.
//
//	the curve lands on the PR2 curve, 8 lanes  -> co-residency was not the subject. The ceiling belongs
//	near 4 700/s                                  to the router or to the broker, 4 702/s keeps its
//	                                              meaning, and step-270 provisions on it.
//	the curve sits FRANKLY above it            -> the PR2 ceiling WAS contention. 4 702/s is a property
//	                                              of one laptop hosting nine components and three
//	                                              containers, not of the router, and the README figure
//	                                              must be annotated rather than carried into step-280.
//	1 lane alone already beats the full-stack  -> nothing measured in PR2 was about the router at all,
//	8-lane figure                                 and the lane sweep of step-201d D11 priced a
//	                                              contention rather than a fan-out.
//	the curve bends before 8 lanes, then flat  -> lanes stop buying throughput before the host runs out.
//	                                              The cost is per record and serialised elsewhere. PR1
//	                                              can only reach that by subtraction; the produce
//	                                              histogram of step-201e D3 observes it, and the
//	                                              broker's own latency (D2) says whether it is Kafka.
//	the curve FALLS as lanes rise              -> the fan-out itself is the cost, and the shard-by-account
//	                                              suite of step-201d D11 becomes a real question with its
//	                                              own ADR rather than a possible one.
//
// # The second question, added by step-201e D3: where the per-lane rendering goes
//
// PR1 measured 5 842 msg/s at one lane and 27 856 at sixteen — but that is 1 741/s PER LANE, a 70%
// collapse. The fan-out still buys throughput; it buys less and less of it. Since a message's cost has
// to be paid somewhere, the produce histogram below says whether it is paid in the synchronous acks=all
// produce or outside it. The answer is what step-270 provisions on.
//
//	the mean produce climbs with the lane count,  -> the cost is IN the produce: the broker serialises
//	and its share of the budget grows               what the lanes parallelise, and more partitions per
//	                                                pod buy more than more pods.
//	the mean stays flat while the rendering falls -> the cost is elsewhere — poll, decode, offset commit
//	                                                — and D3 has cleared a suspect without naming a
//	                                                replacement. Say exactly that; do not conclude.
//	the mean climbs but stays a small share of    -> the produce got slower and it still is not what
//	the budget                                       bounds the palier. Both facts are worth the line.
//
// The histogram lives in the harness, never in internal/router: step-201d D7 dropped a production-side
// histogram because the pipeline is 2.3% of the budget, and nothing has changed. router.Producer being a
// one-method interface declared consumer-side is what makes the decorator free.
//
// # What the curve is drawn against, and what would silently make it a lie
//
// Three facts are read BEFORE the rates, and the palier fails rather than prints if any breaks:
//
//   - The prefill must have reached every partition. prefillBalance reads the end offsets rather than
//     trusting the partitioner: kafka.NewProducer configures none, so franz-go's default decides, and a
//     version bump could move it without a test going red.
//   - The backlog must still be there when the window closes. A window that drained measures the end of
//     a queue, not a ceiling — and it reads as a perfectly plausible rate. backlogHeld refuses per
//     partition, never on the total: one lane running dry understates the palier while the total still
//     looks healthy.
//   - The lanes must exist. handleBatch opens one goroutine per partition PRESENT IN THE BATCH, and the
//     batch is bounded by FetchMaxPartitionBytes (56 KiB, ADR-0012), not by the topic. laneShape reports
//     the observed mean and says when it fell short.
//
// The clock starts at the first observed produce, not at Run: a fresh group takes hundreds of
// milliseconds to a few seconds to join, and over a ten-second window divided by the REAL elapsed time
// that is 10-30% of understatement, worst at the small paliers — a distortion of the slope, which is
// exactly what this curve is read for. Neither ceiling test beside this one has the problem: neither
// has a consumer group.
//
// # Why the sweep uses private topics
//
// KAFKATEST_PARTITIONS is read once with the container, so a sweep through it is one `go test` per
// point, on a different broker each time; and it widens mt.routed along with mt.inbound, so the curve
// would vary the input AND the output (test/load/README.md, step-201d PR2). A private topic per palier
// varies the router's lane count and nothing else. mt.routed stays at whatever the shared broker was
// created with, on every row.
//
// # What is stubbed, and what that costs
//
// The resolver, sender-ID, opt-out and anti-spam stages are stubs, and RateLimiter/Credit are nil, as
// in the reference run this curve is compared against (test/load/README.md, step-201d D4). The cost is
// bounded by a figure already measured: Pipeline.Process is 2.3% of the per-message budget, and those
// four stages are ~10% of the pipeline (step-201d D8) — under 0.3% of the budget. e164 normalisation
// and segmentation, which are 74% of the pipeline, run for real.
//
// The record is the one the REST API and the SMPP server publish: encoded by pipeline.EncodeInbound,
// keyed by account id, a GSM-7 body of the length the injector sends. Only the topic is overridden.
func TestRouterConsumeCeiling(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	hold := envDuration(t, envCalHold, 10*time.Second)
	records := int(envFloat(t, envPrefill, 150000))

	for _, partitions := range []int{1, 2, 4, 8, 16} {
		measureRouterCeiling(t, brokers, partitions, records, hold)
	}
}
