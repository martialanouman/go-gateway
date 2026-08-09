package e2e_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// minPrefillShare is the fraction of the mean below which a partition is treated as starved.
//
// It is deliberately generous: the point is not to police the hash's uniformity but to catch the lane
// that will run dry mid-window. franz-go's default partitioner spreads a handful of keys unevenly by
// construction, and a tight band would fail runs that measure perfectly well.
const minPrefillShare = 0.5

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
