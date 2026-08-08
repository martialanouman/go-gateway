package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TestSplitLagRefusesATopicWithNoPartitions is the sibling of the partial-total guard, and it exists for
// the same reason: a backlog gauge that reads "0" when it means "I could not tell" is the one failure a
// backlog gauge must never have.
//
// kadm seeds its lag map from the topics in the members' Join, then leaves a topic's partition map EMPTY
// when that topic is missing from endOffsets — a shard error on ListEndOffsets, or a topic that does not
// exist. Reachable in practice: a deploy that introduces mt.outcome with broker auto-creation disabled,
// or a missing ACL. Summing an empty map yields 0 and publishes "we are caught up" while the projection
// is in fact consuming nothing at all (step-201c, D20).
func TestSplitLagRefusesATopicWithNoPartitions(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{"mt.outcome": {}}

	if _, err := splitLag("router-svc-outcome-cdr", lag); err == nil {
		t.Fatal("splitLag reported a lag for a topic with no partitions at all: the gauge would publish 0, " +
			"which reads as 'caught up' precisely when nothing is being consumed")
	}
}

// TestSplitLagStillRefusesAPartitionError pins the guard that already existed, so the new one cannot be
// mistaken for a replacement.
func TestSplitLagStillRefusesAPartitionError(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{"mt.outcome": {
		0: {Partition: 0, Lag: 12},
		1: {Partition: 1, Lag: -1, Err: errors.New("no offset")},
	}}

	if _, err := splitLag("router-svc-outcome-cdr", lag); err == nil {
		t.Fatal("splitLag reported a total while one partition could not be computed")
	}
}

// TestSumLagAddsThePartitionsOfEachTopic is the happy path: negative lags (a partition with no committed
// offset yet) contribute nothing rather than subtracting from their neighbours.
func TestSumLagAddsThePartitionsOfEachTopic(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{
		"mt.outcome": {0: {Partition: 0, Lag: 10}, 1: {Partition: 1, Lag: 32}},
		"mt.inbound": {0: {Partition: 0, Lag: -1}, 1: {Partition: 1, Lag: 5}},
	}

	split, err := splitLag("router-svc", lag)
	if err != nil {
		t.Fatalf("splitLag: %v", err)
	}
	got := sumLag(split)
	for topic, want := range map[string]int64{"mt.outcome": 42, "mt.inbound": 5} {
		if got[topic] != want {
			t.Errorf("lag[%q] = %d, want %d", topic, got[topic], want)
		}
	}
}

// TestSplitLagKeepsEachPartitionApart: the split is the whole point of the function. A group whose keys
// all hash to one partition is serialised however many partitions the topic has, and the TOTAL cannot
// tell that apart from a balanced group — so a measurement that concludes anything about parallelism has
// to read this (step-201d, D5).
func TestSplitLagKeepsEachPartitionApart(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{"mt.inbound": {
		0: {Partition: 0, Lag: 22403},
		1: {Partition: 1, Lag: 0},
		2: {Partition: 2, Lag: 0},
		3: {Partition: 3, Lag: -1}, // never committed: clamped, never subtracted
	}}

	got, err := splitLag("loadref-router", lag)
	if err != nil {
		t.Fatalf("splitLag: %v", err)
	}
	want := map[int32]int64{0: 22403, 1: 0, 2: 0, 3: 0}
	for partition, w := range want {
		if got["mt.inbound"][partition] != w {
			t.Errorf("lag[mt.inbound][%d] = %d, want %d", partition, got["mt.inbound"][partition], w)
		}
	}
	if n := len(got["mt.inbound"]); n != len(want) {
		t.Errorf("mt.inbound reported %d partitions, want %d — a dropped partition is a backlog nobody sees", n, len(want))
	}
}
