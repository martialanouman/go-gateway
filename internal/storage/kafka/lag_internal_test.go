package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TestSumLagRefusesATopicWithNoPartitions is the sibling of the partial-total guard, and it exists for
// the same reason: a backlog gauge that reads "0" when it means "I could not tell" is the one failure a
// backlog gauge must never have.
//
// kadm seeds its lag map from the topics in the members' Join, then leaves a topic's partition map EMPTY
// when that topic is missing from endOffsets — a shard error on ListEndOffsets, or a topic that does not
// exist. Reachable in practice: a deploy that introduces mt.outcome with broker auto-creation disabled,
// or a missing ACL. Summing an empty map yields 0 and publishes "we are caught up" while the projection
// is in fact consuming nothing at all (step-201c, D20).
func TestSumLagRefusesATopicWithNoPartitions(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{"mt.outcome": {}}

	if _, err := sumLag("router-svc-outcome-cdr", lag); err == nil {
		t.Fatal("sumLag reported a lag for a topic with no partitions at all: the gauge would publish 0, " +
			"which reads as 'caught up' precisely when nothing is being consumed")
	}
}

// TestSumLagStillRefusesAPartitionError pins the guard that already existed, so the new one cannot be
// mistaken for a replacement.
func TestSumLagStillRefusesAPartitionError(t *testing.T) {
	t.Parallel()

	lag := kadm.GroupLag{"mt.outcome": {
		0: {Partition: 0, Lag: 12},
		1: {Partition: 1, Lag: -1, Err: errors.New("no offset")},
	}}

	if _, err := sumLag("router-svc-outcome-cdr", lag); err == nil {
		t.Fatal("sumLag reported a total while one partition could not be computed")
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

	got, err := sumLag("router-svc", lag)
	if err != nil {
		t.Fatalf("sumLag: %v", err)
	}
	for topic, want := range map[string]int64{"mt.outcome": 42, "mt.inbound": 5} {
		if got[topic] != want {
			t.Errorf("lag[%q] = %d, want %d", topic, got[topic], want)
		}
	}
}
