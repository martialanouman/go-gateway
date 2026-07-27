package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestCommittablePrefix covers the per-partition commit-watermark logic that RunBatch relies on for
// at-least-once with concurrent, out-of-order batch handling.
func TestCommittablePrefix(t *testing.T) {
	rec := func(part int32, off int64) *kgo.Record {
		return &kgo.Record{Topic: "mt.routed", Partition: part, Offset: off}
	}
	fail := errors.New("boom")
	key := func(kr *kgo.Record) string { return fmt.Sprintf("%d:%d", kr.Partition, kr.Offset) }
	offsets := func(krs []*kgo.Record) []string {
		s := make([]string, 0, len(krs))
		for _, kr := range krs {
			s = append(s, key(kr))
		}
		return s
	}

	t.Run("all handled commits everything", func(t *testing.T) {
		krs := []*kgo.Record{rec(0, 10), rec(0, 11), rec(1, 5)}
		got := committablePrefix(krs, []error{nil, nil, nil})
		if len(got) != 3 {
			t.Errorf("committed %v, want all 3", offsets(got))
		}
	})

	t.Run("stops a partition at its first failure but not siblings", func(t *testing.T) {
		// p0: 10 ok, 11 FAIL, 12 ok(skipped). p1: 5 ok, 6 ok — a p0 failure must not hold p1 back.
		krs := []*kgo.Record{rec(0, 10), rec(0, 11), rec(0, 12), rec(1, 5), rec(1, 6)}
		got := committablePrefix(krs, []error{nil, fail, nil, nil, nil})
		want := map[string]bool{"0:10": true, "1:5": true, "1:6": true}
		if len(got) != len(want) {
			t.Fatalf("committed %v, want %v", offsets(got), want)
		}
		for _, kr := range got {
			if !want[key(kr)] {
				t.Errorf("committed %s, which should have been held back", key(kr))
			}
		}
	})

	t.Run("never commits a success after an earlier failure in the same partition", func(t *testing.T) {
		// The gap invariant: 12 succeeded but 11 failed → 12 must NOT commit (would skip 11).
		krs := []*kgo.Record{rec(0, 11), rec(0, 12)}
		got := committablePrefix(krs, []error{fail, nil})
		if len(got) != 0 {
			t.Errorf("committed %v, want nothing (11 failed, 12 is past the gap)", offsets(got))
		}
	})
}
