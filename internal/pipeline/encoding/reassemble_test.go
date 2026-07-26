package encoding_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// offer is a tiny helper so each test reads as a sequence of segment arrivals.
type offerFn func(seq, total int, content string) (string, bool)

func newReassembler(t *testing.T, ttl time.Duration, clock func() time.Time) (*encoding.Reassembler, offerFn) {
	t.Helper()
	rdb := redistest.Client(t)
	opts := []encoding.ReassemblerOption{}
	if clock != nil {
		opts = append(opts, encoding.WithReassemblyClock(clock))
	}
	r := encoding.NewReassembler(rdb, ttl, opts...)
	// A distinct group per test instance so the shared Redis container never crosses two tests.
	from, to, connector, ref := "22600000001", "3615", uuid.New(), uint16(42)
	do := func(seq, total int, content string) (string, bool) {
		t.Helper()
		body, complete, err := r.Offer(context.Background(), from, to, connector, ref, total, seq, []byte(content))
		if err != nil {
			t.Fatalf("offer seq %d: %v", seq, err)
		}
		return string(body), complete
	}
	return r, do
}

// TestReassembleInOrder: two segments arriving in order complete into the concatenated body.
func TestReassembleInOrder(t *testing.T) {
	_, offer := newReassembler(t, time.Minute, nil)

	if body, complete := offer(1, 2, "Hello, "); complete {
		t.Fatalf("segment 1 of 2 should not complete the message, got %q", body)
	}
	body, complete := offer(2, 2, "concatenated world")
	if !complete {
		t.Fatal("segment 2 of 2 should complete the message")
	}
	if body != "Hello, concatenated world" {
		t.Errorf("assembled body = %q, want the two segments joined in order", body)
	}
}

// TestReassembleOutOfOrder: segments arriving out of order still assemble in sequence order — the SMSC
// gives no ordering guarantee, so reassembly must key on the sequence number, not arrival.
func TestReassembleOutOfOrder(t *testing.T) {
	_, offer := newReassembler(t, time.Minute, nil)

	// Three segments, delivered 3, 1, 2.
	if _, complete := offer(3, 3, "!"); complete {
		t.Fatal("first arrival (seq 3) must not complete")
	}
	if _, complete := offer(1, 3, "one "); complete {
		t.Fatal("second arrival (seq 1) must not complete")
	}
	body, complete := offer(2, 3, "two ")
	if !complete {
		t.Fatal("the third arrival completes the group")
	}
	if body != "one two !" {
		t.Errorf("assembled body = %q, want the segments joined by sequence order", body)
	}
}

// TestReassembleMissingSegmentNeverCompletes: a group with a permanently missing segment never emits.
func TestReassembleMissingSegmentNeverCompletes(t *testing.T) {
	_, offer := newReassembler(t, time.Minute, nil)

	if _, complete := offer(1, 3, "a"); complete {
		t.Fatal("1/3 must not complete")
	}
	if _, complete := offer(3, 3, "c"); complete {
		t.Fatal("2 of 3 present (seq 2 missing) must not complete")
	}
}

// TestReassembleRedeliveredSegmentIsIdempotent: a segment redelivered (SMSC retransmit, at-least-once)
// must not count twice — the group completes only when DISTINCT sequences are all present.
func TestReassembleRedeliveredSegmentIsIdempotent(t *testing.T) {
	_, offer := newReassembler(t, time.Minute, nil)

	offer(1, 2, "first")
	if _, complete := offer(1, 2, "first"); complete {
		t.Fatal("a redelivered segment 1 must not complete a 2-part message on its own")
	}
	if _, complete := offer(2, 2, "second"); !complete {
		t.Fatal("the message completes once segment 2 arrives")
	}
}

// TestReassembleEvictsStaleSegments: a segment older than the TTL is evicted, so a later segment cannot
// complete a group whose earlier part has already timed out. The clock is injected, so the test drives
// eviction deterministically without sleeping.
func TestReassembleEvictsStaleSegments(t *testing.T) {
	clock := time.Now()
	_, offer := newReassembler(t, time.Minute, func() time.Time { return clock })

	if _, complete := offer(1, 2, "early"); complete {
		t.Fatal("1/2 must not complete")
	}
	// Advance past the TTL, then deliver segment 2: segment 1 is now stale and evicted, so the group is
	// still incomplete (only a fresh segment 2 remains).
	clock = clock.Add(2 * time.Minute)
	if body, complete := offer(2, 2, "late"); complete {
		t.Fatalf("a group whose first segment timed out must not complete, got %q", body)
	}
}
