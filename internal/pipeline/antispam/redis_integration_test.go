package antispam_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRedisStateDuplicate proves the SET NX EX semantics against real Redis: the first sighting is
// new, an immediate repeat is a duplicate, and after the window elapses it is new again.
func TestRedisStateDuplicate(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	const fp = "abc123fingerprint"

	if seen, err := state.Seen(ctx, fp, 200*time.Millisecond); err != nil || seen {
		t.Fatalf("first Seen = (%t, %v), want (false, nil)", seen, err)
	}
	if seen, err := state.Seen(ctx, fp, 200*time.Millisecond); err != nil || !seen {
		t.Fatalf("immediate repeat = (%t, %v), want (true, nil)", seen, err)
	}
	time.Sleep(300 * time.Millisecond)
	if seen, err := state.Seen(ctx, fp, 200*time.Millisecond); err != nil || seen {
		t.Fatalf("post-expiry Seen = (%t, %v), want (false, nil)", seen, err)
	}
}

// TestRedisStateVelocity proves the sliding-window counter against real Redis: repeated Hits within
// the window accumulate, and a MO Record contributes to the SAME key a later Hit counts.
func TestRedisStateVelocity(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	const key = "global:source:22507000001"
	for want := 1; want <= 3; want++ {
		n, err := state.Hit(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("Hit %d: %v", want, err)
		}
		if n != want {
			t.Errorf("Hit returned %d, want %d (sliding window accumulates)", n, want)
		}
	}

	// An inbound-MO Record on the same source key contributes to its count.
	moKey := antispam.MOSourceVelocityKey("22507000002")
	if err := state.Record(ctx, moKey); err != nil {
		t.Fatalf("Record: %v", err)
	}
	n, err := state.Hit(ctx, moKey, time.Minute)
	if err != nil {
		t.Fatalf("Hit after Record: %v", err)
	}
	if n != 2 {
		t.Errorf("count after MO Record + one Hit = %d, want 2 (MO and MT share the window)", n)
	}
}

// TestRedisStateVelocityWindowTrims proves old events fall out of the window: a Hit with a tiny window
// after a pause counts only the recent event.
func TestRedisStateVelocityWindowTrims(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	const key = "global:source:22507000005"
	if _, err := state.Hit(ctx, key, 100*time.Millisecond); err != nil {
		t.Fatalf("first Hit: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	n, err := state.Hit(ctx, key, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("second Hit: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1 (the first event fell outside the window)", n)
	}
}

// TestRedisStateReputation proves the score lookup: a missing source is unscored, a set score is
// returned.
func TestRedisStateReputation(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	if _, found, err := state.Reputation(ctx, "22507000006"); err != nil || found {
		t.Fatalf("unscored source = (found %t, %v), want (false, nil)", found, err)
	}
	if err := rdb.Set(ctx, "antispam:rep:22507000006", 42, 0).Err(); err != nil {
		t.Fatalf("seed score: %v", err)
	}
	score, found, err := state.Reputation(ctx, "22507000006")
	if err != nil || !found || score != 42 {
		t.Errorf("scored source = (%d, %t, %v), want (42, true, nil)", score, found, err)
	}
}
