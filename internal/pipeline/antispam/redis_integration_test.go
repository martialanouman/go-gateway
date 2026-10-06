package antispam_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
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

	if seen, err := state.Seen(ctx, fp, uuid.New(), 200*time.Millisecond); err != nil || seen {
		t.Fatalf("first Seen = (%t, %v), want (false, nil)", seen, err)
	}
	if seen, err := state.Seen(ctx, fp, uuid.New(), 200*time.Millisecond); err != nil || !seen {
		t.Fatalf("immediate repeat = (%t, %v), want (true, nil)", seen, err)
	}
	time.Sleep(300 * time.Millisecond)
	if seen, err := state.Seen(ctx, fp, uuid.New(), 200*time.Millisecond); err != nil || seen {
		t.Fatalf("post-expiry Seen = (%t, %v), want (false, nil)", seen, err)
	}
}

// TestRedisStateReplayIsNotItsOwnDuplicate proves a redelivered message (same message_id) is not a duplicate
// of its own first pass, while another message with the same fingerprint still is (step-285c).
func TestRedisStateReplayIsNotItsOwnDuplicate(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	fp, first := uuid.NewString(), uuid.New()
	for pass := 1; pass <= 2; pass++ {
		if seen, err := state.Seen(ctx, fp, first, time.Minute); err != nil || seen {
			t.Fatalf("pass %d of the same message = (%t, %v), want (false, nil)", pass, seen, err)
		}
	}
	if seen, err := state.Seen(ctx, fp, uuid.New(), time.Minute); err != nil || !seen {
		t.Fatalf("another message, same fingerprint = (%t, %v), want (true, nil)", seen, err)
	}
}

// TestRedisStateReplayCountsOnce proves a redelivered message is counted once in a velocity window.
func TestRedisStateReplayCountsOnce(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)

	key, replayed := "global:source:"+uuid.NewString(), uuid.New()
	for _, id := range []uuid.UUID{replayed, replayed, uuid.New()} {
		if _, err := state.Hit(ctx, key, id, time.Minute); err != nil {
			t.Fatalf("Hit: %v", err)
		}
	}
	n, err := state.Hit(ctx, key, replayed, time.Minute)
	if err != nil {
		t.Fatalf("Hit: %v", err)
	}
	if n != 2 {
		t.Errorf("count = %d, want 2 (two distinct messages, one replayed three times)", n)
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
		n, err := state.Hit(ctx, key, uuid.New(), time.Minute)
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
	n, err := state.Hit(ctx, moKey, uuid.New(), time.Minute)
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
	if _, err := state.Hit(ctx, key, uuid.New(), 100*time.Millisecond); err != nil {
		t.Fatalf("first Hit: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	n, err := state.Hit(ctx, key, uuid.New(), 100*time.Millisecond)
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

// TestRedisStateCountsCategoryMismatchesOverTwentyFourHours proves the per-sender counter against real
// Redis: a match lands in the current hour, the 23 previous hours still count, the one before does not, a
// redelivered message is counted once, and the counts come back in the order of the senders asked for,
// another customer's same address apart.
func TestRedisStateCountsCategoryMismatchesOverTwentyFourHours(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	state := antispam.NewRedisState(rdb)
	now := time.Date(2026, 10, 6, 10, 5, 0, 0, time.UTC)
	state.SetClock(func() time.Time { return now })
	customer, other := uuid.New(), uuid.New()

	replayed := uuid.New()
	for _, id := range []uuid.UUID{replayed, replayed, uuid.New()} {
		if _, err := state.CountCategoryMismatch(ctx, id, customer, "BANK"); err != nil {
			t.Fatalf("CountCategoryMismatch: %v", err)
		}
	}
	if err := rdb.Set(ctx, antispam.CategoryMismatchKey(customer, "BANK", now.Add(-23*time.Hour)), 3, time.Hour).Err(); err != nil {
		t.Fatalf("seed the 23rd hour back: %v", err)
	}
	if err := rdb.Set(ctx, antispam.CategoryMismatchKey(customer, "BANK", now.Add(-24*time.Hour)), 100, time.Hour).Err(); err != nil {
		t.Fatalf("seed the 24th hour back: %v", err)
	}
	if ttl := rdb.TTL(ctx, antispam.CategoryMismatchKey(customer, "BANK", now)).Val(); ttl <= 24*time.Hour || ttl > 25*time.Hour {
		t.Errorf("current bucket TTL = %v, want just over 24h so the window outlives it", ttl)
	}

	got, err := state.RecentCategoryMismatches(ctx, []cp.SenderAddress{
		{CustomerID: other, Address: "BANK"}, {CustomerID: customer, Address: "BANK"},
	})
	if err != nil {
		t.Fatalf("RecentCategoryMismatches: %v", err)
	}
	if len(got) != 2 || got[0] != 0 || got[1] != 5 {
		t.Fatalf("counts = %v, want [0 5]: two messages now (one replayed), three 23h back, none 24h back", got)
	}
}
