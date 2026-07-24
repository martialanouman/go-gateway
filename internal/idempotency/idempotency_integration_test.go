package idempotency_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// newStore builds a store against the shared test Redis with a short poll so pending waits stay fast.
func newStore(t *testing.T, opts ...idempotency.Option) *idempotency.Store {
	t.Helper()
	rdb := redistest.Client(t)
	opts = append([]idempotency.Option{idempotency.WithPollInterval(5 * time.Millisecond)}, opts...)
	return idempotency.New(rdb, opts...)
}

func TestReserveThenReplay(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()
	response := []byte(`{"id":"abc","status":"accepted"}`)

	res, err := store.Reserve(ctx, account, idemKey, "hash-1", response)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if res.Outcome != idempotency.Reserved {
		t.Fatalf("first reserve: outcome=%v, want Reserved", res.Outcome)
	}

	if err := store.Finalize(ctx, account, idemKey); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	// A repeat with the same body replays the original result.
	res, err = store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"ignored"}`))
	if err != nil {
		t.Fatalf("replay reserve: %v", err)
	}
	if res.Outcome != idempotency.Replay {
		t.Fatalf("replay reserve: outcome=%v, want Replay", res.Outcome)
	}
	if string(res.Response) != string(response) {
		t.Fatalf("replay response = %q, want the original %q", res.Response, response)
	}
}

func TestReserveConflictOnDifferentBody(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()

	if _, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("first reserve: %v", err)
	}

	res, err := store.Reserve(ctx, account, idemKey, "hash-2", []byte(`{"id":"b"}`))
	if err != nil {
		t.Fatalf("conflicting reserve: %v", err)
	}
	if res.Outcome != idempotency.Conflict {
		t.Fatalf("conflicting reserve: outcome=%v, want Conflict", res.Outcome)
	}
}

func TestReservePendingThenAwait(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()
	response := []byte(`{"id":"pending-then-done"}`)

	// Winner reserves but has not finalized yet.
	if _, err := store.Reserve(ctx, account, idemKey, "hash-1", response); err != nil {
		t.Fatalf("winner reserve: %v", err)
	}

	// A concurrent submit of the same key sees the in-flight reservation.
	res, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"loser"}`))
	if err != nil {
		t.Fatalf("loser reserve: %v", err)
	}
	if res.Outcome != idempotency.Pending {
		t.Fatalf("loser reserve: outcome=%v, want Pending", res.Outcome)
	}

	// Finalize concurrently; Await must return the winner's response once done.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = store.Finalize(ctx, account, idemKey)
	}()

	got, err := store.Await(ctx, account, idemKey, time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if string(got) != string(response) {
		t.Fatalf("await response = %q, want %q", got, response)
	}
}

func TestAwaitTimesOutWhilePending(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()

	if _, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// Never finalized: Await gives up with ErrAwaitTimeout rather than blocking forever.
	_, err := store.Await(ctx, account, idemKey, 50*time.Millisecond)
	if !errors.Is(err, idempotency.ErrAwaitTimeout) {
		t.Fatalf("await: err=%v, want ErrAwaitTimeout", err)
	}
}

func TestReleaseAllowsReReserve(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()

	if _, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// A failed publish releases the slot, so a retry (even with a different body) reserves afresh.
	if err := store.Release(ctx, account, idemKey); err != nil {
		t.Fatalf("release: %v", err)
	}

	res, err := store.Reserve(ctx, account, idemKey, "hash-2", []byte(`{"id":"b"}`))
	if err != nil {
		t.Fatalf("re-reserve: %v", err)
	}
	if res.Outcome != idempotency.Reserved {
		t.Fatalf("re-reserve after release: outcome=%v, want Reserved", res.Outcome)
	}
}

func TestReserveSetsWindowTTL(t *testing.T) {
	rdb := redistest.Client(t)
	store := idempotency.New(rdb, idempotency.WithTTL(2*time.Hour))
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()

	if _, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	ttl, err := rdb.TTL(ctx, "idem:{"+account+"}:"+idemKey).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	// EXPIRE was applied at ~2h; allow a small margin below the ceiling.
	if ttl <= 0 || ttl > 2*time.Hour {
		t.Fatalf("ttl = %v, want (0, 2h]", ttl)
	}
}

// TestConcurrentReserveSingleWinner proves the reserve is atomic: N parallel reserves of the same key
// yield exactly one Reserved; the rest see Pending or Replay, never a second Reserved. Run under -race.
func TestConcurrentReserveSingleWinner(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	account, idemKey := uuid.NewString(), uuid.NewString()

	const n = 32
	var (
		wg       sync.WaitGroup
		reserved atomic.Int64
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all at once to maximise contention
			res, err := store.Reserve(ctx, account, idemKey, "hash-1", []byte(`{"id":"a"}`))
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			switch res.Outcome {
			case idempotency.Reserved:
				reserved.Add(1)
			case idempotency.Pending, idempotency.Replay:
				// expected for the losers
			default:
				t.Errorf("unexpected outcome %v", res.Outcome)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := reserved.Load(); got != 1 {
		t.Fatalf("concurrent reserve: %d winners, want exactly 1", got)
	}
}
