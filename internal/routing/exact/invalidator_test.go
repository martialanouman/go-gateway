package exact

import (
	"context"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// fakePipeliner implements only the one command the invalidator may use. Embedding the interface
// satisfies the compiler for the ~200 methods it does not implement, and any call to one of them
// panics — which is the assertion: the invalidator must reach for Del and nothing else.
type fakePipeliner struct {
	goredis.Pipeliner
	deleted []string
}

func (p *fakePipeliner) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	// Every command inside a pipeline must stay single-key: the {msisdn} hash tag puts each number on
	// its own cluster slot, so a multi-key DEL is a CROSSSLOT error waiting for the first cluster
	// deployment. Recorded rather than asserted here, and checked by the caller's test.
	p.deleted = append(p.deleted, keys...)
	return goredis.NewIntResult(int64(len(keys)), nil)
}

// fakePipeRedis records one fakePipeliner per Pipelined call, so a test can count round trips.
type fakePipeRedis struct {
	batches []*fakePipeliner
	err     error
}

func (f *fakePipeRedis) Pipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error) {
	p := &fakePipeliner{}
	f.batches = append(f.batches, p)
	if err := fn(p); err != nil {
		return nil, err
	}
	return nil, f.err
}

func (f *fakePipeRedis) keys() []string {
	var all []string
	for _, b := range f.batches {
		all = append(all, b.deleted...)
	}
	return all
}

// TestInvalidateDeletesTheCacheKeys: the control plane's whole contribution to the data plane is
// "forget what you know about this number". It must clear exactly the keys the resolver reads — through
// redisKey, so the Admin API never learns the wire form.
func TestInvalidateDeletesTheCacheKeys(t *testing.T) {
	rdb := &fakePipeRedis{}
	inv := NewInvalidator(rdb)

	if err := inv.Invalidate(context.Background(), "2250700000001", "2250700000002"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	got := rdb.keys()
	want := []string{"exactroute:{2250700000001}", "exactroute:{2250700000002}"}
	if len(got) != len(want) {
		t.Fatalf("deleted %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("deleted[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestInvalidateDeletesOneKeyPerCommand: the hash tag pins each number to its own cluster slot, so a
// single DEL carrying several keys is a CROSSSLOT error the moment Redis runs clustered — the very
// deployment the tag exists for. Batching belongs to the pipeline, never to the command.
func TestInvalidateDeletesOneKeyPerCommand(t *testing.T) {
	rdb := &multiKeyWatcher{}
	inv := NewInvalidator(rdb)

	if err := inv.Invalidate(context.Background(), "2250700000001", "2250700000002", "2250700000003"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if rdb.maxKeysPerCall != 1 {
		t.Errorf("a DEL carried %d keys, want 1 per command (cross-slot in a cluster)", rdb.maxKeysPerCall)
	}
}

// TestInvalidateChunksLargeBatches: an MNP import invalidates up to 10 000 numbers at once. They go out
// in bounded pipelines rather than one unbounded buffer.
func TestInvalidateChunksLargeBatches(t *testing.T) {
	msisdns := make([]string, invalidateChunk*2+1)
	for i := range msisdns {
		msisdns[i] = "225070000" + string(rune('0'+i%10)) + string(rune('0'+(i/10)%10))
	}
	rdb := &fakePipeRedis{}

	if err := NewInvalidator(rdb).Invalidate(context.Background(), msisdns...); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if len(rdb.batches) != 3 {
		t.Errorf("%d pipeline round trip(s) for %d numbers, want 3 of at most %d",
			len(rdb.batches), len(msisdns), invalidateChunk)
	}
	if got := len(rdb.keys()); got != len(msisdns) {
		t.Errorf("deleted %d key(s), want %d — chunking must not drop any", got, len(msisdns))
	}
}

// TestInvalidateNoNumbersIsNoOp: an import that validated nothing must not open a Redis round trip.
func TestInvalidateNoNumbersIsNoOp(t *testing.T) {
	rdb := &fakePipeRedis{}
	if err := NewInvalidator(rdb).Invalidate(context.Background()); err != nil {
		t.Fatalf("Invalidate() = %v, want nil", err)
	}
	if len(rdb.batches) != 0 {
		t.Errorf("%d round trip(s) for zero numbers, want 0", len(rdb.batches))
	}
}

// TestInvalidateSurfacesTheFault: the invalidator reports a failure rather than swallowing it. What to
// do with it is the caller's decision — the Admin handlers log and carry on, because the TTL bounds the
// staleness — but a silent error would make that choice unobservable.
func TestInvalidateSurfacesTheFault(t *testing.T) {
	want := errors.New("redis down")
	rdb := &fakePipeRedis{err: want}

	if err := NewInvalidator(rdb).Invalidate(context.Background(), "2250700000001"); !errors.Is(err, want) {
		t.Errorf("Invalidate = %v, want it to wrap %v", err, want)
	}
}

// multiKeyWatcher records the widest DEL the invalidator issues inside a pipeline.
type multiKeyWatcher struct{ maxKeysPerCall int }

func (m *multiKeyWatcher) Pipelined(_ context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error) {
	return nil, fn(&countingPipeliner{owner: m})
}

type countingPipeliner struct {
	goredis.Pipeliner
	owner *multiKeyWatcher
}

func (p *countingPipeliner) Del(_ context.Context, keys ...string) *goredis.IntCmd {
	if len(keys) > p.owner.maxKeysPerCall {
		p.owner.maxKeysPerCall = len(keys)
	}
	return goredis.NewIntResult(int64(len(keys)), nil)
}
