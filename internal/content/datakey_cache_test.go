package content_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/content"
)

type fakeFetcher struct {
	calls   atomic.Int64
	dek     []byte
	keyID   uuid.UUID
	err     error
	release chan struct{} // if non-nil, Fetch blocks until closed (for the singleflight test)
}

func (f *fakeFetcher) Fetch(_ context.Context, _ uuid.UUID) (content.DataKey, error) {
	f.calls.Add(1)
	if f.release != nil {
		<-f.release
	}
	if f.err != nil {
		return content.DataKey{}, f.err
	}
	return content.DataKey{KeyID: f.keyID, DEK: f.dek}, nil
}

func newDEK(b byte) []byte {
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = b
	}
	return dek
}

// TestDataKeyCacheHitAvoidsRefetch: a second Get within the TTL is served from cache (one fetch total).
func TestDataKeyCacheHitAvoidsRefetch(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(1), keyID: uuid.New()}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute))
	cust := uuid.New()

	for i := 0; i < 3; i++ {
		got, err := c.Get(context.Background(), cust)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.KeyID != f.keyID || len(got.DEK) != 32 {
			t.Fatalf("got %+v", got)
		}
	}
	if f.calls.Load() != 1 {
		t.Errorf("fetcher called %d times, want 1 (cache hit)", f.calls.Load())
	}
}

// TestDataKeyCacheReturnsPrivateCopy: mutating a returned DEK must not corrupt the cached one.
func TestDataKeyCacheReturnsPrivateCopy(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(7), keyID: uuid.New()}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute))
	cust := uuid.New()

	first, _ := c.Get(context.Background(), cust)
	for i := range first.DEK {
		first.DEK[i] = 0xFF // caller scribbles on its copy
	}
	second, _ := c.Get(context.Background(), cust)
	if second.DEK[0] != 7 {
		t.Errorf("cached DEK was corrupted by a caller mutation: got %d, want 7", second.DEK[0])
	}
}

// TestDataKeyCacheExpiryRefetches: past the TTL the cache re-fetches.
func TestDataKeyCacheExpiryRefetches(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(2), keyID: uuid.New()}
	nowP := &atomicTime{}
	nowP.set(time.Unix(1_000_000, 0))
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute), content.WithClock(nowP.get))
	cust := uuid.New()

	if _, err := c.Get(context.Background(), cust); err != nil {
		t.Fatal(err)
	}
	nowP.set(nowP.get().Add(2 * time.Minute)) // past TTL
	if _, err := c.Get(context.Background(), cust); err != nil {
		t.Fatal(err)
	}
	if f.calls.Load() != 2 {
		t.Errorf("fetcher called %d times, want 2 (expiry re-fetch)", f.calls.Load())
	}
}

// TestDataKeyCacheErrorNotCached: a failed fetch is not cached — the next Get retries.
func TestDataKeyCacheErrorNotCached(t *testing.T) {
	f := &fakeFetcher{err: errors.New("billing down")}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute))
	cust := uuid.New()

	if _, err := c.Get(context.Background(), cust); err == nil {
		t.Fatal("Get = nil, want the fetch error")
	}
	f.err = nil
	f.dek, f.keyID = newDEK(3), uuid.New()
	if _, err := c.Get(context.Background(), cust); err != nil {
		t.Fatalf("retry after error failed: %v", err)
	}
	if f.calls.Load() != 2 {
		t.Errorf("fetcher called %d times, want 2 (error not cached)", f.calls.Load())
	}
}

// TestDataKeyCacheSingleflight: a burst of concurrent Gets for one customer during a slow fetch triggers a
// single fetch.
func TestDataKeyCacheSingleflight(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(4), keyID: uuid.New(), release: make(chan struct{})}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute))
	cust := uuid.New()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Get(context.Background(), cust)
		}()
	}
	close(start)
	time.Sleep(20 * time.Millisecond) // let the goroutines pile into the flight
	close(f.release)                  // let the single fetch complete
	wg.Wait()

	if got := f.calls.Load(); got != 1 {
		t.Errorf("fetcher called %d times under a concurrent burst, want 1 (singleflight)", got)
	}
}

// TestDataKeyCacheLRUEviction: beyond the cap the least-recently-used customer is evicted; the survivor stays
// cached. With cap 2: a, b cached (a is LRU); inserting d evicts a; a then re-fetches while d stays cached.
func TestDataKeyCacheLRUEviction(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(5), keyID: uuid.New()}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Hour), content.WithMaxEntries(2))
	a, b, d := uuid.New(), uuid.New(), uuid.New()

	get := func(id uuid.UUID) { _, _ = c.Get(context.Background(), id) }
	get(a) // [a]
	get(b) // [b,a]  -> a is least-recently-used
	get(d) // insert d, evict a -> [d,b]

	was := f.calls.Load()
	get(a) // a was evicted -> re-fetch
	if f.calls.Load() != was+1 {
		t.Errorf("evicted customer a should re-fetch")
	}
	// The get(a) above made the cache [a,d] (b evicted). d is still cached.
	was = f.calls.Load()
	get(d)
	if f.calls.Load() != was {
		t.Errorf("recently-used customer d should still be cached, got a re-fetch")
	}
}

// TestDataKeyCacheConcurrentCallersGetIsolatedCopies: under a singleflight burst, each waiter must receive a
// PRIVATE DEK slice — one caller zeroizing its copy must not corrupt another's (regression guard for the
// shared-slice bug where singleflight hands every duplicate the same value).
func TestDataKeyCacheConcurrentCallersGetIsolatedCopies(t *testing.T) {
	f := &fakeFetcher{dek: newDEK(9), keyID: uuid.New(), release: make(chan struct{})}
	c := content.NewDataKeyCache(f, content.WithTTL(time.Minute))
	cust := uuid.New()

	const n = 16
	keys := make([]content.DataKey, n)
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			k, _ := c.Get(context.Background(), cust)
			keys[i] = k
		}(i)
	}
	close(start)
	time.Sleep(20 * time.Millisecond)
	close(f.release)
	wg.Wait()

	// Each caller zeroizes its own DEK; if any two shared a backing array this races/corrupts.
	for i := range keys {
		if len(keys[i].DEK) != 32 {
			t.Fatalf("caller %d got DEK len %d", i, len(keys[i].DEK))
		}
		for j := range keys[i].DEK {
			keys[i].DEK[j] = 0
		}
	}
	// A fresh Get (still within TTL) must still see the intact cached DEK.
	fresh, _ := c.Get(context.Background(), cust)
	if fresh.DEK[0] != 9 {
		t.Errorf("cached DEK corrupted by callers zeroizing their copies: got %d, want 9", fresh.DEK[0])
	}
}

// atomicTime is a tiny mutex-guarded clock for the expiry test.
type atomicTime struct {
	mu sync.Mutex
	t  time.Time
}

func (a *atomicTime) set(t time.Time) { a.mu.Lock(); a.t = t; a.mu.Unlock() }
func (a *atomicTime) get() time.Time  { a.mu.Lock(); defer a.mu.Unlock(); return a.t }
