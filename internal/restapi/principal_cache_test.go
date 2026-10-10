package restapi_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/restapi"
)

type countingPrincipals struct {
	calls     atomic.Int32
	principal cp.APIKeyPrincipal
	found     bool
	err       error
	// gate, when set, holds every lookup until it is closed.
	gate chan struct{}
}

func (c *countingPrincipals) PrincipalByAPIKeyHash(context.Context, string) (cp.APIKeyPrincipal, bool, error) {
	c.calls.Add(1)
	if c.gate != nil {
		<-c.gate
	}
	return c.principal, c.found, c.err
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func lookup(t *testing.T, c *restapi.PrincipalCache, hash string) (cp.APIKeyPrincipal, bool) {
	t.Helper()
	p, found, err := c.PrincipalByAPIKeyHash(context.Background(), hash)
	if err != nil {
		t.Fatalf("PrincipalByAPIKeyHash() error = %v", err)
	}
	return p, found
}

func TestPrincipalCacheServesARepeatFromMemory(t *testing.T) {
	store := &countingPrincipals{principal: activePrincipal(), found: true}
	clock := &fakeClock{t: time.Unix(1_000, 0)}
	cache := restapi.NewPrincipalCache(store, 30*time.Second, clock.now)

	for range 3 {
		p, found := lookup(t, cache, "h1")
		if !found || p != store.principal {
			t.Fatalf("lookup = (%+v, %v), want the store's principal", p, found)
		}
	}
	if got := store.calls.Load(); got != 1 {
		t.Errorf("store called %d times for 3 lookups of one key, want 1", got)
	}
	lookup(t, cache, "h2")
	if got := store.calls.Load(); got != 2 {
		t.Errorf("store called %d times after a second key, want 2: entries are per hash", got)
	}
}

func TestPrincipalCacheFlushForgetsEverything(t *testing.T) {
	store := &countingPrincipals{principal: activePrincipal(), found: true}
	clock := &fakeClock{t: time.Unix(1_000, 0)}
	cache := restapi.NewPrincipalCache(store, 30*time.Second, clock.now)

	lookup(t, cache, "h1")
	cache.Flush()
	lookup(t, cache, "h1")
	if got := store.calls.Load(); got != 2 {
		t.Errorf("store called %d times across a Flush, want 2", got)
	}
}

// A lookup that read Postgres before an admin commit must not land in the cache after the Flush that
// announced the commit: the stale principal would outlive the announcement meant to erase it.
func TestPrincipalCacheDropsALookupStartedBeforeAFlush(t *testing.T) {
	store := &countingPrincipals{principal: activePrincipal(), found: true, gate: make(chan struct{})}
	clock := &fakeClock{t: time.Unix(1_000, 0)}
	cache := restapi.NewPrincipalCache(store, 30*time.Second, clock.now)

	var wg sync.WaitGroup
	wg.Go(func() { lookup(t, cache, "h1") })
	for store.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cache.Flush()
	close(store.gate)
	wg.Wait()

	lookup(t, cache, "h1")
	if got := store.calls.Load(); got != 2 {
		t.Errorf("store called %d times, want 2: the in-flight lookup outlived the Flush", got)
	}
}

func TestPrincipalCacheEntryExpires(t *testing.T) {
	grace := time.Unix(1_010, 0)
	cases := []struct {
		name     string
		grace    time.Time
		stillHit time.Time
		expired  time.Time
	}{
		{"after the ttl", time.Time{}, time.Unix(1_029, 0), time.Unix(1_030, 0)},
		{"at the rotation grace deadline when it comes first", grace, time.Unix(1_009, 0), grace},
		{"after the ttl when the grace deadline is later", time.Unix(5_000, 0), time.Unix(1_029, 0), time.Unix(1_030, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := activePrincipal()
			p.GraceExpiresAt = tc.grace
			store := &countingPrincipals{principal: p, found: true}
			clock := &fakeClock{t: time.Unix(1_000, 0)}
			cache := restapi.NewPrincipalCache(store, 30*time.Second, clock.now)

			lookup(t, cache, "h1")
			clock.t = tc.stillHit
			lookup(t, cache, "h1")
			if got := store.calls.Load(); got != 1 {
				t.Fatalf("store called %d times at %v, want 1: the entry is still valid", got, tc.stillHit.Unix())
			}
			clock.t = tc.expired
			lookup(t, cache, "h1")
			if got := store.calls.Load(); got != 2 {
				t.Errorf("store called %d times at %v, want 2: the entry has expired", got, tc.expired.Unix())
			}
		})
	}
}

func TestPrincipalCacheNeverKeepsAMissOrAnError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *countingPrincipals
	}{
		{"unknown key", &countingPrincipals{}},
		{"store error", &countingPrincipals{err: errors.New("postgres down")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := restapi.NewPrincipalCache(tc.store, 30*time.Second, (&fakeClock{t: time.Unix(1_000, 0)}).now)
			for range 2 {
				_, found, err := cache.PrincipalByAPIKeyHash(context.Background(), "h1")
				if found || (err != nil) != (tc.store.err != nil) {
					t.Fatalf("lookup = (found %v, err %v), want the store's answer passed through", found, err)
				}
			}
			if got := tc.store.calls.Load(); got != 2 {
				t.Errorf("store called %d times for 2 lookups, want 2: nothing is cached", got)
			}
		})
	}
}
