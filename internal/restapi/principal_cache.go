package restapi

import (
	"context"
	"sync"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// PrincipalCacheTTL bounds how long a revoked key keeps working when its invalidation announcement is
// lost (step-287j).
const PrincipalCacheTTL = 30 * time.Second

// PrincipalCache keeps the principals of valid API keys in memory, keyed by key hash. Flush empties it;
// the service calls it on every config announcement. Unknown keys and store errors are never kept.
type PrincipalCache struct {
	store PrincipalStore
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	gen     uint64
	entries map[string]cachedPrincipal
}

type cachedPrincipal struct {
	principal cp.APIKeyPrincipal
	expiresAt time.Time
}

// NewPrincipalCache wraps store; now is the clock (time.Now outside tests).
func NewPrincipalCache(store PrincipalStore, ttl time.Duration, now func() time.Time) *PrincipalCache {
	return &PrincipalCache{store: store, ttl: ttl, now: now, entries: map[string]cachedPrincipal{}}
}

// PrincipalByAPIKeyHash answers from memory while the entry is fresh, from the store otherwise.
func (c *PrincipalCache) PrincipalByAPIKeyHash(ctx context.Context, hash string) (cp.APIKeyPrincipal, bool, error) {
	c.mu.Lock()
	entry, ok := c.entries[hash]
	gen := c.gen
	c.mu.Unlock()
	if ok && c.now().Before(entry.expiresAt) {
		return entry.principal, true, nil
	}

	principal, found, err := c.store.PrincipalByAPIKeyHash(ctx, hash)
	if err != nil || !found {
		return principal, found, err
	}

	expiresAt := c.now().Add(c.ttl)
	if !principal.GraceExpiresAt.IsZero() && principal.GraceExpiresAt.Before(expiresAt) {
		expiresAt = principal.GraceExpiresAt
	}
	c.mu.Lock()
	// A Flush since the read means an admin commit may postdate it: keeping it would let the old
	// principal outlive the announcement meant to erase it.
	if c.gen == gen {
		c.entries[hash] = cachedPrincipal{principal: principal, expiresAt: expiresAt}
	}
	c.mu.Unlock()
	return principal, true, nil
}

// Flush forgets every entry, including the lookups still in flight.
func (c *PrincipalCache) Flush() {
	c.mu.Lock()
	c.gen++
	c.entries = map[string]cachedPrincipal{}
	c.mu.Unlock()
}
