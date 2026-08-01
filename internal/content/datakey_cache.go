package content

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// DataKey is a customer's active content-key id and its plaintext data key (DEK). The DEK is SENSITIVE:
// hold it only as long as needed to seal a body, never log or persist it. Callers receive a copy.
type DataKey struct {
	KeyID uuid.UUID
	DEK   []byte
}

// DataKeyFetcher fetches a customer's active DEK from the authority (content-key-svc). It is the slow, remote
// source the cache sits in front of. Declared consumer-side.
type DataKeyFetcher interface {
	Fetch(ctx context.Context, customerID uuid.UUID) (DataKey, error)
}

// DataKeyCache fronts a DataKeyFetcher with a per-customer, TTL-bounded, LRU-capped cache. The data plane
// encrypts bodies at up to thousands per second; without this it would call the key service once per message.
//
// It collapses concurrent misses for the same customer with singleflight (one fetch feeds the whole burst),
// bounds memory with an LRU cap, and NEVER caches an error — a failed fetch is returned to the caller and the
// next call retries. Entries are copied out, so a caller cannot mutate (or zeroize) the cached DEK.
//
// The TTL is deliberately short: it also bounds how long a rotated-away or crypto-shredded key can still be
// used by a data-plane pod before the cache re-fetches (the durable key is the authority).
type DataKeyCache struct {
	fetcher DataKeyFetcher
	ttl     time.Duration
	maxSize int
	now     func() time.Time
	group   singleflight.Group

	mu      sync.Mutex
	entries map[uuid.UUID]*list.Element // customer_id -> LRU element holding *dkEntry
	lru     *list.List                  // front = most recently used
}

type dkEntry struct {
	customerID uuid.UUID
	key        DataKey
	expiresAt  time.Time
}

// DataKeyCacheOption configures a DataKeyCache.
type DataKeyCacheOption func(*DataKeyCache)

// WithTTL sets how long a cached DEK is served before a re-fetch. Non-positive values are ignored.
func WithTTL(ttl time.Duration) DataKeyCacheOption {
	return func(c *DataKeyCache) {
		if ttl > 0 {
			c.ttl = ttl
		}
	}
}

// WithMaxEntries caps the number of customers held (LRU eviction beyond it). Non-positive values are ignored.
func WithMaxEntries(n int) DataKeyCacheOption {
	return func(c *DataKeyCache) {
		if n > 0 {
			c.maxSize = n
		}
	}
}

// WithClock overrides the time source (tests). Ignored if nil.
func WithClock(now func() time.Time) DataKeyCacheOption {
	return func(c *DataKeyCache) {
		if now != nil {
			c.now = now
		}
	}
}

// NewDataKeyCache builds a cache over fetcher with sensible defaults (5-minute TTL, 4096 customers).
func NewDataKeyCache(fetcher DataKeyFetcher, opts ...DataKeyCacheOption) *DataKeyCache {
	c := &DataKeyCache{
		fetcher: fetcher,
		ttl:     5 * time.Minute,
		maxSize: 4096,
		now:     time.Now,
		entries: make(map[uuid.UUID]*list.Element),
		lru:     list.New(),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Get returns the customer's active DEK, serving a fresh cached copy or fetching (once, even under a
// concurrent burst) when absent or expired. The returned DataKey holds a private copy of the DEK.
func (c *DataKeyCache) Get(ctx context.Context, customerID uuid.UUID) (DataKey, error) {
	if key, ok := c.lookup(customerID); ok {
		return key, nil
	}
	// Miss or expired: fetch once for all concurrent callers of this customer. Caveat (singleflight): the
	// leader's ctx bounds the shared fetch, so if the leader is cancelled the waiters see its error even if
	// their own ctx was live — acceptable for a cache; the next call retries.
	v, err, _ := c.group.Do(customerID.String(), func() (any, error) {
		// Re-check under the flight: a racing caller may have populated the entry between our lookup and here.
		if key, ok := c.lookup(customerID); ok {
			return key, nil
		}
		fetched, ferr := c.fetcher.Fetch(ctx, customerID)
		if ferr != nil {
			return DataKey{}, ferr
		}
		c.store(customerID, fetched)
		return c.copyOf(fetched), nil
	})
	if err != nil {
		return DataKey{}, err
	}
	// singleflight hands the SAME value to every duplicate caller of a flight, so the boxed DataKey (and its
	// DEK slice) is shared. Copy per caller here so each gets a private DEK — the whole point of this cache is
	// to collapse a concurrent burst into one fetch, and a shared slice would let one caller's zeroize corrupt
	// the others. Reading the shared DEK to copy it is safe (no writer inside the flight).
	return c.copyOf(v.(DataKey)), nil
}

// lookup returns a copy of a live (non-expired) cached DEK, refreshing its recency.
func (c *DataKeyCache) lookup(customerID uuid.UUID) (DataKey, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[customerID]
	if !ok {
		return DataKey{}, false
	}
	e := el.Value.(*dkEntry)
	if !c.now().Before(e.expiresAt) {
		return DataKey{}, false // expired; leave it for store() to overwrite
	}
	c.lru.MoveToFront(el)
	return c.copyOf(e.key), true
}

// store inserts or refreshes the customer's entry and evicts the least-recently-used beyond the cap.
func (c *DataKeyCache) store(customerID uuid.UUID, key DataKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &dkEntry{customerID: customerID, key: c.copyOf(key), expiresAt: c.now().Add(c.ttl)}
	if el, ok := c.entries[customerID]; ok {
		zero(el.Value.(*dkEntry).key.DEK)
		el.Value = e
		c.lru.MoveToFront(el)
	} else {
		c.entries[customerID] = c.lru.PushFront(e)
	}
	for c.lru.Len() > c.maxSize {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		ev := oldest.Value.(*dkEntry)
		zero(ev.key.DEK)
		delete(c.entries, ev.customerID)
		c.lru.Remove(oldest)
	}
}

// copyOf returns a DataKey with a private copy of the DEK bytes, so the cache's slice is never aliased out.
func (c *DataKeyCache) copyOf(k DataKey) DataKey {
	dek := make([]byte, len(k.DEK))
	copy(dek, k.DEK)
	return DataKey{KeyID: k.KeyID, DEK: dek}
}

// zero best-effort wipes a DEK slice on eviction/overwrite so it does not linger in freed memory.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
