package exact

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
)

// bloomFP is the target false-positive rate of the boot-loaded filter. A Bloom miss is definitive, so
// it never drops a real override; a Bloom hit costs one Redis GET, so a low rate keeps wasted lookups
// rare without an oversized filter (~1.2 MB per million entries at this rate).
const bloomFP = 0.001

// minBloomCapacity floors the sizing estimate so an empty or tiny table still yields a usable filter:
// NewWithEstimates(0, …) would size for no elements and degrade as the table grows before the next
// reload.
const minBloomCapacity = 1024

// bloomPageSize is how many MSISDNs LoadBloom reads per keyset page while building the filter.
const bloomPageSize = 1000

// MSISDNLister pages the exact-route MSISDNs for the Bloom build, msisdn-ordered by keyset cursor.
// *postgres.ExactRouteRepo satisfies it via List(ctx, after, limit).
type MSISDNLister interface {
	List(ctx context.Context, after string, limit int) ([]Route, error)
}

// Bloom is an in-memory membership filter over the configured exact-route MSISDNs — the L0 fast path's
// negative gate. MightContain==false is a definitive miss (no Redis read); ==true is a possible hit
// that Redis must confirm. The filter is held behind an atomic pointer so Reload can swap a freshly
// built filter in under live traffic, lock-free and with no routing hole: readers see either the whole
// old filter or the whole new one, never a partial (step-106).
type Bloom struct {
	current atomic.Pointer[bloom.BloomFilter]
}

// LoadBloom builds the filter by paging every exact-route MSISDN once at startup. It never yields a
// false negative, so "absent" is a certain "no override" (spec §6.1). config-sync calls Reload
// thereafter to hot-swap it without a restart.
func LoadBloom(ctx context.Context, lister MSISDNLister) (*Bloom, error) {
	filter, err := buildFilter(ctx, lister)
	if err != nil {
		return nil, err
	}
	b := &Bloom{}
	b.current.Store(filter)
	return b, nil
}

// Reload rebuilds the filter from the current exact_routes and swaps it in atomically. On a build
// failure the current filter keeps serving (nothing is swapped), so a transient Postgres blip never
// leaves the L0 gate empty. Safe to call concurrently with MightContain.
func (b *Bloom) Reload(ctx context.Context, lister MSISDNLister) error {
	filter, err := buildFilter(ctx, lister)
	if err != nil {
		return err
	}
	b.current.Store(filter)
	return nil
}

// MightContain reports whether msisdn may be a configured exact route. false is definitive (the caller
// skips Redis and resolves normally); true means "confirm against Redis" (it may be a false positive).
func (b *Bloom) MightContain(msisdn string) bool {
	return b.current.Load().TestString(msisdn)
}

// CapacityBits is the filter's bit-array size (m), for a reload-size metric.
func (b *Bloom) CapacityBits() uint { return b.current.Load().Cap() }

// buildFilter pages every exact-route MSISDN and builds a fresh filter. It is the shared core of
// LoadBloom and Reload.
func buildFilter(ctx context.Context, lister MSISDNLister) (*bloom.BloomFilter, error) {
	var msisdns []string
	after := ""
	for {
		page, err := lister.List(ctx, after, bloomPageSize)
		if err != nil {
			return nil, fmt.Errorf("exact: build bloom: %w", err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			msisdns = append(msisdns, r.MSISDN)
		}
		after = page[len(page)-1].MSISDN
		if len(page) < bloomPageSize {
			break
		}
	}
	return newFilter(msisdns), nil
}

// newFilter sizes and fills a Bloom filter for the given MSISDNs.
func newFilter(msisdns []string) *bloom.BloomFilter {
	capacity := len(msisdns)
	if capacity < minBloomCapacity {
		capacity = minBloomCapacity
	}
	filter := bloom.NewWithEstimates(uint(capacity), bloomFP) //nolint:gosec // capacity is a non-negative count
	for _, m := range msisdns {
		filter.AddString(m)
	}
	return filter
}

// newBloom wraps a filter over the given MSISDNs — the in-memory test constructor.
func newBloom(msisdns []string) *Bloom {
	b := &Bloom{}
	b.current.Store(newFilter(msisdns))
	return b
}
