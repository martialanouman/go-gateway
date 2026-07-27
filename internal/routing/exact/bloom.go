package exact

import (
	"context"
	"fmt"

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

// Bloom is an immutable in-memory membership filter over the configured exact-route MSISDNs — the L0
// fast path's negative gate. MightContain==false is a definitive miss (no Redis read); ==true is a
// possible hit that Redis must confirm. Safe for concurrent reads: nothing mutates it after LoadBloom
// (hot reload swaps the whole filter, step-106).
type Bloom struct {
	filter *bloom.BloomFilter
}

// LoadBloom builds the filter by paging every exact-route MSISDN once at startup. It never yields a
// false negative, so "absent" is a certain "no override" (spec §6.1). Hot reload without a restart
// arrives with config-sync (step-106); here the filter is loaded once.
func LoadBloom(ctx context.Context, lister MSISDNLister) (*Bloom, error) {
	var msisdns []string
	after := ""
	for {
		page, err := lister.List(ctx, after, bloomPageSize)
		if err != nil {
			return nil, fmt.Errorf("exact: load bloom: %w", err)
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
	return newBloom(msisdns), nil
}

// newBloom sizes and fills a filter for the given MSISDNs. It is the shared core of LoadBloom and the
// in-memory test constructor.
func newBloom(msisdns []string) *Bloom {
	capacity := len(msisdns)
	if capacity < minBloomCapacity {
		capacity = minBloomCapacity
	}
	filter := bloom.NewWithEstimates(uint(capacity), bloomFP) //nolint:gosec // capacity is a non-negative count
	for _, m := range msisdns {
		filter.AddString(m)
	}
	return &Bloom{filter: filter}
}

// MightContain reports whether msisdn may be a configured exact route. false is definitive (the caller
// skips Redis and resolves normally); true means "confirm against Redis" (it may be a false positive).
func (b *Bloom) MightContain(msisdn string) bool {
	return b.filter.TestString(msisdn)
}
