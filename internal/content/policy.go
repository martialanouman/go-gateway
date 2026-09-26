package content

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// EffectiveStorage resolves a customer's raw content_storage into the policy actually applied at CDR write.
// `inherit` takes the platform default (step-370), which can only be off or stored_encrypted: a plaintext
// platform — refused by the database already — still resolves to off here, because storage in clear needs
// the customer's own contract. Any unknown value is off.
func EffectiveStorage(cs, platform cp.ContentStorage) cp.ContentStorage {
	switch cs {
	case cp.ContentOff, cp.ContentStoredPlaintext, cp.ContentStoredEncrypted:
		return cs
	case cp.ContentInherit:
		if platform == cp.ContentStoredEncrypted {
			return platform
		}
		return cp.ContentOff
	default:
		return cp.ContentOff
	}
}

// PolicyLister loads every customer's content_storage and the platform default inherit resolves to.
// *postgres.CustomerRepo satisfies it; declared consumer-side.
type PolicyLister interface {
	ListContentStorage(ctx context.Context) ([]cp.CustomerContentPolicy, error)
	PlatformContentStorage(ctx context.Context) (cp.ContentStorage, error)
}

// PolicySnapshot is an immutable, lock-free map of customer_id → effective content_storage, loaded once at
// boot (the sender-ID snapshot pattern). The data plane resolves a message's storage policy per message with
// a plain map read. A customer absent from the snapshot resolves to OFF.
//
// A snapshot is IMMUTABLE. To honour a policy change without a restart — critically an opt-out (stored_*→off,
// a consent withdrawal or GDPR request), which must take effect promptly — hold it in a PolicyHolder and swap
// a freshly loaded snapshot on each config-change invalidation (router-svc's snapshot watcher does this). A
// bare snapshot without the holder only reflects the state at load time.
type PolicySnapshot struct {
	byCustomer map[uuid.UUID]cp.ContentStorage
}

// LoadPolicySnapshot reads every customer's content_storage once and indexes the EFFECTIVE policy per customer
// (inherit already resolved against the platform default). An unreadable platform default is an error, never
// a silent off: pods falling back one by one would store differently for the same customer.
func LoadPolicySnapshot(ctx context.Context, lister PolicyLister) (*PolicySnapshot, error) {
	platform, err := lister.PlatformContentStorage(ctx)
	if err != nil {
		return nil, fmt.Errorf("content: load platform content-storage default: %w", err)
	}
	rows, err := lister.ListContentStorage(ctx)
	if err != nil {
		return nil, fmt.Errorf("content: load content-storage snapshot: %w", err)
	}
	byCustomer := make(map[uuid.UUID]cp.ContentStorage, len(rows))
	for _, r := range rows {
		byCustomer[r.CustomerID] = EffectiveStorage(r.ContentStorage, platform)
	}
	return &PolicySnapshot{byCustomer: byCustomer}, nil
}

// For returns the customer's effective content-storage policy (off when the customer is unknown).
func (s *PolicySnapshot) For(customerID uuid.UUID) cp.ContentStorage {
	if cs, ok := s.byCustomer[customerID]; ok {
		return cs
	}
	return cp.ContentOff
}

// PolicyHolder keeps the current PolicySnapshot behind an atomic pointer so the data plane can HOT-RELOAD the
// content-storage policy on a config change without a restart. This is what makes an opt-out (stored_*→off, a
// consent withdrawal or GDPR request) take effect promptly rather than only at the next restart — the unsafe
// staleness direction the plain snapshot could not honour. Store swaps the whole snapshot atomically; For is
// lock-free on the hot path. Before any Store every customer resolves to off. *PolicyHolder satisfies the
// per-message policy resolver the content sealer reads.
type PolicyHolder struct {
	snap atomic.Pointer[PolicySnapshot]
}

// Store atomically swaps in a freshly loaded snapshot.
func (h *PolicyHolder) Store(s *PolicySnapshot) { h.snap.Store(s) }

// For resolves a customer's effective policy against the current snapshot (off until one is stored).
func (h *PolicyHolder) For(customerID uuid.UUID) cp.ContentStorage {
	s := h.snap.Load()
	if s == nil {
		return cp.ContentOff
	}
	return s.For(customerID)
}
