// Package optout is the opt-out (suppression) foundation on the MT critical path (§6.20). A message
// must not be delivered to a destination suppressed in any applicable scope. To keep the check off
// the database for the overwhelming majority of (non-suppressed) traffic, membership is tested first
// against an in-memory Bloom filter per scope, loaded once at startup; only a Bloom hit is confirmed
// exactly against the source of truth. The blocking pipeline stage that consumes this lands in
// step-062; STOP-driven writes and hot reload arrive later (step-063, M7).
package optout

import (
	"context"
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// Tuning for each scope's Bloom filter. A false positive only costs one exact confirmation query, so
// a low rate keeps the database out of the hot path without over-sizing memory. The snapshot is
// immutable and rebuilt from the current count at each boot, so the 2x factor is pure headroom today;
// it becomes load-bearing when live STOP writes land (step-063), since bloom.NewWithEstimates
// degrades its false-positive rate sharply once the actual count exceeds the estimate.
const (
	falsePositiveRate = 0.001
	safetyFactor      = 2
	minCapacity       = 1024
)

// SuppressionLister loads every suppression to seed the Bloom snapshot. *postgres.SuppressionRepo
// satisfies it structurally.
type SuppressionLister interface {
	ListSuppressions(ctx context.Context) ([]cp.Suppression, error)
}

// ExactChecker confirms a Bloom hit against the source of truth. *postgres.SuppressionRepo satisfies
// it structurally.
type ExactChecker interface {
	IsSuppressed(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error)
}

// Snapshot holds one immutable Bloom filter per scope, seeded once at startup. It is safe for
// concurrent reads: nothing mutates a filter after LoadSnapshot, so MightBeSuppressed is lock-free.
type Snapshot struct {
	filters map[cp.SuppressionScope]*bloom.BloomFilter
}

// LoadSnapshot reads every suppression once and builds a per-scope Bloom filter. Each filter is sized
// from that scope's actual entry count (with a safety factor), so a scope with no suppressions holds
// no filter and always answers "not suppressed".
func LoadSnapshot(ctx context.Context, lister SuppressionLister) (*Snapshot, error) {
	all, err := lister.ListSuppressions(ctx)
	if err != nil {
		return nil, fmt.Errorf("optout: load suppressions: %w", err)
	}

	byScope := make(map[cp.SuppressionScope][]cp.Suppression)
	for _, s := range all {
		byScope[s.Scope] = append(byScope[s.Scope], s)
	}

	filters := make(map[cp.SuppressionScope]*bloom.BloomFilter, len(byScope))
	for scope, rows := range byScope {
		f := bloom.NewWithEstimates(capacityFor(len(rows)), falsePositiveRate)
		for _, s := range rows {
			f.AddString(key(scope, s.ScopeID, s.MSISDN))
		}
		filters[scope] = f
	}
	return &Snapshot{filters: filters}, nil
}

// MightBeSuppressed reports whether msisdn MAY be suppressed in the given scope. A false answer is
// definitive (the Bloom property guarantees no false negatives): the message is not suppressed in
// this scope. A true answer must be confirmed with an exact check — the Bloom admits false positives.
// msisdn must already be E.164-normalized (the form internal/platform/e164 produces, as the pipeline
// destination is); scopeID is nil for the platform scope.
func (s *Snapshot) MightBeSuppressed(scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) bool {
	f := s.filters[scope]
	if f == nil {
		return false
	}
	return f.TestString(key(scope, scopeID, msisdn))
}

// Guard is the two-stage opt-out check: the in-memory Bloom snapshot gates the exact confirmation, so
// the database is consulted only when a destination might be suppressed.
type Guard struct {
	snapshot *Snapshot
	exact    ExactChecker
}

// NewGuard composes a Bloom snapshot with the exact checker behind it.
func NewGuard(snapshot *Snapshot, exact ExactChecker) *Guard {
	return &Guard{snapshot: snapshot, exact: exact}
}

// IsSuppressed reports whether msisdn is suppressed in the given scope. It short-circuits to false
// when the Bloom says the destination is definitely absent (no database hit), and confirms exactly
// otherwise. An error from the exact check is returned for the caller to treat as transient. msisdn
// must be E.164-normalized; scopeID is nil for the platform scope.
func (g *Guard) IsSuppressed(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error) {
	if !g.snapshot.MightBeSuppressed(scope, scopeID, msisdn) {
		return false, nil
	}
	return g.exact.IsSuppressed(ctx, scope, scopeID, msisdn)
}

// key builds a scope-local Bloom/confirmation key. The platform scope has no scope_id, so its key is
// the bare MSISDN; every other scope namespaces the MSISDN by its scope entity so two customers'
// suppressions of the same number never collide.
func key(scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) string {
	if scope == cp.SuppressionScopePlatform || scopeID == nil {
		return msisdn
	}
	return scopeID.String() + "|" + msisdn
}

// capacityFor returns the Bloom capacity to request for a scope holding n entries: n with a safety
// factor, floored so a tiny scope is not sized down to a high false-positive rate.
func capacityFor(n int) uint {
	c := n * safetyFactor
	if c < minCapacity {
		c = minCapacity
	}
	return uint(c)
}
