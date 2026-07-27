// Package strategy selects a connector among a route's targets for a non-static distribution strategy
// (schema §11). weighted and hash_based are deterministic (the same key always maps to the same
// connector, so all segments of a message go to one connector, §7.3); round_robin uses a caller-owned
// monotonic counter from the mutable overlay (never the immutable snapshot, step-104). failover_priority
// and least_loaded arrive in step-114.
package strategy

import (
	"hash/fnv"
	"sort"

	"github.com/google/uuid"
)

// Target is a compiled route target: a connector with its weight (weighted) and priority
// (failover_priority, step-114).
type Target struct {
	ConnectorID uuid.UUID
	Weight      int
	Priority    int
}

// Weighted picks a connector by cumulative weight, deterministically from key: the same key always
// yields the same connector, and over uniformly-distributed keys the split matches the weights (e.g.
// 70/30). ok is false when there are no targets or the total weight is non-positive.
func Weighted(targets []Target, key string) (uuid.UUID, bool) {
	total := 0
	for _, t := range targets {
		total += t.Weight
	}
	if total <= 0 {
		return uuid.Nil, false
	}
	pos := int(hash(key) % uint64(total)) //nolint:gosec // total>0 bounds the modulo to a small int
	for _, t := range targets {
		pos -= t.Weight
		if pos < 0 {
			return t.ConnectorID, true
		}
	}
	return targets[len(targets)-1].ConnectorID, true // unreachable rounding guard
}

// HashBased picks a connector by hash(key) % len(targets): a given key (e.g. the destination) always
// maps to the same connector, ignoring weight. ok is false when there are no targets.
func HashBased(targets []Target, key string) (uuid.UUID, bool) {
	if len(targets) == 0 {
		return uuid.Nil, false
	}
	idx := hash(key) % uint64(len(targets))
	return targets[idx].ConnectorID, true
}

// RoundRobin picks targets[n % len]. n is the caller's monotonic counter (from the mutable overlay), so
// successive calls rotate evenly. ok is false when there are no targets.
func RoundRobin(targets []Target, n uint64) (uuid.UUID, bool) {
	if len(targets) == 0 {
		return uuid.Nil, false
	}
	return targets[n%uint64(len(targets))].ConnectorID, true
}

// FailoverPriority picks the target with the lowest priority number (evaluated first). In M7 every
// target is available (no circuit breaker yet, step-123), so it is deterministic — the primary target.
// Ties break on connector id for stability. ok is false when there are no targets.
func FailoverPriority(targets []Target) (uuid.UUID, bool) {
	if len(targets) == 0 {
		return uuid.Nil, false
	}
	best := targets[0]
	for _, t := range targets[1:] {
		if t.Priority < best.Priority || (t.Priority == best.Priority && t.ConnectorID.String() < best.ConnectorID.String()) {
			best = t
		}
	}
	return best.ConnectorID, true
}

// LeastLoaded picks the target with the smallest in-flight gauge (connectorload:{id}), read via loadOf
// (a missing gauge reads 0). Ties break on connector id for stability. ok is false when there are no
// targets. loadOf must not mutate state (it reads a published Redis gauge, no Go read-modify-write).
func LeastLoaded(targets []Target, loadOf func(uuid.UUID) int) (uuid.UUID, bool) {
	if len(targets) == 0 {
		return uuid.Nil, false
	}
	best := targets[0]
	bestLoad := loadOf(best.ConnectorID)
	for _, t := range targets[1:] {
		load := loadOf(t.ConnectorID)
		if load < bestLoad || (load == bestLoad && t.ConnectorID.String() < best.ConnectorID.String()) {
			best, bestLoad = t, load
		}
	}
	return best.ConnectorID, true
}

// FailoverPriorityChain returns every target's connector ordered by priority (lowest first), ties on
// connector id — the fallback order the connector pool follows when a target degrades (step-125). The
// head equals FailoverPriority's pick, so the chain is a superset that starts at the primary.
func FailoverPriorityChain(targets []Target) []uuid.UUID {
	ordered := append([]Target(nil), targets...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority < ordered[j].Priority
		}
		return ordered[i].ConnectorID.String() < ordered[j].ConnectorID.String()
	})
	return connectorIDs(ordered)
}

// LeastLoadedChain returns every target's connector ordered by in-flight load (lowest first), ties on
// connector id — the reroute fallback order for a least_loaded route (step-125). loadOf must not mutate
// state (it reads a published Redis gauge). The head equals LeastLoaded's pick.
func LeastLoadedChain(targets []Target, loadOf func(uuid.UUID) int) []uuid.UUID {
	ordered := append([]Target(nil), targets...)
	load := make(map[uuid.UUID]int, len(ordered))
	for _, t := range ordered {
		load[t.ConnectorID] = loadOf(t.ConnectorID)
	}
	sort.Slice(ordered, func(i, j int) bool {
		li, lj := load[ordered[i].ConnectorID], load[ordered[j].ConnectorID]
		if li != lj {
			return li < lj
		}
		return ordered[i].ConnectorID.String() < ordered[j].ConnectorID.String()
	})
	return connectorIDs(ordered)
}

// connectorIDs projects a target slice to its connector ids, in order.
func connectorIDs(targets []Target) []uuid.UUID {
	ids := make([]uuid.UUID, len(targets))
	for i, t := range targets {
		ids[i] = t.ConnectorID
	}
	return ids
}

// hash is a stable non-cryptographic digest of key. FNV-1a is fast and well-distributed; routing keys
// are not adversarial, so collision resistance is not required.
func hash(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
