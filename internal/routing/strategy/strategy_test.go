package strategy_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/strategy"
)

func twoTargets(wa, wb int) (uuid.UUID, uuid.UUID, []strategy.Target) {
	a, b := uuid.New(), uuid.New()
	return a, b, []strategy.Target{{ConnectorID: a, Weight: wa}, {ConnectorID: b, Weight: wb}}
}

// TestWeightedIsDeterministicAndProportional: the same key always maps to the same connector, and over
// many uniformly-distributed keys a 70/30 weight split lands ~70/30 (statistical acceptance criterion).
func TestWeightedIsDeterministicAndProportional(t *testing.T) {
	a, b, targets := twoTargets(70, 30)

	// Determinism: the same key resolves identically.
	got1, _ := strategy.Weighted(targets, "2250700000001")
	got2, _ := strategy.Weighted(targets, "2250700000001")
	if got1 != got2 {
		t.Fatalf("weighted not deterministic: %s != %s", got1, got2)
	}

	const n = 20000
	counts := map[uuid.UUID]int{}
	for i := 0; i < n; i++ {
		conn, ok := strategy.Weighted(targets, fmt.Sprintf("+22507%08d", i))
		if !ok {
			t.Fatal("weighted returned no connector")
		}
		counts[conn]++
	}
	shareA := float64(counts[a]) / float64(n)
	if shareA < 0.66 || shareA > 0.74 {
		t.Errorf("connector A share = %.3f, want ~0.70 (±0.04); A=%d B=%d", shareA, counts[a], counts[b])
	}
}

// TestHashBasedStablePerKey: a key always maps to the same connector, and both connectors are reachable
// across keys.
func TestHashBasedStablePerKey(t *testing.T) {
	_, _, targets := twoTargets(1, 1)
	seen := map[uuid.UUID]bool{}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("+22507%08d", i)
		first, _ := strategy.HashBased(targets, key)
		again, _ := strategy.HashBased(targets, key)
		if first != again {
			t.Fatalf("hash_based not stable for %s", key)
		}
		seen[first] = true
	}
	if len(seen) < 2 {
		t.Errorf("hash_based used %d connectors, want both reachable", len(seen))
	}
}

// TestRoundRobinRotates: successive counters rotate through the targets in order.
func TestRoundRobinRotates(t *testing.T) {
	a, b, targets := twoTargets(1, 1)
	// targets order as given: index 0 = a, 1 = b.
	if c, _ := strategy.RoundRobin(targets, 0); c != a {
		t.Errorf("n=0 -> %s, want a", c)
	}
	if c, _ := strategy.RoundRobin(targets, 1); c != b {
		t.Errorf("n=1 -> %s, want b", c)
	}
	if c, _ := strategy.RoundRobin(targets, 2); c != a {
		t.Errorf("n=2 -> %s, want a (wrap)", c)
	}
}

// TestFailoverPriority: picks the lowest priority number (evaluated first).
func TestFailoverPriority(t *testing.T) {
	primary, secondary := uuid.New(), uuid.New()
	targets := []strategy.Target{{ConnectorID: secondary, Priority: 5}, {ConnectorID: primary, Priority: 1}}
	if got, _ := strategy.FailoverPriority(targets); got != primary {
		t.Errorf("failover -> %s, want the priority-1 %s", got, primary)
	}
}

// TestLeastLoaded: picks the connector with the smallest load; a missing gauge reads 0.
func TestLeastLoaded(t *testing.T) {
	busy, idle := uuid.New(), uuid.New()
	targets := []strategy.Target{{ConnectorID: busy}, {ConnectorID: idle}}
	load := map[uuid.UUID]int{busy: 50, idle: 2}
	if got, _ := strategy.LeastLoaded(targets, func(id uuid.UUID) int { return load[id] }); got != idle {
		t.Errorf("least_loaded -> %s, want the idle %s", got, idle)
	}
}

// TestEmptyTargets: every strategy reports ok=false with no targets.
func TestEmptyTargets(t *testing.T) {
	if _, ok := strategy.Weighted(nil, "k"); ok {
		t.Error("Weighted(nil) ok=true, want false")
	}
	if _, ok := strategy.HashBased(nil, "k"); ok {
		t.Error("HashBased(nil) ok=true, want false")
	}
	if _, ok := strategy.RoundRobin(nil, 0); ok {
		t.Error("RoundRobin(nil) ok=true, want false")
	}
	if _, ok := strategy.FailoverPriority(nil); ok {
		t.Error("FailoverPriority(nil) ok=true, want false")
	}
	if _, ok := strategy.LeastLoaded(nil, func(uuid.UUID) int { return 0 }); ok {
		t.Error("LeastLoaded(nil) ok=true, want false")
	}
}

func TestFailoverPriorityChainOrdersByPriority(t *testing.T) {
	a := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	b := uuid.MustParse("00000000-0000-0000-0000-0000000000bb")
	c := uuid.MustParse("00000000-0000-0000-0000-0000000000cc")
	targets := []strategy.Target{
		{ConnectorID: c, Priority: 3},
		{ConnectorID: a, Priority: 1},
		{ConnectorID: b, Priority: 2},
	}
	got := strategy.FailoverPriorityChain(targets)
	want := []uuid.UUID{a, b, c}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("chain = %v, want %v (priority order)", got, want)
	}
	// The head matches the single-pick strategy.
	if head, _ := strategy.FailoverPriority(targets); head != got[0] {
		t.Errorf("chain head %v != FailoverPriority pick %v", got[0], head)
	}
}

func TestLeastLoadedChainOrdersByLoad(t *testing.T) {
	busy := uuid.MustParse("00000000-0000-0000-0000-0000000000b1")
	idle := uuid.MustParse("00000000-0000-0000-0000-0000000000d1")
	mid := uuid.MustParse("00000000-0000-0000-0000-0000000000c1")
	targets := []strategy.Target{{ConnectorID: busy}, {ConnectorID: idle}, {ConnectorID: mid}}
	load := map[uuid.UUID]int{busy: 100, mid: 10, idle: 1}
	got := strategy.LeastLoadedChain(targets, func(id uuid.UUID) int { return load[id] })
	want := []uuid.UUID{idle, mid, busy}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("chain[%d] = %v, want %v (load order)", i, got[i], want[i])
		}
	}
}
