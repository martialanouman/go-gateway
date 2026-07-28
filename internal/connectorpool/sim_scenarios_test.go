package connectorpool_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

// scenarioBreakerConfig opens the breaker after 2 failing submits and keeps it OPEN long enough to
// observe: a 5s cooldown means that once the aggregate opens it stays open (no half-open probe) well past
// the poll deadline, while the response timeout (300ms) stays comfortably below it (the documented
// breaker invariant, breaker.go).
var scenarioBreakerConfig = breaker.Config{
	MinRequests: 2, FailureRate: 1.0, Window: 2 * time.Second, Cooldown: 5 * time.Second,
	HalfOpenProbes: 1, HalfOpenTimeout: 2 * time.Second,
}

// TestSimMultiPodBreakerAggregate is the step-130b cross-pod acceptance scenario (plan §12): two
// connector-pool pods bind the SAME degraded connector; each pod's local breaker opens on its own
// timed-out submits, and the Redis aggregate opens only once a strict majority of the pods' live
// sub-binds are open — proving the breaker aggregate is correct with binds spread across pods, not just
// within one process. The messages carry a throwaway fallback chain purely so a timed-out submit
// reroutes (committing the record) and the pool keeps consuming rather than dying on the first failure.
func TestSimMultiPodBreakerAggregate(t *testing.T) {
	rdb := redistest.Client(t)
	deadSim := smscsim.Launch(t, smscsim.DeadCarrierConfig("dead", "pw"))

	connID := uuid.New()
	throwaway := uuid.New() // an unbound reroute target: keeps the pools alive without a live consumer

	base := simPoolConfig{
		BindAddr: deadSim.SMPPAddr, SystemID: "dead", Password: "pw", ConnID: connID,
		BreakerConfig: scenarioBreakerConfig, ResponseTimeout: 300 * time.Millisecond, Redis: rdb,
	}
	cfgA, cfgB := base, base
	cfgA.PodID, cfgB.PodID = "pod-a", "pod-b"
	poolA := startSimPool(t, cfgA)
	_ = startSimPool(t, cfgB)

	// Both pods must have bound the simulator AND joined the shared group (2 members, partitions split)
	// before injecting, so both feed their local breaker — a strict majority needs BOTH sub-binds open.
	deadSim.WaitBindCount(t, "carrier", 2, 20*time.Second)
	waitGroupStable(t, poolA.group, 2, 20*time.Second)

	// 32 records (random message_id keys) spread across the 4 partitions split between the two pods, so
	// each pod comfortably clears MinRequests=2: P(a pod gets <2 of 32) is ~1e-8, not the ~3e-4 that 16
	// would leave — deterministic enough that a single skewed split cannot flake the strict-majority open.
	for i := 0; i < 32; i++ {
		poolA.injectRoutedTo(t, connID, []uuid.UUID{throwaway})
	}

	waitBreakerState(t, rdb, connID, breaker.Open, 25*time.Second)
}
