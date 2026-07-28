package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// waitGroupStable blocks until the consumer group has reached the Stable state with at least minMembers
// members joined, or fails at the deadline. The resilience pools consume FromLatest (so they never churn
// through the whole shared mt.routed history other tests left behind — the AtStart backlog problem); a
// record produced before the group has stabilised would then be missed. Waiting for Stable before the
// first inject closes that race deterministically (Fable's P1), where a fixed sleep never could.
func waitGroupStable(t *testing.T, group string, minMembers int, within time.Duration) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkatest.Brokers(t)...))
	if err != nil {
		t.Fatalf("kadm client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	ctx := context.Background()
	deadline := time.Now().Add(within)
	lastState, lastMembers := "(none)", 0
	for time.Now().Before(deadline) {
		described, err := adm.DescribeGroups(ctx, group)
		if err == nil {
			if g, ok := described[group]; ok {
				lastState, lastMembers = g.State, len(g.Members)
				if g.State == "Stable" && len(g.Members) >= minMembers {
					return
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("group %q never reached Stable with >= %d members within %s (last: state=%q members=%d)",
		group, minMembers, within, lastState, lastMembers)
}
