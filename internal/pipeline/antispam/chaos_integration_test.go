package antispam_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestAntispamFlagsInsteadOfBlockingWhenRedisIsCut is the step-250 acceptance test for the second row
// of the failure-policy matrix (guide de codage §16): a stateful anti-spam check whose Redis is gone
// FAILS OPEN with a flag, while the static content rules stay in force.
//
// TestFailOpenOnRedisFault already covers this shape with a fake whose every operation errors. That
// fake cannot show the one thing that matters most here: that the rule was GENUINELY BEING ENFORCED
// before the outage. A store that never worked and a store that stopped working are indistinguishable
// to it, so it cannot tell fail-open from a rule that was silently inert all along. This test runs the
// real RedisState against real Redis, blocks a duplicate for real, and only then cuts the link.
//
// Both halves of the policy are exercised in one engine, deliberately. "Fail-open" alone would pass
// under an engine that had simply stopped evaluating anything — so the same cut must leave the static
// content rule still blocking. A fixture that asserted only the first half would be blind to the
// degradation that actually matters: an outage that disables anti-spam wholesale.
func TestAntispamFlagsInsteadOfBlockingWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	metric := &fakeMetric{}
	e := engineWith(t, antispam.NewRedisState(rdb), metric,
		dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 60),
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)spam`),
	)

	ctx := context.Background()
	account, customer := uuid.New(), uuid.New()
	dest := "2250700000001"
	body := []byte("a message repeated twice " + uuid.NewString())
	eval := func(b []byte) cp.AntispamAction {
		t.Helper()
		action, err := e.Evaluate(ctx, account, customer, src, dest, b)
		if err != nil {
			t.Fatalf("Evaluate must never error: %v", err)
		}
		return action
	}

	// Control, with Redis up: the first send is clean, the identical second is a duplicate and is
	// BLOCKED by the Redis-backed rule. Without this the outage half proves nothing — a rule that never
	// worked would also "let the message through" once Redis died.
	if action := eval(body); action != "" {
		t.Fatalf("first send action = %q, want none", action)
	}
	if action := eval(body); action != cp.AntispamActionBlock {
		t.Fatalf("with redis up the duplicate rule must BLOCK, got %q — the control failed, so the "+
			"outage assertions below would be meaningless", action)
	}

	proxy.Cut()

	// The same duplicate, now that the store is gone: it must PASS, flagged — availability over
	// precision — and the fail-open must be counted so the degraded mode is observable.
	before := metric.failOpens
	if action := eval(body); action != cp.AntispamActionFlag {
		t.Errorf("with redis cut the duplicate action = %q, want flag: a state-backed rule must fail "+
			"OPEN, never block", action)
	}
	if metric.failOpens == before {
		t.Error("a fail-open under a real outage must be counted, or the degraded mode is invisible")
	}

	// The other half of the policy: static content rules are in memory and owe Redis nothing. If the
	// outage silenced them too, an outage would become a hole in anti-spam rather than a degradation.
	if action := eval([]byte("buy SPAM now")); action != cp.AntispamActionBlock {
		t.Errorf("with redis cut the content action = %q, want block: static content rules must stay "+
			"in force through an outage", action)
	}

	proxy.Resume()

	// Recovery: the state-backed rule blocks again. A fail-open that latched would leave dedup off long
	// after Redis returned.
	if action := eval(body); action != cp.AntispamActionBlock {
		t.Errorf("after redis came back the duplicate action = %q, want block", action)
	}
}
