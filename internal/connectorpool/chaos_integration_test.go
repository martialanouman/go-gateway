package connectorpool_test

import (
	"context"
	"testing"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestConnectorDispatchesWhenTheCancelTokenStoreIsCut is the step-250d acceptance test for the POOL half
// of the seventh row of the failure-policy matrix (guide de codage §16): "Redis (jeton d'annulation) ->
// asymétrique : ... fail-open côté pool (journalise et envoie)". Its mirror is
// TestCancelFailsClosedWhenRedisIsCut in internal/cancel: the same outage, the opposite verdict. Proving
// only one side proves half a policy, and the half that is left out is the one that could quietly become
// the other (ADR-0013).
//
// This side needs bracketing, because on its own the outage assertion is close to a tautology: the pool
// dispatches whether the token store answers "free" or fails, so "the message went out" is true either
// way. What the control and the recovery add is that the store is genuinely in the path — it takes the
// dispatched token when it can, and it still forbids a cancelled message when it comes back. Without
// those two, this test would pass against a connector that had stopped consulting the store at all.
func TestConnectorDispatchesWhenTheCancelTokenStoreIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	// Control, with Redis up: the message goes out AND the store records that it did. The second half is
	// the one that matters — it is what makes the outage phase an absence rather than a no-op.
	up := routed()
	_, submitted, err := runWithFlags(t, flags, up)
	if err != nil {
		t.Fatalf("with redis up Run: %v", err)
	}
	if !*submitted {
		t.Fatal("with redis up the message must be submitted")
	}
	if holder, err := flags.Peek(ctx, up.MessageID); err != nil || holder != cancel.HolderDispatched {
		t.Fatalf("token after the control dispatch = (%q, %v), want %q: the pool must claim the token "+
			"before putting anything on the wire (ADR-0013)", holder, err, cancel.HolderDispatched)
	}

	proxy.Cut()

	// The outage: cancellation is best-effort, delivery is not. An unreachable token store must not
	// stall the backlog — the pool logs and dispatches. Run returns nil, so the offset is committed and
	// the message is not redelivered into a second submit.
	during := routed()
	sink, submitted, err := runWithFlags(t, flags, during)
	if err != nil {
		t.Fatalf("with redis cut Run = %v, want nil: a cancel-token outage must not halt the pool nor "+
			"redeliver the record into a duplicate submit", err)
	}
	if !*submitted {
		t.Error("fail-open: the message must still reach the SMSC when the cancel token cannot be claimed")
	}
	if got := sink.outcome(t).Status; got != string(clickhouse.StatusEnroute) {
		t.Errorf("outcome status = %q, want enroute", got)
	}

	proxy.Resume()

	// The claim really did not land — the pool dispatched on an unheld token, which is precisely the
	// residual ADR-0013 accepts and bounds: a cancel_sm arriving now can still win it.
	if holder, err := flags.Peek(ctx, during.MessageID); err != nil || holder != cancel.HolderNone {
		t.Errorf("token after the outage dispatch = (%q, %v), want free: the claim failed, so nothing "+
			"should have been written", holder, err)
	}

	// Recovery, and the assertion that keeps this test honest: a message whose token is held by a
	// cancellation must NOT be dispatched. Fail-open is a behaviour under fault, not a licence to stop
	// consulting the store — one that latched would silently deliver every cancelled message from the
	// first Redis blip onwards.
	cancelled := routed()
	if _, err := flags.Claim(ctx, cancelled.MessageID, cancel.HolderCancel); err != nil {
		t.Fatalf("claim the cancellation token: %v", err)
	}
	sink, submitted, err = runWithFlags(t, flags, cancelled)
	if err != nil {
		t.Fatalf("after redis came back Run: %v", err)
	}
	if *submitted {
		t.Error("after recovery a cancelled message was still submitted: the pool is no longer honouring " +
			"the token store, so the fail-open latched")
	}
	if rows := sink.rows(); len(rows) != 1 || rows[0].Status != clickhouse.StatusCancelled {
		t.Errorf("after recovery the connector must record the cancellation itself, got %+v", rows)
	}
}
