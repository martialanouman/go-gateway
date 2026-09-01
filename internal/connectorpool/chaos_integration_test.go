package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
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

// TestExpiredCancellationIsMisfiledWhenTheCancelTokenStoreIsCut covers the SECOND fail-open site of the
// seventh row of the failure-policy matrix (guide de codage §16). The row names two, and until now only
// the first was proven on a real outage: TestConnectorDispatchesWhenTheCancelTokenStoreIsCut goes
// through the Claim before a submit (connectorpool.go:927). This one goes through the Peek on the
// max-age expiry branch (connectorpool.go:900), whose only coverage was a fake returning an error
// (deadletter_test.go, "peek fails") — the very kind of stand-in step-250d exists to replace.
//
// What it pins is a LOSS, not a safeguard, and §16 now says so: with the token store unreachable, a
// message the customer cancelled is dead-lettered as delivery_expired instead of being recorded as
// cancelled. That is the accepted price of failing open here (the message was not going out either way,
// and halting the backlog on a Redis fault is worse), but it is a real misfiling and it belongs in a
// test rather than in a sentence nobody can run.
//
// The cancellation token is what keeps this from being a tautology: an expired message is parked
// whether or not the store answers, so only a message that IS cancelled can tell the two apart. The
// control puts one in and watches it come out cancelled; the outage phase does exactly the same thing
// and watches it come out expired. The single difference between them is the cut.
func TestExpiredCancellationIsMisfiledWhenTheCancelTokenStoreIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	// An already-expired message: two hours old against the one-hour SLA wired below.
	expired := func() pipeline.RoutedMT {
		r := routed()
		r.SubmittedAt = time.Now().Add(-2 * time.Hour).UTC()
		return r
	}

	// cancelled marks a message as cancelled in the token store, the way a cancel_sm would. It must be
	// called BEFORE the cut: during the outage nothing can be written, and a message with no token would
	// be parked on the merits rather than for want of an answer.
	cancelled := func(t *testing.T, r pipeline.RoutedMT) pipeline.RoutedMT {
		t.Helper()
		if _, err := flags.Claim(ctx, r.MessageID, cancel.HolderCancel); err != nil {
			t.Fatalf("claim the cancellation token: %v", err)
		}
		return r
	}

	// runExpired drives one record through a pool with a one-hour max-age SLA and reports what became of
	// it. deadLetterDeps fails the test if Run returns an error, which is also how a fail-open turned
	// fail-closed would be caught here: an unreachable Peek must never leave the offset uncommitted.
	runExpired := func(t *testing.T, r pipeline.RoutedMT) (parked int, status clickhouse.Status) {
		t.Helper()
		rec, err := pipeline.EncodeRouted(r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		prod, cdr, _ := deadLetterDeps(t, &fakeConsumer{records: []kafka.Record{rec}},
			func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0, time.Hour, flags)
		for _, got := range prod.records() {
			if got.Topic == kafka.TopicMTDeadLetter {
				parked++
			}
		}
		if len(cdr.rows) != 1 {
			t.Fatalf("wrote %d cdr rows, want exactly one", len(cdr.rows))
		}
		return parked, cdr.rows[0].Status
	}

	// Control, with Redis up: the expiry branch reads the token, finds a cancellation, and records it as
	// such. Without this the outage phase below could not distinguish a misfiling from a branch that
	// parks everything regardless.
	if parked, status := runExpired(t, cancelled(t, expired())); parked != 0 || status != clickhouse.StatusCancelled {
		t.Fatalf("with redis up an expired-but-cancelled message was parked=%d status=%q, want 0 and "+
			"cancelled: the control never exercised the token read, so the outage phase would prove nothing",
			parked, status)
	}

	during := cancelled(t, expired())
	proxy.Cut()

	// The outage. Fail-open: the pool does not stall on an unreachable token store, and the message is
	// dead-lettered as delivery_expired — a cancellation recorded as an expiry. The loss §16 documents.
	parked, status := runExpired(t, during)
	if parked != 1 {
		t.Errorf("parked %d records during the outage, want 1: a cancel-token outage must not stop the "+
			"expiry branch from clearing its backlog", parked)
	}
	if status != clickhouse.StatusFailed {
		t.Errorf("cdr status during the outage = %q, want %q: with the token unreadable the branch fails "+
			"open and files the message as expired", status, clickhouse.StatusFailed)
	}

	proxy.Resume()

	// Recovery, and the assertion that keeps the fail-open honest: the branch consults the store again.
	// One that latched would file every cancelled message as expired from the first Redis blip onwards,
	// and the misfiling would stop being bounded by the outage.
	if parked, status := runExpired(t, cancelled(t, expired())); parked != 0 || status != clickhouse.StatusCancelled {
		t.Errorf("after redis came back an expired-but-cancelled message was parked=%d status=%q, want 0 "+
			"and cancelled: the expiry branch is no longer reading the token store, so the fail-open latched",
			parked, status)
	}
}
