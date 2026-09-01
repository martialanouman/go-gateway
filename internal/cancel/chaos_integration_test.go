package cancel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/martialanouman/go-gateway/internal/cancel"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestCancelFailsClosedWhenRedisIsCut is the step-250d acceptance test for the SMPP half of the seventh
// row of the failure-policy matrix (guide de codage §16): "Redis (jeton d'annulation) -> asymétrique :
// fail-closed côté SMPP (refuse l'annulation)". Its mirror, the pool half, is
// TestConnectorDispatchesWhenTheCancelTokenStoreIsCut — the same outage, the opposite verdict, which is
// what makes the policy asymmetric rather than merely inconsistent.
//
// What stood in for this was a fake whose Claim returned errors.New("redis down") (cancel_test.go:250).
// It could never reach the discrimination that matters: RedisFlags.Claim reads a SET ... NX GET whose
// "token was free" answer IS an error — goredis.Nil (flags.go:96). If a real socket fault ever came
// back looking like that Nil, the case ordering would hand the Canceller a free token and it would
// record a cancellation of a message the connector may already have put on the wire (ADR-0013). A fake
// that short-circuits Claim entirely cannot tell those two apart; a dead socket can.
func TestCancelFailsClosedWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	// Control, with Redis up: a still-queued message cancels, and the cancelled row is written. Without
	// it the outage half could not distinguish fail-closed from a harness that never worked.
	up := rowWith(clickhouse.StatusAccepted)
	upWriter := &fakeWriter{}
	if err := cancel.NewCanceller(fakeReader{row: up, found: true}, upWriter, flags, nil).
		Cancel(ctx, up.CustomerID, up.AccountID, up.MessageID); err != nil {
		t.Fatalf("with redis up the cancel must succeed: %v", err)
	}
	if len(upWriter.rows) != 1 {
		t.Fatalf("cancelled rows written with redis up = %d, want 1", len(upWriter.rows))
	}

	proxy.Cut()

	during := rowWith(clickhouse.StatusAccepted)
	writer := &fakeWriter{}
	err := cancel.NewCanceller(fakeReader{row: during, found: true}, writer, flags, nil).
		Cancel(ctx, during.CustomerID, during.AccountID, during.MessageID)
	if err == nil {
		t.Fatal("with redis cut the cancel returned nil: an unverifiable token is not a free token, and " +
			"conceding one records a cancellation the connector may already have contradicted")
	}

	// The assertion that carries the invariant. A cancelled CDR row is a claim about the world: this
	// message did not go out. With the token store unreachable, nothing establishes that — the connector
	// may hold the dispatched token and be on the wire right now. Writing the row anyway is the exact
	// bug ADR-0013 exists to close, and it is invisible to any assertion that only checks err != nil.
	if len(writer.rows) != 0 {
		t.Errorf("a cancel refused during the outage still wrote %d cancelled CDR row(s): the message is "+
			"not proven un-dispatched, so the row is a claim nothing backs", len(writer.rows))
	}

	// The camp matters as much as the refusal. ErrInternal maps to ESME_RSYSERR (retryable: the ESME may
	// try again once Redis is back), while ErrCancelFailed maps to ESME_RCANCELFAIL — "this message has
	// left the queue", a permanent no. Those are opposite instructions to the ESME, and a Redis outage
	// is squarely the first: the answer is unknown, not negative.
	if !errors.Is(err, errs.ErrInternal) {
		t.Errorf("outage error = %v, want ErrInternal: a Redis fault is an unknown answer, not a refusal "+
			"on the merits", err)
	}
	if got := errs.SMPPStatusForError(err); got != errs.StatusSysErr {
		t.Errorf("command_status = %#x, want %#x (ESME_RSYSERR): telling the ESME ESME_RCANCELFAIL would "+
			"claim the message had already left, which the outage cannot establish", got, errs.StatusSysErr)
	}

	proxy.Resume()

	// Recovery: the retry the ESME was invited to make now goes through, on the very message the outage
	// refused. A store that latched would refuse cancellations long after Redis came back.
	recovered := &fakeWriter{}
	if err := cancel.NewCanceller(fakeReader{row: during, found: true}, recovered, flags, nil).
		Cancel(ctx, during.CustomerID, during.AccountID, during.MessageID); err != nil {
		t.Fatalf("after redis came back the retried cancel must succeed: %v", err)
	}
	if len(recovered.rows) != 1 {
		t.Errorf("cancelled rows after recovery = %d, want 1", len(recovered.rows))
	}
}
