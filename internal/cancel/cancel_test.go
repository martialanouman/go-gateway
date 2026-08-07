package cancel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/cancel"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// fakeReader returns a fixed snapshot, standing in for the scoped ClickHouse reader.
type fakeReader struct {
	row   clickhouse.CDRRow
	found bool
	err   error
}

func (f fakeReader) Current(_ context.Context, _, _, _ uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return f.row, f.found, f.err
}

// fakeWriter records the rows the Canceller appends.
type fakeWriter struct {
	rows []clickhouse.CDRRow
	err  error
}

func (f *fakeWriter) Insert(_ context.Context, row clickhouse.CDRRow) error {
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, row)
	return nil
}

// fakeFlags records the claims, standing in for the Redis cancel-token store. holder is the token
// already in place, if any; HolderNone means the token is free and the Canceller wins it.
type fakeFlags struct {
	holder  cancel.Holder
	claimed []uuid.UUID
	err     error
}

func (f *fakeFlags) Claim(_ context.Context, id uuid.UUID, _ cancel.Holder) (cancel.Holder, error) {
	if f.err != nil {
		return cancel.HolderNone, f.err
	}
	f.claimed = append(f.claimed, id)
	return f.holder, nil
}

func rowWith(status clickhouse.Status) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   "GATEWAY",
		DestAddr:     "+2250700000000",
		SubmittedAt:  time.Now().UTC(),
		Status:       status,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingGSM7,
	}
}

// TestCancelAcceptedMessage pins the happy path: a still-queued (accepted) message is cancelled —
// the intent is flagged in Redis AND a cancelled CDR row (rank 60) is written, and the returned row
// reflects the cancelled state.
func TestCancelAcceptedMessage(t *testing.T) {
	row := rowWith(clickhouse.StatusAccepted)
	reader := fakeReader{row: row, found: true}
	writer := &fakeWriter{}
	flags := &fakeFlags{}
	c := cancel.NewCanceller(reader, writer, flags, nil)

	if err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(flags.claimed) != 1 || flags.claimed[0] != row.MessageID {
		t.Errorf("cancel intent not claimed, claimed = %v", flags.claimed)
	}
	if len(writer.rows) != 1 {
		t.Fatalf("cancelled CDR row not written, rows = %+v", writer.rows)
	}
	got := writer.rows[0]
	if got.Status != clickhouse.StatusCancelled {
		t.Errorf("written status = %q, want cancelled", got.Status)
	}
	if got.ErrorCode != nil {
		t.Errorf("cancelled row must have no error code, got %v", *got.ErrorCode)
	}
	if got.MessageID != row.MessageID || !got.SubmittedAt.Equal(row.SubmittedAt) {
		t.Error("cancelled row must carry the original identifiers and submitted_at")
	}
}

// TestCancelDispatchedMessage pins that a message already sent (enroute or any terminal state) cannot
// be cancelled: ErrCancelFailed, and neither the flag nor a CDR row is written.
func TestCancelDispatchedMessage(t *testing.T) {
	for _, status := range []clickhouse.Status{
		clickhouse.StatusEnroute,
		clickhouse.StatusDelivered,
		clickhouse.StatusFailed,
		clickhouse.StatusExpired,
		clickhouse.StatusRejected,
		clickhouse.StatusRerouted,
	} {
		t.Run(string(status), func(t *testing.T) {
			row := rowWith(status)
			writer := &fakeWriter{}
			flags := &fakeFlags{}
			c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, flags, nil)

			err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID)
			if !errors.Is(err, errs.ErrCancelFailed) {
				t.Errorf("err = %v, want ErrCancelFailed", err)
			}
			if len(flags.claimed) != 0 {
				t.Error("a dispatched message must not be claimed")
			}
			if len(writer.rows) != 0 {
				t.Error("a dispatched message must not write a cancelled CDR row")
			}
		})
	}
}

// TestCancelUnknownMessage pins that a message absent from the caller's scope is ErrMessageNotFound
// (which carries ESME_RINVMSGID on the SMPP boundary), not ErrNotFound (no SMPP surface).
func TestCancelUnknownMessage(t *testing.T) {
	c := cancel.NewCanceller(fakeReader{found: false}, &fakeWriter{}, &fakeFlags{}, nil)

	err := c.Cancel(context.Background(), uuid.New(), uuid.New(), uuid.New())
	if !errors.Is(err, errs.ErrMessageNotFound) {
		t.Errorf("err = %v, want ErrMessageNotFound", err)
	}
}

// TestCancelAlreadyCancelledIsIdempotent pins that re-cancelling an already-cancelled message
// succeeds with the existing row and writes nothing new (natural idempotency, no double flag/write).
func TestCancelAlreadyCancelledIsIdempotent(t *testing.T) {
	row := rowWith(clickhouse.StatusCancelled)
	writer := &fakeWriter{}
	flags := &fakeFlags{}
	c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, flags, nil)

	if err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if len(flags.claimed) != 0 || len(writer.rows) != 0 {
		t.Error("re-cancelling must not re-claim or re-write")
	}
}

// TestCancelLosesTheRaceToTheConnector is the test step-209 exists for.
//
// The Canceller decides cancellability on the CDR projection, which lags: a message already on the
// wire keeps reading `accepted` until mt.outcome is projected (tens of ms in steady state, bounded
// only by the 30s lag alert under saturation). Throughout that window the status check above passes
// for a message that WILL be delivered. The cancel token is the second guard, and it is the one that
// holds: the connector claimed it before submit_sm, so the Canceller loses.
//
// Losing must mean ESME_RCANCELFAIL and, above all, NO CDR ROW. A cancelled row ranks 60, above
// delivered (40) and failed (50), and ReplacingMergeTree keeps the max rank whatever the insertion
// order — so one wrongly written row makes get-message report `cancelled` for ever on a delivered,
// billed message. The row is the damage; the error code is only the symptom.
func TestCancelLosesTheRaceToTheConnector(t *testing.T) {
	row := rowWith(clickhouse.StatusAccepted) // the projection still says "queued"; it is stale
	writer := &fakeWriter{}
	flags := &fakeFlags{holder: cancel.HolderDispatched}
	c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, flags, nil)

	err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID)

	if !errors.Is(err, errs.ErrCancelFailed) {
		t.Errorf("err = %v, want ErrCancelFailed (ESME_RCANCELFAIL) — the message is already gone", err)
	}
	if len(writer.rows) != 0 {
		t.Errorf("a lost race wrote %d CDR row(s), want 0 — a cancelled row (rank 60) would bury "+
			"the enroute and delivered rows that follow, for ever", len(writer.rows))
	}
}

// TestCancelRefusesAnUnknownHolder pins the direction of the unknown case: a token this build cannot
// name is still a token we did NOT win, so the cancel is refused and no row is written.
//
// This is not hypothetical. The token key is the one the previous build used as a plain flag, whose
// value was "1" and whose TTL is 72h. During a rolling deploy a message cancelled just before the
// switch carries exactly that value, and reading it as "free" would let the cancel record a row for a
// message the connector is about to send anyway.
func TestCancelRefusesAnUnknownHolder(t *testing.T) {
	for _, holder := range []cancel.Holder{"1", "something-a-future-build-writes"} {
		t.Run(string(holder), func(t *testing.T) {
			row := rowWith(clickhouse.StatusAccepted)
			writer := &fakeWriter{}
			c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer,
				&fakeFlags{holder: holder}, nil)

			err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID)

			if !errors.Is(err, errs.ErrCancelFailed) {
				t.Errorf("err = %v, want ErrCancelFailed — an unheld token is not a free one", err)
			}
			if len(writer.rows) != 0 {
				t.Errorf("an unknown holder wrote %d row(s), want 0", len(writer.rows))
			}
		})
	}
}

// TestCancelWinsTheRaceAgainstItself pins that a repeat cancel_sm whose CDR projection has not yet
// caught up stays idempotent: the token already reads `cancel`, so it is our own earlier intent, not
// a dispatch. It must succeed (ESME_ROK) and write nothing new.
func TestCancelWinsTheRaceAgainstItself(t *testing.T) {
	row := rowWith(clickhouse.StatusAccepted) // the first cancel's row is not projected yet
	writer := &fakeWriter{}
	flags := &fakeFlags{holder: cancel.HolderCancel}
	c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, flags, nil)

	if err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID); err != nil {
		t.Fatalf("a repeat cancel must stay idempotent, got %v", err)
	}
	if len(writer.rows) != 0 {
		t.Errorf("a repeat cancel wrote %d row(s), want 0", len(writer.rows))
	}
}

// TestCancelReadErrorIsInternal pins that a reader failure surfaces as ErrInternal (a 5xx-class code),
// not a leaked infrastructure error.
func TestCancelReadErrorIsInternal(t *testing.T) {
	c := cancel.NewCanceller(fakeReader{err: errors.New("clickhouse down")}, &fakeWriter{}, &fakeFlags{}, nil)

	err := c.Cancel(context.Background(), uuid.New(), uuid.New(), uuid.New())
	if !errors.Is(err, errs.ErrInternal) {
		t.Errorf("err = %v, want ErrInternal", err)
	}
}

// TestCancelFlagErrorIsInternal pins that a failure to record the intent aborts the cancel with
// ErrInternal and writes no cancelled CDR row (the message is not proven un-dispatched).
func TestCancelFlagErrorIsInternal(t *testing.T) {
	row := rowWith(clickhouse.StatusAccepted)
	writer := &fakeWriter{}
	c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, &fakeFlags{err: errors.New("redis down")}, nil)

	err := c.Cancel(context.Background(), row.CustomerID, row.AccountID, row.MessageID)
	if !errors.Is(err, errs.ErrInternal) {
		t.Errorf("err = %v, want ErrInternal", err)
	}
	if len(writer.rows) != 0 {
		t.Error("a failed flag must not write a cancelled CDR row")
	}
}
