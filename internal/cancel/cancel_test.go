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

// fakeFlags records the marked ids, standing in for the Redis flag store.
type fakeFlags struct {
	marked []uuid.UUID
	err    error
}

func (f *fakeFlags) Mark(_ context.Context, id uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.marked = append(f.marked, id)
	return nil
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
	if len(flags.marked) != 1 || flags.marked[0] != row.MessageID {
		t.Errorf("cancel intent not flagged, marked = %v", flags.marked)
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
			if len(flags.marked) != 0 {
				t.Error("a dispatched message must not be flagged")
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
	if len(flags.marked) != 0 || len(writer.rows) != 0 {
		t.Error("re-cancelling must not re-flag or re-write")
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
