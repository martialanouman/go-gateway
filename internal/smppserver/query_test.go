package smppserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// scopedReader serves rows only to the (customer, account) that owns them, like CDRReader.Current: a
// reader that ignored the scope would leak another account's message and fail the cross-account case.
type scopedReader struct {
	rows     map[[3]uuid.UUID]clickhouse.CDRRow
	err      error
	deadline bool
}

func (r *scopedReader) Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error) {
	_, r.deadline = ctx.Deadline()
	if r.err != nil {
		return clickhouse.CDRRow{}, false, r.err
	}
	row, ok := r.rows[[3]uuid.UUID{customerID, accountID, messageID}]
	return row, ok, nil
}

func TestOnQueryResolvesTheMessageState(t *testing.T) {
	st := &connState{querySMEnabled: true, customerID: uuid.New(), accountID: uuid.New()}
	msgID := uuid.New()
	deliveredAt := time.Date(2026, 9, 27, 14, 5, 9, 700_000_000, time.UTC)
	reader := &scopedReader{rows: map[[3]uuid.UUID]clickhouse.CDRRow{
		{st.customerID, st.accountID, msgID}: {MessageID: msgID, Status: clickhouse.StatusDelivered, DeliveredAt: &deliveredAt},
	}}
	l := New(nil, nil, nil, Options{MessageReader: reader}, discardLog())

	res := l.onQuery(context.Background(), st)(context.Background(), session.QueryRequest{MessageID: msgID.String()})

	want := session.QueryResult{Status: smpp.StatusOK, MessageID: msgID.String(), MessageState: smpp.MessageStateDelivered, FinalDate: "260927140509700+"}
	if res != want {
		t.Errorf("res = %+v, want %+v", res, want)
	}
}

func TestOnQueryMapsEveryCDRStatus(t *testing.T) {
	someTime := time.Date(2026, 9, 27, 14, 5, 9, 0, time.UTC)
	for status, want := range map[clickhouse.Status]uint8{
		clickhouse.StatusAccepted:  smpp.MessageStateEnroute,
		clickhouse.StatusEnroute:   smpp.MessageStateEnroute,
		clickhouse.StatusRerouted:  smpp.MessageStateEnroute,
		clickhouse.StatusExpired:   smpp.MessageStateExpired,
		clickhouse.StatusCancelled: smpp.MessageStateDeleted,
		clickhouse.StatusFailed:    smpp.MessageStateUndeliverable,
		clickhouse.StatusRejected:  smpp.MessageStateRejected,
	} {
		t.Run(string(status), func(t *testing.T) {
			st := &connState{querySMEnabled: true, customerID: uuid.New(), accountID: uuid.New()}
			msgID := uuid.New()
			reader := &scopedReader{rows: map[[3]uuid.UUID]clickhouse.CDRRow{
				{st.customerID, st.accountID, msgID}: {MessageID: msgID, Status: status, DeliveredAt: &someTime},
			}}
			l := New(nil, nil, nil, Options{MessageReader: reader}, discardLog())

			res := l.onQuery(context.Background(), st)(context.Background(), session.QueryRequest{MessageID: msgID.String()})

			if res.Status != smpp.StatusOK || res.MessageState != want {
				t.Errorf("res = {%#x, state %d}, want {ESME_ROK, state %d}", res.Status, res.MessageState, want)
			}
			if res.FinalDate != "" {
				t.Errorf("final_date = %q, want empty: delivered_at dates a delivery, not this state", res.FinalDate)
			}
		})
	}
}

func TestOnQueryRejectsWhatItCannotResolve(t *testing.T) {
	owner := &connState{querySMEnabled: true, customerID: uuid.New(), accountID: uuid.New()}
	msgID := uuid.New()
	rows := map[[3]uuid.UUID]clickhouse.CDRRow{
		{owner.customerID, owner.accountID, msgID}: {MessageID: msgID, Status: clickhouse.StatusDelivered},
	}

	for _, tc := range []struct {
		name   string
		st     *connState
		id     string
		reader *scopedReader
		want   uint32
	}{
		{"unparsable id", owner, "not-a-uuid", &scopedReader{rows: rows}, errs.StatusInvalidMsgID},
		{"unknown id", owner, uuid.NewString(), &scopedReader{rows: rows}, errs.StatusInvalidMsgID},
		{"another account's message", &connState{querySMEnabled: true, customerID: owner.customerID, accountID: uuid.New()},
			msgID.String(), &scopedReader{rows: rows}, errs.StatusInvalidMsgID},
		{"reader failure", owner, msgID.String(), &scopedReader{err: errors.New("clickhouse down")}, errs.StatusQueryFail},
		{"status outside the mapping", owner, msgID.String(), &scopedReader{rows: map[[3]uuid.UUID]clickhouse.CDRRow{
			{owner.customerID, owner.accountID, msgID}: {MessageID: msgID, Status: clickhouse.Status("unmapped")},
		}}, errs.StatusQueryFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := New(nil, nil, nil, Options{MessageReader: tc.reader}, discardLog())
			res := l.onQuery(context.Background(), tc.st)(context.Background(), session.QueryRequest{MessageID: tc.id})
			if res.Status != tc.want {
				t.Errorf("status = %#x, want %#x", res.Status, tc.want)
			}
		})
	}
}

func TestOnQueryDeliveredWithoutADateLeavesFinalDateEmpty(t *testing.T) {
	st := &connState{querySMEnabled: true, customerID: uuid.New(), accountID: uuid.New()}
	msgID := uuid.New()
	reader := &scopedReader{rows: map[[3]uuid.UUID]clickhouse.CDRRow{
		{st.customerID, st.accountID, msgID}: {MessageID: msgID, Status: clickhouse.StatusDelivered},
	}}
	l := New(nil, nil, nil, Options{MessageReader: reader}, discardLog())

	res := l.onQuery(context.Background(), st)(context.Background(), session.QueryRequest{MessageID: msgID.String()})

	if res.Status != smpp.StatusOK || res.MessageState != smpp.MessageStateDelivered || res.FinalDate != "" {
		t.Errorf("res = %+v, want DELIVERED with an empty final_date", res)
	}
}

// TestOnQueryBoundsTheLookup pins that the CDR read runs under its own deadline: it runs on the session's
// read goroutine, and ClickHouse's default read timeout would freeze the whole bind.
func TestOnQueryBoundsTheLookup(t *testing.T) {
	reader := &scopedReader{}
	l := New(nil, nil, nil, Options{MessageReader: reader}, discardLog())

	l.onQuery(context.Background(), &connState{querySMEnabled: true})(
		context.Background(), session.QueryRequest{MessageID: uuid.NewString()})

	if !reader.deadline {
		t.Error("CDR read without a deadline")
	}
}

func TestOnQueryTracesTheLookupOutcome(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader *scopedReader
		want   errs.Code
	}{
		{"unknown message", &scopedReader{}, errs.ErrMessageNotFound},
		{"failed read", &scopedReader{err: errors.New("clickhouse down")}, errs.ErrInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")
			l := New(nil, nil, nil, Options{MessageReader: tc.reader, Tracer: tracer}, discardLog())

			l.onQuery(context.Background(), &connState{querySMEnabled: true})(
				context.Background(), session.QueryRequest{MessageID: uuid.NewString()})

			spans := rec.Ended()
			if len(spans) != 1 || spans[0].Name() != "smpp.query" {
				t.Fatalf("ended spans = %d, want one smpp.query", len(spans))
			}
			if got := spans[0].Status(); got.Code != codes.Error || got.Description != string(tc.want) {
				t.Errorf("span status = %+v, want Error %q", got, tc.want)
			}
		})
	}
}
