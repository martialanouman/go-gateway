package smppserver

import (
	"context"
	"testing"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// fakeCanceller records the scope it was called with and returns a fixed error, standing in for the
// shared *cancel.Canceller.
type fakeCanceller struct {
	err        error
	called     bool
	customerID uuid.UUID
	accountID  uuid.UUID
	messageID  uuid.UUID
}

func (f *fakeCanceller) Cancel(_ context.Context, customerID, accountID, messageID uuid.UUID) error {
	f.called = true
	f.customerID, f.accountID, f.messageID = customerID, accountID, messageID
	return f.err
}

func TestOnQueryToggle(t *testing.T) {
	l := New(nil, nil, nil, Options{}, discardLog())

	t.Run("disabled is ESME_RINVCMDID", func(t *testing.T) {
		res := l.onQuery(context.Background(), &connState{querySMEnabled: false})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if res.Status != errs.StatusInvalidCmdID {
			t.Errorf("status = %#x, want ESME_RINVCMDID (%#x)", res.Status, errs.StatusInvalidCmdID)
		}
	})

	t.Run("enabled is a skeleton OK echoing the id with a valid state", func(t *testing.T) {
		res := l.onQuery(context.Background(), &connState{querySMEnabled: true})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if res.Status != smpp.StatusOK {
			t.Errorf("status = %#x, want StatusOK", res.Status)
		}
		if res.MessageID != "m1" {
			t.Errorf("message id = %q, want m1", res.MessageID)
		}
		// message_state must be a valid SMPP v3.4 value (1..8); the skeleton has no real lookup yet, so
		// it reports UNKNOWN rather than the undefined 0.
		if res.MessageState != smpp.MessageStateUnknown {
			t.Errorf("message_state = %d, want UNKNOWN (%d)", res.MessageState, smpp.MessageStateUnknown)
		}
	})
}

// stubQueryLimiter is a controllable QueryLimiter recording the account it was asked about.
type stubQueryLimiter struct {
	allow      bool
	calls      int
	gotAccount uuid.UUID
}

func (s *stubQueryLimiter) Allow(_ context.Context, accountID uuid.UUID) bool {
	s.calls++
	s.gotAccount = accountID
	return s.allow
}

// TestOnQueryRateLimited: an enabled query_sm is checked against the per-account query_sm limiter —
// over the limit it is refused ESME_RTHROTTLED, under it answers normally, and a disabled query_sm
// never even reaches the limiter (its own budget is untouched).
func TestOnQueryRateLimited(t *testing.T) {
	account := uuid.New()

	t.Run("over the limit is ESME_RTHROTTLED", func(t *testing.T) {
		lim := &stubQueryLimiter{allow: false}
		l := New(nil, nil, nil, Options{QueryLimiter: lim}, discardLog())
		res := l.onQuery(context.Background(), &connState{querySMEnabled: true, accountID: account})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if res.Status != errs.StatusThrottled {
			t.Errorf("status = %#x, want ESME_RTHROTTLED (%#x)", res.Status, errs.StatusThrottled)
		}
		if lim.gotAccount != account {
			t.Errorf("limiter checked account %s, want %s", lim.gotAccount, account)
		}
	})

	t.Run("under the limit answers normally", func(t *testing.T) {
		lim := &stubQueryLimiter{allow: true}
		l := New(nil, nil, nil, Options{QueryLimiter: lim}, discardLog())
		res := l.onQuery(context.Background(), &connState{querySMEnabled: true, accountID: account})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if res.Status != smpp.StatusOK || res.MessageID != "m1" {
			t.Errorf("res = {%#x, %q}, want a StatusOK echoing m1", res.Status, res.MessageID)
		}
	})

	t.Run("disabled never reaches the limiter", func(t *testing.T) {
		lim := &stubQueryLimiter{allow: true}
		l := New(nil, nil, nil, Options{QueryLimiter: lim}, discardLog())
		_ = l.onQuery(context.Background(), &connState{querySMEnabled: false, accountID: account})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if lim.calls != 0 {
			t.Errorf("limiter consulted %d times for a disabled query_sm, want 0", lim.calls)
		}
	})
}

func TestOnCancelDisabledIsInvalidCmdID(t *testing.T) {
	l := New(nil, nil, nil, Options{Canceller: &fakeCanceller{}}, discardLog())

	res := l.onCancel(context.Background(), &connState{cancelSMEnabled: false})(
		context.Background(), session.CancelRequest{MessageID: uuid.NewString()})
	if res.Status != errs.StatusInvalidCmdID {
		t.Errorf("status = %#x, want ESME_RINVCMDID (%#x)", res.Status, errs.StatusInvalidCmdID)
	}
}

// TestOnCancelNilCancellerFails pins the fail-closed fallback: cancel_sm enabled on the account but no
// Canceller wired (bind-only build) rejects with ESME_RCANCELFAIL rather than silently acking.
func TestOnCancelNilCancellerFails(t *testing.T) {
	l := New(nil, nil, nil, Options{}, discardLog())

	res := l.onCancel(context.Background(), &connState{cancelSMEnabled: true})(
		context.Background(), session.CancelRequest{MessageID: uuid.NewString()})
	if res.Status != errs.StatusCancelFail {
		t.Errorf("status = %#x, want ESME_RCANCELFAIL (%#x)", res.Status, errs.StatusCancelFail)
	}
}

// TestOnCancelMapsCancellerOutcome pins that the Canceller's domain error is mapped once to its SMPP
// command_status, and that a nil error is ESME_ROK.
func TestOnCancelMapsCancellerOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want uint32
	}{
		{"queued cancels", nil, smpp.StatusOK},
		{"already dispatched", errs.ErrCancelFailed, errs.StatusCancelFail},
		{"unknown message", errs.ErrMessageNotFound, errs.StatusInvalidMsgID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeCanceller{err: tc.err}
			l := New(nil, nil, nil, Options{Canceller: fc}, discardLog())

			res := l.onCancel(context.Background(), &connState{cancelSMEnabled: true})(
				context.Background(), session.CancelRequest{MessageID: uuid.NewString()})
			if res.Status != tc.want {
				t.Errorf("status = %#x, want %#x", res.Status, tc.want)
			}
			if !fc.called {
				t.Error("Canceller was not invoked")
			}
		})
	}
}

// TestOnCancelScopesToBindAccount pins that the cancel is scoped to the bound connection's account
// (invariant: a bind cannot cancel another account's message), and that the message_id is parsed and
// forwarded verbatim.
func TestOnCancelScopesToBindAccount(t *testing.T) {
	fc := &fakeCanceller{}
	l := New(nil, nil, nil, Options{Canceller: fc}, discardLog())
	st := &connState{cancelSMEnabled: true, accountID: uuid.New(), customerID: uuid.New()}
	msgID := uuid.New()

	l.onCancel(context.Background(), st)(context.Background(), session.CancelRequest{MessageID: msgID.String()})

	if fc.accountID != st.accountID || fc.customerID != st.customerID {
		t.Errorf("scope = (cust %s, acct %s), want (cust %s, acct %s)", fc.customerID, fc.accountID, st.customerID, st.accountID)
	}
	if fc.messageID != msgID {
		t.Errorf("message id = %s, want %s", fc.messageID, msgID)
	}
}

// TestOnCancelRejectsMalformedMessageID pins that a message_id that is not a UUID is answered
// ESME_RINVMSGID without ever calling the Canceller (it names no message).
func TestOnCancelRejectsMalformedMessageID(t *testing.T) {
	fc := &fakeCanceller{}
	l := New(nil, nil, nil, Options{Canceller: fc}, discardLog())

	res := l.onCancel(context.Background(), &connState{cancelSMEnabled: true})(
		context.Background(), session.CancelRequest{MessageID: "not-a-uuid"})
	if res.Status != errs.StatusInvalidMsgID {
		t.Errorf("status = %#x, want ESME_RINVMSGID (%#x)", res.Status, errs.StatusInvalidMsgID)
	}
	if fc.called {
		t.Error("Canceller must not be called for a malformed message_id")
	}
}
