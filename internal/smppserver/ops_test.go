package smppserver

import (
	"context"
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

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

func TestOnCancelToggle(t *testing.T) {
	l := New(nil, nil, nil, Options{}, discardLog())

	t.Run("disabled is ESME_RINVCMDID", func(t *testing.T) {
		res := l.onCancel(context.Background(), &connState{cancelSMEnabled: false})(
			context.Background(), session.CancelRequest{MessageID: "m1"})
		if res.Status != errs.StatusInvalidCmdID {
			t.Errorf("status = %#x, want ESME_RINVCMDID (%#x)", res.Status, errs.StatusInvalidCmdID)
		}
	})

	t.Run("enabled is a skeleton OK", func(t *testing.T) {
		res := l.onCancel(context.Background(), &connState{cancelSMEnabled: true})(
			context.Background(), session.CancelRequest{MessageID: "m1"})
		if res.Status != smpp.StatusOK {
			t.Errorf("status = %#x, want StatusOK", res.Status)
		}
	})
}
