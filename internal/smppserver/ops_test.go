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

	t.Run("enabled is a skeleton OK echoing the id", func(t *testing.T) {
		res := l.onQuery(context.Background(), &connState{querySMEnabled: true})(
			context.Background(), session.QueryRequest{MessageID: "m1"})
		if res.Status != smpp.StatusOK {
			t.Errorf("status = %#x, want StatusOK", res.Status)
		}
		if res.MessageID != "m1" {
			t.Errorf("message id = %q, want m1", res.MessageID)
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
