package session_test

import (
	"context"
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

func TestSession_QueryAfterBind(t *testing.T) {
	t.Parallel()
	got := make(chan session.QueryRequest, 1)
	cfg := session.Config{
		OnQuery: func(_ context.Context, req session.QueryRequest) session.QueryResult {
			got <- req
			return session.QueryResult{Status: smpp.StatusOK, MessageID: req.MessageID, MessageState: 2}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransceiver)

	q := &smpp.QuerySM{MessageID: "msg-7", SourceAddr: "GATEWAY"}
	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: q})
	if resp.Status != smpp.StatusOK {
		t.Fatalf("query status = %#x, want StatusOK", resp.Status)
	}
	out, ok := resp.Body.(*smpp.QuerySMResp)
	if !ok {
		t.Fatalf("resp body = %T, want *smpp.QuerySMResp", resp.Body)
	}
	if out.MessageID != "msg-7" || out.MessageState != 2 {
		t.Errorf("resp = {id:%q state:%d}, want {msg-7 2}", out.MessageID, out.MessageState)
	}
	select {
	case req := <-got:
		if req.MessageID != "msg-7" || req.SourceAddr != "GATEWAY" {
			t.Errorf("hook req = {id:%q src:%q}, want {msg-7 GATEWAY}", req.MessageID, req.SourceAddr)
		}
	default:
		t.Fatal("OnQuery was not called")
	}
}

func TestSession_QueryBeforeBind(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})

	resp := roundtrip(t, client, smpp.PDU{Sequence: 1, Body: &smpp.QuerySM{MessageID: "x"}})
	if resp.Status != errs.StatusInvalidBindStatus {
		t.Errorf("status = %#x, want ESME_RINVBNDSTS (%#x)", resp.Status, errs.StatusInvalidBindStatus)
	}
	if _, ok := resp.Body.(*smpp.QuerySMResp); !ok {
		t.Errorf("resp body = %T, want *smpp.QuerySMResp", resp.Body)
	}
}

func TestSession_CancelAfterBind(t *testing.T) {
	t.Parallel()
	got := make(chan session.CancelRequest, 1)
	cfg := session.Config{
		OnCancel: func(_ context.Context, req session.CancelRequest) session.CancelResult {
			got <- req
			return session.CancelResult{Status: smpp.StatusOK}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	c := &smpp.CancelSM{MessageID: "msg-9", SourceAddr: "GATEWAY", DestinationAddr: "22990001111"}
	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: c})
	if resp.Status != smpp.StatusOK {
		t.Fatalf("cancel status = %#x, want StatusOK", resp.Status)
	}
	if _, ok := resp.Body.(*smpp.CancelSMResp); !ok {
		t.Fatalf("resp body = %T, want *smpp.CancelSMResp", resp.Body)
	}
	select {
	case req := <-got:
		if req.MessageID != "msg-9" {
			t.Errorf("hook req id = %q, want msg-9", req.MessageID)
		}
	default:
		t.Fatal("OnCancel was not called")
	}
}

func TestSession_CancelBeforeBind(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})

	resp := roundtrip(t, client, smpp.PDU{Sequence: 1, Body: &smpp.CancelSM{MessageID: "x"}})
	if resp.Status != errs.StatusInvalidBindStatus {
		t.Errorf("status = %#x, want ESME_RINVBNDSTS (%#x)", resp.Status, errs.StatusInvalidBindStatus)
	}
	if _, ok := resp.Body.(*smpp.CancelSMResp); !ok {
		t.Errorf("resp body = %T, want *smpp.CancelSMResp", resp.Body)
	}
}

// TestSession_QueryHookPanicRecovered pins that a panicking OnQuery rejects with ESME_RSYSERR and
// keeps the session alive.
func TestSession_QueryHookPanicRecovered(t *testing.T) {
	t.Parallel()
	cfg := session.Config{
		Logger: discardLogger(),
		OnQuery: func(context.Context, session.QueryRequest) session.QueryResult {
			panic("boom in OnQuery")
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransceiver)

	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.QuerySM{MessageID: "x"}})
	if resp.Status != errs.StatusSysErr {
		t.Errorf("status = %#x, want ESME_RSYSERR (%#x)", resp.Status, errs.StatusSysErr)
	}
}
