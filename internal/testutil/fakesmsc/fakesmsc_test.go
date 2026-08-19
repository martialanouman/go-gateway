package fakesmsc_test

import (
	"net"
	"slices"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
)

// client is a minimal SMPP peer for driving the fake SMSC in tests.
type client struct {
	t   *testing.T
	nc  net.Conn
	seq uint32
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	return &client{t: t, nc: nc}
}

func (c *client) send(body smpp.Body) uint32 {
	c.t.Helper()
	c.seq++
	if err := smpp.WritePDU(c.nc, smpp.PDU{Sequence: c.seq, Body: body}); err != nil {
		c.t.Fatalf("write: %v", err)
	}
	return c.seq
}

func (c *client) read() smpp.PDU {
	c.t.Helper()
	_ = c.nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	pdu, err := smpp.ReadPDU(c.nc)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return pdu
}

func (c *client) bindTransceiver() {
	c.t.Helper()
	c.send(&smpp.BindTransceiver{BindFields: smpp.BindFields{SystemID: "esme", Password: "pw"}})
	resp := c.read()
	if _, ok := resp.Body.(*smpp.BindTransceiverResp); !ok {
		c.t.Fatalf("bind resp: got %T", resp.Body)
	}
	if resp.Status != smpp.StatusOK {
		c.t.Fatalf("bind status: got %#x", resp.Status)
	}
}

func TestSubmitOK(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	seq := c.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000", ShortMessage: []byte("hi")}})
	resp := c.read()
	body, ok := resp.Body.(*smpp.SubmitSMResp)
	if !ok {
		t.Fatalf("submit resp: got %T", resp.Body)
	}
	if resp.Status != smpp.StatusOK {
		t.Errorf("status: got %#x want OK", resp.Status)
	}
	if resp.Sequence != seq {
		t.Errorf("sequence: got %d want %d", resp.Sequence, seq)
	}
	if body.MessageID == "" {
		t.Error("expected an assigned SMSC message id")
	}
}

func TestSubmitThrottled(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() },
	})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	c.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000"}})
	resp := c.read()
	if resp.Status != errs.StatusThrottled {
		t.Errorf("status: got %#x want ESME_RTHROTTLED", resp.Status)
	}
	if body := resp.Body.(*smpp.SubmitSMResp); body.MessageID != "" {
		t.Errorf("a rejected submit must not assign a message id: %q", body.MessageID)
	}
}

func TestSubmitDelay(t *testing.T) {
	const delay = 150 * time.Millisecond
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Delay(delay) },
	})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	start := time.Now()
	c.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000"}})
	_ = c.read()
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("expected the response to be delayed by at least %s, took %s", delay, elapsed)
	}
}

func TestEnquireLink(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	c.send(&smpp.EnquireLink{})
	if _, ok := c.read().Body.(*smpp.EnquireLinkResp); !ok {
		t.Fatal("expected enquire_link_resp")
	}
}

func TestSendDLR(t *testing.T) {
	var assignedID string
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	c.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000", ShortMessage: []byte("hi")}})
	assignedID = c.read().Body.(*smpp.SubmitSMResp).MessageID

	if err := s.SendDLR(assignedID, fakesmsc.Delivered); err != nil {
		t.Fatalf("SendDLR: %v", err)
	}
	pdu := c.read()
	dlr, ok := pdu.Body.(*smpp.DeliverSM)
	if !ok {
		t.Fatalf("expected deliver_sm, got %T", pdu.Body)
	}
	if dlr.ESMClass&smpp.ESMClassMCDeliveryReceipt == 0 {
		t.Error("deliver_sm should carry the delivery-receipt esm_class bit")
	}
	if v, ok := dlr.TLVs.Get(smpp.TagReceiptedMessageID); !ok || string(v) != assignedID {
		t.Errorf("receipted_message_id: got %q ok=%v want %q", v, ok, assignedID)
	}
}

func TestSendDLRWithoutReceiverFails(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})
	// No bound connection at all: SendDLR has nowhere to deliver.
	if err := s.SendDLR("smsc-1", fakesmsc.Delivered); err == nil {
		t.Fatal("expected an error when no receiver is bound")
	}
}

// TestSubmitsByConnCountsEachBindSeparately pins the reading a throughput bench uses to tell a pool
// that fanned out from one that did not.
//
// The pool shards a poll batch by FNV32a(message id) % binds, so a bind that carried nothing means the
// geometry left it idle and the palier measured a smaller pool than its label claims. A total would
// hide that: two binds carrying 3 and 0 sum to the same 3 as 2 and 1.
func TestSubmitsByConnCountsEachBindSeparately(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})

	// The counts are ordered by the connection id assigned at accept time, so the first dialer is
	// index 0 whatever order the responses come back in.
	first := dial(t, s.Addr())
	first.bindTransceiver()
	second := dial(t, s.Addr())
	second.bindTransceiver()

	for range 3 {
		first.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000"}})
		first.read()
	}
	second.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000001"}})
	second.read()

	counts := s.SubmitsByConn()
	if want := []int64{3, 1}; !slices.Equal(counts, want) {
		t.Fatalf("submits by conn: got %v want %v", counts, want)
	}
}

// TestSubmitsByConnCountsARejectedSubmit: the count is what the bind CARRIED, not what succeeded.
//
// A bench reads it to check the fan-out geometry, and a throttled peer is precisely when the geometry
// matters most. Counting only successes would report a bind as idle exactly when it is working hardest.
func TestSubmitsByConnCountsARejectedSubmit(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() },
	})
	c := dial(t, s.Addr())
	c.bindTransceiver()

	c.send(&smpp.SubmitSM{SMFields: smpp.SMFields{DestinationAddr: "22507000000"}})
	c.read()

	if counts := s.SubmitsByConn(); !slices.Equal(counts, []int64{1}) {
		t.Errorf("a throttled submit still rode the bind: got %v want [1]", counts)
	}
}
