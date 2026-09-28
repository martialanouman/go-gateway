package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// smokeMessageID is the message id the fake SMSC always assigns in submit_sm_resp, matching what a
// well-behaved smoke() is expected to wait for in the DLR.
const smokeMessageID = "gw-1"

// statusBindFailed is command_status ESME_RBINDFAIL (SMPP v3.4 §5.1.3) — what a real SMSC answers a
// bind carrying a password it does not recognize.
const statusBindFailed uint32 = 0x0000000D

// dlrMessage describes the delivery receipt a smokePeer sends after submit_sm, or nil for none.
type dlrMessage struct {
	id, stat string
}

// smokePeer is an in-process stand-in for the smoke SMSC simulator: it binds, submits and delivers
// exactly like session-manager would, so smoke() can be tested without Docker.
type smokePeer struct {
	ln  net.Listener
	f   *fakeAdmin
	dlr *dlrMessage

	mu       sync.Mutex
	refusals int
}

func newSmokePeer(t *testing.T, f *fakeAdmin, refusals int, dlr *dlrMessage) *smokePeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("écoute : %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &smokePeer{ln: ln, f: f, dlr: dlr, refusals: refusals}
	go p.acceptLoop()
	return p
}

func (p *smokePeer) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", p.ln.Addr().String())
}

// acceptLoop's stop condition is the listener closing (t.Cleanup on newSmokePeer).
func (p *smokePeer) acceptLoop() {
	for {
		nc, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(nc)
	}
}

// handle's stop condition is a read error — the client closing its side, at the latest when its
// context deadline fires and smoke() tears the connection down.
func (p *smokePeer) handle(nc net.Conn) {
	defer func() { _ = nc.Close() }()

	req, err := smpp.ReadPDU(nc)
	if err != nil {
		return
	}
	bind, ok := req.Body.(*smpp.BindTransceiver)
	if !ok {
		return
	}

	p.mu.Lock()
	refuse := p.refusals > 0
	if refuse {
		p.refusals--
	}
	p.mu.Unlock()

	if refuse || bind.Password != p.f.credentialSecret(accountName, smokeSystemID) {
		_ = smpp.WritePDU(nc, smpp.PDU{Sequence: req.Sequence, Status: statusBindFailed, Body: &smpp.BindTransceiverResp{}})
		return
	}
	if err := smpp.WritePDU(nc, smpp.PDU{Sequence: req.Sequence, Body: &smpp.BindTransceiverResp{}}); err != nil {
		return
	}

	sub, err := smpp.ReadPDU(nc)
	if err != nil {
		return
	}
	if _, ok := sub.Body.(*smpp.SubmitSM); !ok {
		return
	}
	resp := smpp.PDU{Sequence: sub.Sequence, Body: &smpp.SubmitSMResp{MessageIDResp: smpp.MessageIDResp{MessageID: smokeMessageID}}}
	if err := smpp.WritePDU(nc, resp); err != nil {
		return
	}

	if p.dlr == nil {
		_, _ = smpp.ReadPDU(nc) // no DLR : block until the client gives up at its deadline
		return
	}
	deliver := smpp.PDU{Sequence: 1, Body: &smpp.DeliverSM{SMFields: smpp.SMFields{
		ESMClass:     smpp.ESMClassMCDeliveryReceipt,
		ShortMessage: []byte(fmt.Sprintf("id:%s stat:%s err:001", p.dlr.id, p.dlr.stat)),
	}}}
	if err := smpp.WritePDU(nc, deliver); err != nil {
		return
	}
	_, _ = smpp.ReadPDU(nc) // deliver_sm_resp, or the client closing after its own unbind
}

func seedForSmoke(t *testing.T, a *admin) {
	t.Helper()
	if _, err := seed(context.Background(), a, connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}); err != nil {
		t.Fatalf("seed : %v", err)
	}
}

func TestSmokeSucceedsOnTheReceiptOfItsOwnMessage(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	seedForSmoke(t, a)
	peer := newSmokePeer(t, f, 0, &dlrMessage{id: smokeMessageID, stat: "UNDELIV"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := smoke(ctx, a, peer.dial, 10*time.Millisecond); err != nil {
		t.Fatalf("smoke : %v", err)
	}
}

func TestSmokeIgnoresAReceiptForAnotherMessage(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	seedForSmoke(t, a)
	peer := newSmokePeer(t, f, 0, &dlrMessage{id: "gw-0", stat: "DELIVRD"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := smoke(ctx, a, peer.dial, 10*time.Millisecond); err == nil {
		t.Fatal("smoke : err = nil, want une erreur à l'échéance")
	}
}

func TestSmokeFailsWhenNoReceiptArrives(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	seedForSmoke(t, a)
	peer := newSmokePeer(t, f, 0, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := smoke(ctx, a, peer.dial, 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "DLR") {
		t.Fatalf("err = %v, want une erreur contenant \"DLR\"", err)
	}
}

func TestSmokeRetriesARefusedBindUntilTheRotatedSecretIsKnown(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	seedForSmoke(t, a)
	peer := newSmokePeer(t, f, 2, &dlrMessage{id: smokeMessageID, stat: "DELIVRD"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := smoke(ctx, a, peer.dial, 10*time.Millisecond); err != nil {
		t.Fatalf("smoke : %v", err)
	}
}

func TestSmokeUsesTheRotatedSecret(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	seedForSmoke(t, a)
	creationSecret := f.credentialSecret(accountName, smokeSystemID)
	peer := newSmokePeer(t, f, 0, &dlrMessage{id: smokeMessageID, stat: "DELIVRD"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := smoke(ctx, a, peer.dial, 10*time.Millisecond); err != nil {
		t.Fatalf("smoke : %v", err)
	}
	if rotated := f.credentialSecret(accountName, smokeSystemID); rotated == creationSecret {
		t.Fatal("le secret n'a pas changé : la rotation n'a pas eu lieu")
	}
}
