package smppserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// expectDeliver reads one deliver_sm the server pushed, acknowledges it, and returns it.
func (e *esme) expectDeliver(t *testing.T) *smpp.DeliverSM {
	t.Helper()
	_ = e.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	pdu, err := smpp.ReadPDU(e.conn)
	if err != nil {
		t.Fatalf("read deliver_sm: %v", err)
	}
	ds, ok := pdu.Body.(*smpp.DeliverSM)
	if !ok {
		t.Fatalf("expected deliver_sm, got %T", pdu.Body)
	}
	if err := smpp.WritePDU(e.conn, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.DeliverSMResp{}}); err != nil {
		t.Fatalf("write deliver_sm_resp: %v", err)
	}
	return ds
}

// deliverSMBytes encodes a deliver_sm carrying body, as the caller (step-048) would.
func deliverSMBytes(t *testing.T, source, dest, body string) []byte {
	t.Helper()
	ds := &smpp.DeliverSM{}
	ds.SourceAddr, ds.DestinationAddr = source, dest
	ds.ShortMessage = []byte(body)
	raw, err := smpp.Marshal(smpp.PDU{Sequence: 1, Body: ds})
	if err != nil {
		t.Fatalf("marshal deliver_sm: %v", err)
	}
	return raw
}

// lookupBindID resolves the account's single live bind_id via the registry, as step-048 does.
func lookupBindID(t *testing.T, registry registrypb.SessionRegistryClient, accountID uuid.UUID) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := registry.Lookup(ctx, &registrypb.LookupRequest{AccountId: accountID.String()})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(resp.GetSessions()) != 1 {
		t.Fatalf("lookup returned %d sessions, want 1", len(resp.GetSessions()))
	}
	return resp.GetSessions()[0].GetBindId()
}

// TestDeliverPushesToBoundReceiver drives the full return leg: a transceiver ESME binds, a gRPC
// Deliver pushes a deliver_sm to it, the ESME receives it and acks, and Deliver reports delivered.
func TestDeliverPushesToBoundReceiver(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	sid, pw, accountID := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr, listener := startListenerRef(t, pool, registry)
	deliverer := smppserver.NewDeliverServer(listener, discardLogger())

	e := dialESME(t, addr)
	defer e.close()
	if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}
	bindID := lookupBindID(t, registry, accountID)

	const body = "confidential mo body"
	done := make(chan error, 1)
	go func() {
		resp, err := deliverer.Deliver(context.Background(),
			&registrypb.DeliverRequest{BindId: bindID, Pdu: deliverSMBytes(t, "22507000001", "36000", body)})
		if err == nil && !resp.GetDelivered() {
			err = context.DeadlineExceeded
		}
		done <- err
	}()

	ds := e.expectDeliver(t)
	if err := <-done; err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if string(ds.ShortMessage) != body || ds.DestinationAddr != "36000" {
		t.Errorf("received deliver_sm = dest %q / body %q, want 36000 / %q", ds.DestinationAddr, ds.ShortMessage, body)
	}
}

// TestDeliverToDeadBindIsNotFound: a Deliver to a bind this pod does not hold is a NotFound status,
// which the caller uses to re-resolve the owning pod.
func TestDeliverToDeadBindIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)
	_, listener := startListenerRef(t, pool, registry)
	deliverer := smppserver.NewDeliverServer(listener, discardLogger())

	_, err := deliverer.Deliver(context.Background(),
		&registrypb.DeliverRequest{BindId: uuid.NewString(), Pdu: deliverSMBytes(t, "1", "2", "x")})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Deliver to dead bind status = %v, want NotFound", status.Code(err))
	}
}

// TestDeliverRejectsNonDeliverSM: PDU bytes that are not a deliver_sm (an injected submit_sm, or
// truncated bytes) are refused as InvalidArgument, before any bind lookup.
func TestDeliverRejectsNonDeliverSM(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)
	_, listener := startListenerRef(t, pool, registry)
	deliverer := smppserver.NewDeliverServer(listener, discardLogger())

	// A well-formed submit_sm is not a deliver_sm.
	sm := &smpp.SubmitSM{}
	sm.SourceAddr, sm.DestinationAddr = "1", "2"
	sm.ShortMessage = []byte("x")
	raw, err := smpp.Marshal(smpp.PDU{Sequence: 1, Body: sm})
	if err != nil {
		t.Fatalf("marshal submit_sm: %v", err)
	}
	if _, err := deliverer.Deliver(context.Background(),
		&registrypb.DeliverRequest{BindId: uuid.NewString(), Pdu: raw}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("submit_sm status = %v, want InvalidArgument", status.Code(err))
	}

	// Truncated bytes do not decode.
	if _, err := deliverer.Deliver(context.Background(),
		&registrypb.DeliverRequest{BindId: uuid.NewString(), Pdu: []byte{0x00, 0x01, 0x02}}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("truncated pdu status = %v, want InvalidArgument", status.Code(err))
	}
}

// TestDeliverToTransmitterIsRefused: a transmitter bind cannot receive deliver_sm; Deliver refuses it
// with FailedPrecondition (never retryable).
func TestDeliverToTransmitterIsRefused(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	sid, pw, accountID := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTX})
	addr, listener := startListenerRef(t, pool, registry)
	deliverer := smppserver.NewDeliverServer(listener, discardLogger())

	e := dialESME(t, addr)
	defer e.close()
	if got := e.bind(t, smppsession.BindTransmitter, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}
	bindID := lookupBindID(t, registry, accountID)

	_, err := deliverer.Deliver(context.Background(),
		&registrypb.DeliverRequest{BindId: bindID, Pdu: deliverSMBytes(t, "1", "2", "x")})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Deliver to transmitter status = %v, want FailedPrecondition", status.Code(err))
	}
}
