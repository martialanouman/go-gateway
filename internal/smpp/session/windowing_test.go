package session_test

import (
	"context"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// TestSubmitDoesNotBlockReadLoop is the central step-088 criterion: while a submit_sm is in flight (its
// OnSubmit blocked on a slow produce), the read goroutine keeps reading, so a concurrent enquire_link is
// answered immediately — its enquire_link_resp arrives BEFORE the still-pending submit_sm_resp.
func TestSubmitDoesNotBlockReadLoop(t *testing.T) {
	release := make(chan struct{})
	cfg := session.Config{
		Logger: discardLogger(),
		OnSubmit: func(_ context.Context, _ session.SubmitRequest) session.SubmitResult {
			<-release // a slow produce, in flight
			return session.SubmitResult{Status: smpp.StatusOK, MessageID: "id-1"}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	writePDU(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}}) // dispatched to a worker, blocks
	writePDU(t, client, smpp.PDU{Sequence: 3, Body: &smpp.EnquireLink{}})

	// The keepalive is answered while the submit is still in flight.
	first := readPDU(t, client)
	if first.CommandID() != smpp.CmdEnquireLinkResp || first.Sequence != 3 {
		t.Fatalf("first response = {%#x, seq %d}, want enquire_link_resp seq 3 (the read loop must not block on the submit)", first.CommandID(), first.Sequence)
	}

	close(release) // let the submit finish
	second := readPDU(t, client)
	if second.CommandID() != smpp.CmdSubmitSMResp || second.Sequence != 2 || second.Status != smpp.StatusOK {
		t.Errorf("second response = {%#x, seq %d, status %#x}, want submit_sm_resp seq 2 OK", second.CommandID(), second.Sequence, second.Status)
	}
}

// TestConcurrentSubmitsUpToWindow: a session processes submits concurrently up to InboundWindow — all
// their OnSubmit hooks run at once — and every one is answered OK, correlated by sequence_number (order
// indifferent).
func TestConcurrentSubmitsUpToWindow(t *testing.T) {
	const window = 5
	arrived := make(chan struct{}, window)
	release := make(chan struct{})
	cfg := session.Config{
		Logger:        discardLogger(),
		InboundWindow: window,
		OnSubmit: func(_ context.Context, _ session.SubmitRequest) session.SubmitResult {
			arrived <- struct{}{}
			<-release
			return session.SubmitResult{Status: smpp.StatusOK}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	for seq := uint32(2); seq < 2+window; seq++ {
		writePDU(t, client, smpp.PDU{Sequence: seq, Body: &smpp.SubmitSM{}})
	}
	// All `window` submits must be in flight simultaneously.
	for i := 0; i < window; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d submits were processing concurrently — the window did not parallelise", i, window)
		}
	}
	close(release)

	seen := map[uint32]bool{}
	for i := 0; i < window; i++ {
		resp := readPDU(t, client)
		if resp.CommandID() != smpp.CmdSubmitSMResp || resp.Status != smpp.StatusOK {
			t.Errorf("response %d = {%#x, status %#x}, want submit_sm_resp OK", i, resp.CommandID(), resp.Status)
		}
		seen[resp.Sequence] = true
	}
	for seq := uint32(2); seq < 2+window; seq++ {
		if !seen[seq] {
			t.Errorf("no submit_sm_resp for sequence %d", seq)
		}
	}
}

// TestWindowFullThrottles: when the inbound window is full, a further submit_sm is refused immediately
// with ESME_RTHROTTLED, without blocking the read loop.
func TestWindowFullThrottles(t *testing.T) {
	release := make(chan struct{})
	cfg := session.Config{
		Logger:        discardLogger(),
		InboundWindow: 1,
		OnSubmit: func(_ context.Context, _ session.SubmitRequest) session.SubmitResult {
			<-release
			return session.SubmitResult{Status: smpp.StatusOK}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	writePDU(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}}) // takes the only slot (blocks)
	writePDU(t, client, smpp.PDU{Sequence: 3, Body: &smpp.SubmitSM{}}) // window full

	throttled := readPDU(t, client)
	if throttled.Sequence != 3 || throttled.Status != errs.StatusThrottled {
		t.Fatalf("full-window response = {seq %d, status %#x}, want seq 3 ESME_RTHROTTLED (%#x)", throttled.Sequence, throttled.Status, errs.StatusThrottled)
	}

	close(release)
	ok := readPDU(t, client)
	if ok.Sequence != 2 || ok.Status != smpp.StatusOK {
		t.Errorf("in-flight response = {seq %d, status %#x}, want seq 2 OK", ok.Sequence, ok.Status)
	}
}

// TestSubmitProduceTimeout: a produce that hangs past InboundSubmitTimeout is answered ESME_RSUBMITFAIL
// (and the worker released) rather than pinning the worker forever.
func TestSubmitProduceTimeout(t *testing.T) {
	cfg := session.Config{
		Logger:               discardLogger(),
		InboundSubmitTimeout: 50 * time.Millisecond,
		OnSubmit: func(ctx context.Context, _ session.SubmitRequest) session.SubmitResult {
			<-ctx.Done() // a hung produce that respects cancellation
			return session.SubmitResult{Status: errs.StatusSysErr}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}})
	if resp.Status != errs.StatusSubmitFail {
		t.Errorf("status = %#x, want ESME_RSUBMITFAIL (%#x) on a produce timeout", resp.Status, errs.StatusSubmitFail)
	}
}

// TestCloseDrainsInFlightWorkerPromptly: a server-side Close (a force-disconnect, step-032) cancels the
// in-flight workers' context so they drain at once — NOT after InboundSubmitTimeout, which is set to an
// hour here to prove the drain is via cancellation, not the backstop.
func TestCloseDrainsInFlightWorkerPromptly(t *testing.T) {
	started := make(chan struct{})
	drained := make(chan struct{})
	cfg := session.Config{
		Logger:               discardLogger(),
		InboundSubmitTimeout: time.Hour,
		OnSubmit: func(ctx context.Context, _ session.SubmitRequest) session.SubmitResult {
			close(started)
			<-ctx.Done()
			close(drained)
			return session.SubmitResult{Status: smpp.StatusOK}
		},
	}
	client, sess, _, errc := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)
	writePDU(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}})

	<-started    // the worker is in flight
	sess.Close() // force-disconnect

	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight worker not drained on Close within 3s (it would have waited InboundSubmitTimeout=1h)")
	}
	<-errc // Serve returns after the drain
}

// TestDrainWaitsForInFlightWorkers: cancelling the session drains its in-flight submit workers (their
// ctx is cancelled) before Serve returns — no orphaned goroutine.
func TestDrainWaitsForInFlightWorkers(t *testing.T) {
	done := make(chan struct{})
	cfg := session.Config{
		Logger: discardLogger(),
		OnSubmit: func(ctx context.Context, _ session.SubmitRequest) session.SubmitResult {
			<-ctx.Done() // in flight until the session ends
			close(done)
			return session.SubmitResult{Status: smpp.StatusOK}
		},
	}
	client, _, cancel, errc := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	writePDU(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}}) // worker in flight

	// A drain now sends the ESME an outbound unbind before closing (step-260), and that write is
	// synchronous over net.Pipe. Keep reading like a real ESME would, or Serve blocks on the write
	// deadline and this test measures unbindWriteTimeout instead of the worker drain.
	go func() {
		for {
			if _, err := smpp.ReadPDU(client); err != nil {
				return
			}
		}
	}()

	cancel() // end the session

	select {
	case <-errc:
		// Serve returned; the worker must have been drained (its OnSubmit released by the cancelled ctx).
		select {
		case <-done:
		default:
			t.Error("Serve returned before the in-flight worker was drained")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return — a worker was not drained (orphan goroutine)")
	}
}

// TestDrainReleasesWorkersEvenWhenThePeerStopsReading: the in-flight workers are cancelled at the
// START of the drain, not behind the outbound unbind.
//
// The unbind is a courtesy write bounded by unbindWriteTimeout (5s). A peer that has stopped reading —
// a wedged ESME, a full receive window — makes that write block for the whole deadline. If worker
// cancellation queued behind it, every in-flight submit would stay pending for those five seconds on
// a pod that is already going away. The budget below is deliberately far under unbindWriteTimeout:
// that gap is the assertion.
func TestDrainReleasesWorkersEvenWhenThePeerStopsReading(t *testing.T) {
	released := make(chan struct{})
	cfg := session.Config{
		Logger: discardLogger(),
		OnSubmit: func(ctx context.Context, _ session.SubmitRequest) session.SubmitResult {
			<-ctx.Done()
			close(released)
			return session.SubmitResult{Status: smpp.StatusOK}
		},
	}
	client, _, cancel, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	writePDU(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}}) // worker in flight

	// Deliberately NO reader: the drain's unbind write will stall until its deadline.
	cancel()

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("the in-flight worker was still pending 1s into the drain: worker cancellation is " +
			"queued behind the courtesy unbind write, so a peer that stopped reading holds every " +
			"in-flight submit for the whole unbindWriteTimeout")
	}
}
