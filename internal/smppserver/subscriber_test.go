package smppserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/session/disconnect"
)

// scriptedStream yields queued payloads, then blocks until ctx is cancelled and returns its error —
// mimicking a real subscription that waits for the next message.
type scriptedStream struct {
	payloads chan []byte
	closed   bool
}

func (s *scriptedStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case p := <-s.payloads:
		return p, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *scriptedStream) Close() error {
	s.closed = true
	return nil
}

// spyApplier records the disconnect orders applied to it.
type spyApplier struct {
	mu     sync.Mutex
	orders []disconnect.Event
}

func (s *spyApplier) Disconnect(scope disconnect.Scope, id, reason string) {
	s.mu.Lock()
	s.orders = append(s.orders, disconnect.Event{Scope: scope, ID: id, Reason: reason})
	s.mu.Unlock()
}

func (s *spyApplier) snapshot() []disconnect.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]disconnect.Event(nil), s.orders...)
}

// TestSubscriberDispatchesValidOrders pins that a well-formed payload is decoded and applied, while a
// malformed one is skipped without stopping the loop (fail-open), and that ctx cancellation ends the
// subscriber cleanly and closes the stream.
func TestSubscriberDispatchesValidOrders(t *testing.T) {
	stream := &scriptedStream{payloads: make(chan []byte, 3)}
	spy := &spyApplier{}

	stream.payloads <- disconnect.Encode(disconnect.Event{Scope: disconnect.ScopeAccount, ID: "acct-1", Reason: "credential_revoked"})
	stream.payloads <- []byte("garbage-not-json") // must be skipped, loop survives
	stream.payloads <- disconnect.Encode(disconnect.Event{Scope: disconnect.ScopeCustomer, ID: "cust-9", Reason: "customer_suspended"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunDisconnectSubscriber(ctx, stream, spy, discardLog()) }()

	// Wait until both valid orders land, then stop.
	waitFor(t, func() bool { return len(spy.snapshot()) == 2 })
	cancel()

	if err := <-done; err != nil {
		t.Errorf("subscriber returned %v, want nil on ctx cancel", err)
	}
	if !stream.closed {
		t.Error("subscriber did not close the stream")
	}

	got := spy.snapshot()
	if len(got) != 2 {
		t.Fatalf("applied %d orders, want 2 (the garbage one skipped)", len(got))
	}
	if got[0].Scope != disconnect.ScopeAccount || got[0].ID != "acct-1" {
		t.Errorf("order 0 = %+v, want account/acct-1", got[0])
	}
	if got[1].Scope != disconnect.ScopeCustomer || got[1].ID != "cust-9" {
		t.Errorf("order 1 = %+v, want customer/cust-9", got[1])
	}
}

// waitFor polls cond up to a bounded deadline, failing the test if it never holds.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
