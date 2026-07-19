package session

import (
	"context"
	"fmt"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// Send issues a server-initiated request to the ESME and waits for its correlated response. It
// blocks on the send window first, so at most WindowSize requests are ever outstanding; the slot is
// released when the response arrives, on timeout, or when the session closes. It is the transport
// primitive that step-046 uses to push deliver_sm to a bound receiver.
//
// Send does not gate on session state: the caller decides whether the ESME may receive (a receiver
// or transceiver bind). It returns ErrClosed if the session is shutting down and ctx.Err() if ctx
// ends first.
func (s *Session) Send(ctx context.Context, body smpp.Body) (smpp.PDU, error) {
	select {
	case s.window <- struct{}{}:
	case <-s.done:
		return smpp.PDU{}, ErrClosed
	case <-ctx.Done():
		return smpp.PDU{}, ctx.Err()
	}
	defer func() { <-s.window }()
	return s.roundtrip(ctx, body)
}

// roundtrip sends a request under a freshly allocated sequence_number and waits for the response
// the read loop routes back. It is bounded by ResponseTimeout, ctx and the session lifetime.
func (s *Session) roundtrip(ctx context.Context, body smpp.Body) (smpp.PDU, error) {
	seq := s.serverSeq.Add(1)
	ch := make(chan smpp.PDU, 1)

	s.mu.Lock()
	s.pending[seq] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, seq)
		s.mu.Unlock()
	}()

	if err := s.write(smpp.PDU{Sequence: seq, Body: body}); err != nil {
		return smpp.PDU{}, err
	}

	timer := time.NewTimer(s.cfg.ResponseTimeout)
	defer timer.Stop()
	select {
	case resp := <-ch:
		return resp, nil
	case <-timer.C:
		return smpp.PDU{}, fmt.Errorf("session: response timeout for sequence %d", seq)
	case <-s.done:
		return smpp.PDU{}, ErrClosed
	case <-ctx.Done():
		return smpp.PDU{}, ctx.Err()
	}
}
