package connectorpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// errBindClosed is returned by a bind operation once the connection is shutting down.
var errBindClosed = errors.New("connectorpool: bind closed")

// BindConfig configures the single outbound SMPP bind of M2. In production these fields come from
// the connectors control plane; at M2 they are injected (env-backed), which is what lets a test
// point the pool at the ephemeral fake SMSC address.
type BindConfig struct {
	Addr                 string
	SystemID             string
	Password             string
	SystemType           string
	DialTimeout          time.Duration
	ResponseTimeout      time.Duration
	EnquireLinkInterval  time.Duration
	EnquireLinkMaxMissed int
	WindowSize           int
}

// bind owns one SMPP connection. Its concurrency model (guide §5/§9): a single writer goroutine is
// the only writer of the socket; a single reader goroutine is the only reader and dispatches each
// response to the caller waiting on its sequence number; an enquire_link ticker keeps the link
// alive. window bounds the number of submit_sm in flight. Every goroutine stops on done, joined by
// Close.
type bind struct {
	conn        net.Conn
	respTimeout time.Duration
	logger      *slog.Logger

	seq     atomic.Uint32
	writeCh chan smpp.PDU
	window  chan struct{}

	mu      sync.Mutex
	pending map[uint32]chan smpp.PDU

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup

	enquireInterval time.Duration
	maxMissed       int
}

// dialAndBind connects, performs the bind handshake and starts the connection's goroutines. It
// returns a ready bind or an error; on any handshake failure it tears the connection down before
// returning.
func dialAndBind(ctx context.Context, cfg BindConfig, logger *slog.Logger) (*bind, error) {
	d := net.Dialer{Timeout: cfg.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("connectorpool: dial %s: %w", cfg.Addr, err)
	}

	window := cfg.WindowSize
	if window < 1 {
		window = 1
	}
	b := &bind{
		conn:            conn,
		respTimeout:     cfg.ResponseTimeout,
		logger:          logger,
		writeCh:         make(chan smpp.PDU, 64),
		window:          make(chan struct{}, window),
		pending:         make(map[uint32]chan smpp.PDU),
		done:            make(chan struct{}),
		enquireInterval: cfg.EnquireLinkInterval,
		maxMissed:       cfg.EnquireLinkMaxMissed,
	}

	b.wg.Add(2)
	go b.writeLoop()
	go b.readLoop()

	resp, err := b.roundtrip(ctx, &smpp.BindTransceiver{BindFields: smpp.BindFields{
		SystemID:         cfg.SystemID,
		Password:         cfg.Password,
		SystemType:       cfg.SystemType,
		InterfaceVersion: smpp.InterfaceVersion34,
	}})
	if err != nil {
		b.shutdown()
		b.wg.Wait()
		return nil, fmt.Errorf("connectorpool: bind handshake: %w", err)
	}
	if resp.Status != smpp.StatusOK {
		b.shutdown()
		b.wg.Wait()
		return nil, fmt.Errorf("connectorpool: bind rejected with status 0x%08x", resp.Status)
	}

	b.wg.Add(1)
	// The enquire_link ticker is bound to the connection's lifetime, not to any request: it uses a
	// background context and stops on b.done. Tying it to the dial ctx would kill keep-alives the
	// moment the caller's context ended.
	//nolint:gosec,contextcheck // G118: background context is correct for a connection-lifetime keep-alive
	go b.enquireLoop()
	return b, nil
}

// Submit sends a submit_sm and returns its submit_sm_resp. It blocks on the window semaphore first,
// so at most WindowSize submissions are ever outstanding. The window slot is released once the
// response arrives or the attempt fails.
func (b *bind) Submit(ctx context.Context, sm *smpp.SubmitSM) (smpp.PDU, error) {
	select {
	case b.window <- struct{}{}:
	case <-b.done:
		return smpp.PDU{}, errBindClosed
	case <-ctx.Done():
		return smpp.PDU{}, ctx.Err()
	}
	defer func() { <-b.window }()
	return b.roundtrip(ctx, sm)
}

// Close unbinds and shuts the connection down, joining every goroutine. It is best-effort: the
// unbind is attempted within the response timeout, then the socket is closed regardless.
func (b *bind) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), b.respTimeout)
	_, _ = b.roundtrip(ctx, &smpp.Unbind{})
	cancel()
	b.shutdown()
	b.wg.Wait()
}

// roundtrip sends a request and waits for the response correlated by sequence number. It is bounded
// by the response timeout, ctx and the connection lifetime.
func (b *bind) roundtrip(ctx context.Context, body smpp.Body) (smpp.PDU, error) {
	seq := b.seq.Add(1)
	ch := make(chan smpp.PDU, 1)

	b.mu.Lock()
	b.pending[seq] = ch
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.pending, seq)
		b.mu.Unlock()
	}()

	select {
	case b.writeCh <- smpp.PDU{Sequence: seq, Body: body}:
	case <-b.done:
		return smpp.PDU{}, errBindClosed
	case <-ctx.Done():
		return smpp.PDU{}, ctx.Err()
	}

	timer := time.NewTimer(b.respTimeout)
	defer timer.Stop()
	select {
	case resp := <-ch:
		return resp, nil
	case <-timer.C:
		return smpp.PDU{}, fmt.Errorf("connectorpool: response timeout for sequence %d", seq)
	case <-b.done:
		return smpp.PDU{}, errBindClosed
	case <-ctx.Done():
		return smpp.PDU{}, ctx.Err()
	}
}

func (b *bind) writeLoop() {
	defer b.wg.Done()
	for {
		select {
		case pdu := <-b.writeCh:
			if err := smpp.WritePDU(b.conn, pdu); err != nil {
				if !b.isClosing() {
					b.logger.Error("connectorpool: write failed", "err", err)
				}
				b.shutdown()
				return
			}
		case <-b.done:
			return
		}
	}
}

func (b *bind) readLoop() {
	defer b.wg.Done()
	for {
		pdu, err := smpp.ReadPDU(b.conn)
		if err != nil {
			if !b.isClosing() {
				b.logger.Error("connectorpool: read failed", "err", err)
			}
			b.shutdown()
			return
		}
		b.dispatch(pdu)
	}
}

// dispatch routes an incoming PDU: a server-initiated request is answered inline; a response is
// handed to the waiter registered for its sequence number.
func (b *bind) dispatch(pdu smpp.PDU) {
	switch pdu.Body.(type) {
	case *smpp.EnquireLink:
		b.enqueue(smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.EnquireLinkResp{}})
	case *smpp.DeliverSM:
		// MO / DLR: acknowledged so the SMSC's window frees; correlation and forwarding are M4.
		b.enqueue(smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.DeliverSMResp{}})
	default:
		b.mu.Lock()
		ch := b.pending[pdu.Sequence]
		b.mu.Unlock()
		if ch != nil {
			select {
			case ch <- pdu:
			default: // waiter already gone (timed out); drop
			}
		}
	}
}

// enqueue writes a PDU unless the bind is shutting down. Used by the reader to answer server
// requests without touching the socket directly (the writer owns it).
func (b *bind) enqueue(pdu smpp.PDU) {
	select {
	case b.writeCh <- pdu:
	case <-b.done:
	}
}

func (b *bind) enquireLoop() {
	defer b.wg.Done()
	t := time.NewTicker(b.enquireInterval)
	defer t.Stop()

	missed := 0
	for {
		select {
		case <-t.C:
			if _, err := b.roundtrip(context.Background(), &smpp.EnquireLink{}); err != nil {
				if b.isClosing() {
					return
				}
				missed++
				if missed >= b.maxMissed {
					b.logger.Warn("connectorpool: enquire_link unanswered, unbinding", "missed", missed)
					b.shutdown()
					return
				}
				continue
			}
			missed = 0
		case <-b.done:
			return
		}
	}
}

func (b *bind) shutdown() {
	b.closeOnce.Do(func() {
		close(b.done)
		_ = b.conn.Close()
	})
}

func (b *bind) isClosing() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}
