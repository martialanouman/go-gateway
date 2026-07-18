// Package fakesmsc is the in-repo fake SMSC (plan §1.8, strategie-de-test §2): a minimal SMPP peer
// built on internal/smpp that unblocks the M2→M7 pipeline before the real simulator (M8) exists.
//
// It accepts a bind, answers submit_sm with a scriptable submit_sm_resp (OK / Throttled / SysErr /
// Delay) and an assigned SMSC message id, answers enquire_link, handles unbind, and can emit a
// deliver_sm (MO or DLR) on demand from a test. It deliberately does NOT model realistic fault
// injection, vendor profiles or volume — that is the real simulator's job at M8.
//
// Use Start in a test (embedded, with automatic cleanup) or New for the standalone `make fake-smsc`
// process.
package fakesmsc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// systemID is the system_id the fake SMSC returns in every bind response.
const systemID = "fake-smsc"

// Config scripts a fake SMSC. All fields are optional: the zero value accepts every bind and
// answers every submit_sm with OK.
type Config struct {
	// OnSubmit decides the response to each submit_sm. When nil, every submission is OK.
	OnSubmit func(smpp.SubmitSM) Resp
	// Addr is the listen address for the standalone process (e.g. ":2775"). Tests leave it empty to
	// get an ephemeral 127.0.0.1 port.
	Addr string
}

// Server is a running fake SMSC.
type Server struct {
	ln     net.Listener
	cfg    Config
	logf   func(string, ...any)
	wg     sync.WaitGroup
	seq    atomic.Uint32 // sequence numbers for server-initiated PDUs (deliver_sm)
	msgSeq atomic.Uint64 // assigns SMSC message ids
	closed atomic.Bool

	mu    sync.Mutex
	conns map[*conn]struct{}
}

// conn is one accepted SMPP connection. A per-connection write mutex serialises the handler loop's
// responses with a test-initiated SendDLR, since a net.Conn is not safe for concurrent writes.
type conn struct {
	nc      net.Conn
	writeMu sync.Mutex
	canRecv bool // bound as receiver or transceiver: eligible to receive deliver_sm
}

// Start launches an embedded fake SMSC on an ephemeral port and registers its shutdown with t. It
// fails the test if it cannot listen.
func Start(t testing.TB, cfg Config) *Server {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("fakesmsc: start: %v", err)
	}
	s.logf = t.Logf
	t.Cleanup(s.Close)
	return s
}

// New starts a fake SMSC listening on cfg.Addr (ephemeral 127.0.0.1 when empty). The caller owns
// the returned Server and must Close it. It is the entry point for the standalone process.
func New(cfg Config) (*Server, error) {
	addr := cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("fakesmsc: listen on %q: %w", addr, err)
	}
	s := &Server{
		ln:    ln,
		cfg:   cfg,
		logf:  func(string, ...any) {},
		conns: make(map[*conn]struct{}),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the address the fake SMSC listens on, including the resolved port.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops accepting, closes every connection and waits for all goroutines to exit.
func (s *Server) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	_ = s.ln.Close()

	s.mu.Lock()
	for c := range s.conns {
		_ = c.nc.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return // listener closed on shutdown
		}
		c := &conn{nc: nc}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go s.serve(c)
	}
}

func (s *Server) serve(c *conn) {
	defer s.wg.Done()
	defer func() {
		_ = c.nc.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()

	for {
		pdu, err := smpp.ReadPDU(c.nc)
		if err != nil {
			if !s.closed.Load() && !errors.Is(err, net.ErrClosed) {
				s.logf("fakesmsc: read: %v", err)
			}
			return
		}
		if done := s.handle(c, pdu); done {
			return
		}
	}
}

// handle answers one request PDU. It returns true when the connection should close (after unbind).
func (s *Server) handle(c *conn, pdu smpp.PDU) bool {
	switch body := pdu.Body.(type) {
	case *smpp.BindTransmitter:
		s.reply(c, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransmitterResp{
			BindRespFields: smpp.BindRespFields{SystemID: systemID},
		}})
	case *smpp.BindReceiver:
		s.markReceiver(c)
		s.reply(c, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindReceiverResp{
			BindRespFields: smpp.BindRespFields{SystemID: systemID},
		}})
	case *smpp.BindTransceiver:
		s.markReceiver(c)
		s.reply(c, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransceiverResp{
			BindRespFields: smpp.BindRespFields{SystemID: systemID},
		}})
	case *smpp.SubmitSM:
		s.handleSubmit(c, pdu.Sequence, body)
	case *smpp.EnquireLink:
		s.reply(c, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.EnquireLinkResp{}})
	case *smpp.Unbind:
		s.reply(c, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.UnbindResp{}})
		return true
	case *smpp.DeliverSMResp:
		// The ESME acking a deliver_sm we sent: nothing to do.
	default:
		// Unknown/unsupported request: a real SMSC would generic_nack; the fake ignores it.
		s.logf("fakesmsc: ignoring command %#x", pdu.CommandID())
	}
	return false
}

// markReceiver flags a connection as bound to receive deliver_sm. It writes c.canRecv under s.mu,
// the same lock sendToReceivers reads it under, so the bind goroutine and a test-driven SendDLR do
// not race on the field.
func (s *Server) markReceiver(c *conn) {
	s.mu.Lock()
	c.canRecv = true
	s.mu.Unlock()
}

func (s *Server) handleSubmit(c *conn, seq uint32, body *smpp.SubmitSM) {
	resp := OK()
	if s.cfg.OnSubmit != nil {
		resp = s.cfg.OnSubmit(*body)
	}
	if resp.delay > 0 {
		time.Sleep(resp.delay)
	}

	out := &smpp.SubmitSMResp{}
	if resp.ok() {
		out.MessageID = s.nextMessageID()
	}
	s.reply(c, smpp.PDU{Status: resp.status, Sequence: seq, Body: out})
}

func (s *Server) nextMessageID() string {
	return fmt.Sprintf("%016x", s.msgSeq.Add(1))
}

func (s *Server) reply(c *conn, pdu smpp.PDU) {
	if err := c.write(pdu); err != nil && !s.closed.Load() {
		s.logf("fakesmsc: write %#x: %v", pdu.CommandID(), err)
	}
}

func (c *conn) write(pdu smpp.PDU) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return smpp.WritePDU(c.nc, pdu)
}
