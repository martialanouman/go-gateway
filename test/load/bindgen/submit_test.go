package bindgen_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/test/load/bindgen"
)

// The injector must push on EVERY bound session, not just the first: the point of the mode is to
// measure a peer's aggregate submit_sm ceiling, and a peer's ceiling is reached by all of its binds
// at once. Asserting on the peer's own per-connection record is the only proof of that.
func TestRunSubmitsOnEverySession(t *testing.T) {
	t.Parallel()

	const binds = 5
	s := fakesmsc.Start(t, fakesmsc.Config{RecordSubmits: true})

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: s.Addr(), Binds: binds, SystemID: "esme", Password: "pw",
		Hold:   200 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Count: 4, DestAddr: "33600000001"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != binds {
		t.Fatalf("Bound: got %d want %d (errors: %v)", rep.Bound, binds, rep.Errors)
	}

	sessions := make(map[int]int)
	for _, sub := range s.Submits() {
		if sub.SM.DestinationAddr != "33600000001" {
			t.Fatalf("destination_addr = %q, want %q", sub.SM.DestinationAddr, "33600000001")
		}
		sessions[sub.ConnID]++
	}
	if len(sessions) != binds {
		t.Errorf("sessions that received a submit_sm = %d, want %d (per-session counts: %v)",
			len(sessions), binds, sessions)
	}
	for id, n := range sessions {
		if n != 4 {
			t.Errorf("session %d received %d submit_sm, want %d", id, n, 4)
		}
	}

	if rep.Submitted != binds*4 {
		t.Errorf("Submitted = %d, want %d", rep.Submitted, binds*4)
	}
	if rep.Accepted != binds*4 {
		t.Errorf("Accepted = %d, want %d", rep.Accepted, binds*4)
	}
	if rep.Rejected != 0 {
		t.Errorf("Rejected = %d, want 0", rep.Rejected)
	}
	if rep.Unanswered != 0 {
		t.Errorf("Unanswered = %d, want 0", rep.Unanswered)
	}
	if rep.SubmitErrors != 0 {
		t.Errorf("SubmitErrors = %d, want 0 (first: %v)", rep.SubmitErrors, rep.SubmitErr)
	}
	if rep.Submitting <= 0 {
		t.Error("Submitting: expected a positive injection window, needed to compute a rate")
	}
}

// The whole point of the mode: emission must be WINDOWED. A turn-by-turn injector waits for each
// submit_sm_resp before sending the next, so it measures the peer's round-trip latency and reports it
// as a ceiling — against a peer answering in 10ms it would claim 100 SMS/s per session no matter how
// much the peer could really absorb.
//
// The assertion is made on the peer's side, on how many submit_sm it holds unanswered at once, since
// that is the only thing that tells the two designs apart. The Window=1 case is there to prove the
// fixture measures what it claims: it drives the injector into the very behaviour being ruled out and
// the same peer must then observe exactly one.
func TestRunWindowsSubmitsInFlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		window         int
		wantMaxAtMost  int
		wantMinAtLeast int
	}{
		{"turn by turn", 1, 1, 1},
		{"windowed", 32, 32, 16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := startPacingPeer(t, 10*time.Millisecond)
			rep, err := bindgen.Run(t.Context(), bindgen.Config{
				Addr: p.addr, Binds: 1, SystemID: "esme", Password: "pw",
				Hold:   400 * time.Millisecond,
				Submit: &bindgen.SubmitConfig{Window: tc.window},
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if rep.Bound != 1 {
				t.Fatalf("Bound: got %d want 1 (errors: %v)", rep.Bound, rep.Errors)
			}
			// Guards the fixture: a run that pushed less than a full window could not have exercised one.
			if rep.Submitted < tc.window {
				t.Fatalf("Submitted = %d, under the %d-wide window: this run proves nothing",
					rep.Submitted, tc.window)
			}

			if got := p.maxInFlight(); got < tc.wantMinAtLeast || got > tc.wantMaxAtMost {
				t.Errorf("peak submit_sm in flight at the peer = %d, want between %d and %d",
					got, tc.wantMinAtLeast, tc.wantMaxAtMost)
			}
		})
	}
}

// pacingPeer answers every submit_sm, but only one every `pace`, and records the high-water mark of
// submissions it holds unanswered. Reading and answering are on separate goroutines on purpose: a peer
// that read one PDU per response would flatten every window down to whatever its own loop allows, and
// would measure nothing.
type pacingPeer struct {
	addr string

	mu   sync.Mutex
	live int
	peak int
}

func (p *pacingPeer) enter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live++
	if p.live > p.peak {
		p.peak = p.live
	}
}

func (p *pacingPeer) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live--
}

func (p *pacingPeer) maxInFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

func startPacingPeer(t *testing.T, pace time.Duration) *pacingPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	p := &pacingPeer{addr: ln.Addr().String()}

	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return // listener closed with the test
			}
			go p.serve(nc, pace)
		}
	}()
	return p
}

func (p *pacingPeer) serve(nc net.Conn, pace time.Duration) {
	defer func() { _ = nc.Close() }()

	var writeMu sync.Mutex
	pending := make(chan uint32, 4096)
	var answering sync.WaitGroup
	answering.Add(1)
	go func() {
		defer answering.Done()
		for seq := range pending { // closed by the read loop on its way out: the stop condition
			time.Sleep(pace)
			// Decrement BEFORE answering, never after: the client can write its next submit_sm the
			// instant the response lands, and the read loop would then count it while this one still
			// looked outstanding — a phantom in-flight of 2 on a strictly turn-by-turn exchange.
			p.leave()
			writeMu.Lock()
			err := smpp.WritePDU(nc, smpp.PDU{Sequence: seq, Body: &smpp.SubmitSMResp{
				MessageIDResp: smpp.MessageIDResp{MessageID: "pacing-peer"},
			}})
			writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	defer answering.Wait()
	defer close(pending)

	for {
		pdu, err := smpp.ReadPDU(nc)
		if err != nil {
			return
		}
		switch pdu.Body.(type) {
		case *smpp.BindTransceiver:
			writeMu.Lock()
			err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransceiverResp{
				BindRespFields: smpp.BindRespFields{SystemID: "pacing-peer"},
			}})
			writeMu.Unlock()
			if err != nil {
				return
			}
		case *smpp.SubmitSM:
			p.enter()
			pending <- pdu.Sequence
		case *smpp.Unbind:
			writeMu.Lock()
			_ = smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.UnbindResp{}})
			writeMu.Unlock()
			return
		}
	}
}

// A windowed injector has several submissions outstanding at once, so nothing guarantees the peer
// answers them in the order it received them — a real SMSC with a worker pool routinely does not.
// Responses must therefore be paired to their submit_sm by sequence number, and a submit_sm_resp
// bearing a sequence we never sent must be ignored rather than credited to some submission of ours.
func TestRunPairsResponsesBySequence(t *testing.T) {
	t.Parallel()

	const perSession = 24
	p := startShufflingPeer(t)

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: p.addr, Binds: 1, SystemID: "esme", Password: "pw",
		Hold:   2 * time.Second,
		Submit: &bindgen.SubmitConfig{Window: 8, Count: perSession},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Guards the fixture: without responses that arrived out of order, and without unsolicited ones,
	// the run says nothing about pairing.
	if got := p.reversedFlushes(); got < 2 {
		t.Fatalf("the peer only answered %d batches in reverse order: this run would not exercise pairing", got)
	}
	if got := p.strays(); got == 0 {
		t.Fatal("the peer sent no unsolicited submit_sm_resp: this run would not exercise sequence matching")
	}

	if rep.Submitted != perSession {
		t.Fatalf("Submitted = %d, want %d", rep.Submitted, perSession)
	}
	if rep.Accepted != perSession {
		t.Errorf("Accepted = %d, want %d — every submission was answered, whatever the order", rep.Accepted, perSession)
	}
	if rep.Unanswered != 0 {
		t.Errorf("Unanswered = %d, want 0", rep.Unanswered)
	}
	if rep.Rejected != 0 {
		t.Errorf("Rejected = %d, want 0", rep.Rejected)
	}
}

// shufflingPeer accepts every submit_sm, holds them briefly, then answers each batch in reverse
// arrival order. It also slips one submit_sm_resp per batch bearing a sequence number nobody sent.
type shufflingPeer struct {
	addr string

	mu       sync.Mutex
	reversed int
	stray    int
}

func (p *shufflingPeer) reversedFlushes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reversed
}

func (p *shufflingPeer) strays() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stray
}

// straySequence is far past anything the injector can reach in a test-sized run, so a response
// carrying it cannot be mistaken for a late answer to a real submission.
const straySequence = uint32(0x7fff0000)

func startShufflingPeer(t *testing.T) *shufflingPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	p := &shufflingPeer{addr: ln.Addr().String()}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return // listener closed with the test
			}
			go p.serve(nc)
		}
	}()
	return p
}

func (p *shufflingPeer) serve(nc net.Conn) {
	defer func() { _ = nc.Close() }()

	var (
		writeMu sync.Mutex
		pendMu  sync.Mutex
		pending []uint32
	)
	flush := make(chan struct{})
	var flushing sync.WaitGroup

	writeResp := func(seq uint32) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return smpp.WritePDU(nc, smpp.PDU{Sequence: seq, Body: &smpp.SubmitSMResp{
			MessageIDResp: smpp.MessageIDResp{MessageID: "shuffling-peer"},
		}})
	}

	flushing.Add(1)
	go func() {
		defer flushing.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-flush: // closed by the read loop on its way out: the stop condition
				return
			case <-ticker.C:
			}

			pendMu.Lock()
			batch := pending
			pending = nil
			pendMu.Unlock()
			if len(batch) == 0 {
				continue
			}

			// An unsolicited response first, so a credulous injector credits it before the real ones.
			if err := writeResp(straySequence); err != nil {
				return
			}
			p.mu.Lock()
			p.stray++
			if len(batch) > 1 {
				p.reversed++
			}
			p.mu.Unlock()

			for i := len(batch) - 1; i >= 0; i-- {
				if err := writeResp(batch[i]); err != nil {
					return
				}
			}
		}
	}()
	defer flushing.Wait()
	defer close(flush)

	for {
		pdu, err := smpp.ReadPDU(nc)
		if err != nil {
			return
		}
		switch pdu.Body.(type) {
		case *smpp.BindTransceiver:
			writeMu.Lock()
			err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransceiverResp{
				BindRespFields: smpp.BindRespFields{SystemID: "shuffling-peer"},
			}})
			writeMu.Unlock()
			if err != nil {
				return
			}
		case *smpp.SubmitSM:
			pendMu.Lock()
			pending = append(pending, pdu.Sequence)
			pendMu.Unlock()
		case *smpp.Unbind:
			writeMu.Lock()
			_ = smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.UnbindResp{}})
			writeMu.Unlock()
			return
		}
	}
}

// A peer refusing the traffic is the normal answer above its ceiling, and the reason the injector
// separates Accepted from Rejected: a run where every submission came back ESME_RTHROTTLED reached
// the peer's limit, and reporting it as generic success would hide exactly what was being measured.
func TestRunCountsRejectedSubmits(t *testing.T) {
	t.Parallel()

	const perSession = 6
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() },
	})

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: s.Addr(), Binds: 2, SystemID: "esme", Password: "pw",
		Hold:   500 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Window: 4, Count: perSession},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Submitted != 2*perSession {
		t.Fatalf("Submitted = %d, want %d", rep.Submitted, 2*perSession)
	}
	if rep.Rejected != 2*perSession {
		t.Errorf("Rejected = %d, want %d", rep.Rejected, 2*perSession)
	}
	if rep.Accepted != 0 {
		t.Errorf("Accepted = %d, want 0 — the peer threw every submission back", rep.Accepted)
	}
	if rep.Unanswered != 0 {
		t.Errorf("Unanswered = %d, want 0 — a rejection is still an answer", rep.Unanswered)
	}
}

// The mode is opt-in and must stay so: a run without a SubmitConfig is the bind probe of step-200,
// unchanged, and must not put a single submit_sm on the wire.
func TestRunWithoutSubmitConfigSubmitsNothing(t *testing.T) {
	t.Parallel()

	s := fakesmsc.Start(t, fakesmsc.Config{RecordSubmits: true})
	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: s.Addr(), Binds: 3, SystemID: "esme", Password: "pw",
		Hold: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != 3 {
		t.Fatalf("Bound: got %d want 3 (errors: %v)", rep.Bound, rep.Errors)
	}
	if n := len(s.Submits()); n != 0 {
		t.Errorf("the peer received %d submit_sm, want none", n)
	}
	if rep.Submitted != 0 || rep.Accepted != 0 || rep.Rejected != 0 || rep.Unanswered != 0 {
		t.Errorf("submit counters = %+v, want all zero without a SubmitConfig", rep)
	}
	if rep.Submitting != 0 {
		t.Errorf("Submitting = %s, want 0 without a SubmitConfig", rep.Submitting)
	}
}

func TestRunRejectsInvalidSubmitConfig(t *testing.T) {
	t.Parallel()

	base := bindgen.Config{
		Addr: "127.0.0.1:1", Binds: 1, SystemID: "esme", Password: "pw",
		Hold: time.Second, Submit: &bindgen.SubmitConfig{},
	}

	tests := []struct {
		name  string
		patch func(*bindgen.Config)
	}{
		// The injection window IS the hold window: accepting this would bind, submit nothing, and
		// report a clean zero — indistinguishable from an injector that is broken.
		{"no hold window", func(c *bindgen.Config) { c.Hold = 0 }},
		{"negative window", func(c *bindgen.Config) { c.Submit.Window = -1 }},
		{"negative count", func(c *bindgen.Config) { c.Submit.Count = -1 }},
		// SMPP v3.4 §4.4.1: source_addr and destination_addr are 21-byte C-octet strings, short_message
		// at most 254 octets. Over the limit, every single write would fail on the wire.
		{"source addr over 20 chars", func(c *bindgen.Config) { c.Submit.SourceAddr = "012345678901234567890" }},
		{"dest addr over 20 chars", func(c *bindgen.Config) { c.Submit.DestAddr = "012345678901234567890" }},
		{"body over 254 octets", func(c *bindgen.Config) { c.Submit.ShortMessage = make([]byte, 255) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			sub := *base.Submit
			cfg.Submit = &sub
			tc.patch(&cfg)

			rep, err := bindgen.Run(t.Context(), cfg)
			if err == nil {
				t.Fatalf("Run: expected a config error, got report %+v", rep)
			}
			if rep.Requested != 0 {
				t.Errorf("a refused config must report nothing, got %+v", rep)
			}
		})
	}
}

// Every injector goroutine needs a stop condition that is not "the peer eventually answers". A peer
// that goes silent mid-window leaves the writers blocked on a full window, and only cancellation can
// get them out; a run that hung there would take the whole hold window with it.
func TestRunEndsTheInjectionOnCancel(t *testing.T) {
	t.Parallel()

	s := fakesmsc.Start(t, fakesmsc.Config{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	start := time.Now()
	rep, err := bindgen.Run(ctx, bindgen.Config{
		Addr: s.Addr(), Binds: 3, SystemID: "esme", Password: "pw",
		Hold:       time.Hour,
		Submit:     &bindgen.SubmitConfig{Window: 16},
		OnAllBound: cancel,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("a cancelled injection must return promptly, took %s", elapsed)
	}
	if rep.Bound != 3 {
		t.Errorf("Bound: got %d want 3 (errors: %v)", rep.Bound, rep.Errors)
	}
	waitFor(t, "the peer to see no live connection", func() bool { return s.ConnCount() == 0 })
}

// The window is a cap, not a hint. Against a peer that binds and then never answers a submit_sm, an
// injector holding its window honestly puts exactly Window submissions on the wire and then waits;
// one that merely tries to keep Window in flight would keep writing and drown a peer whose in-flight
// budget is precisely what is being measured. It must also come back when the window closes, since
// every writer is by then blocked on a slot no response will ever free.
func TestRunBoundsInFlightWhenThePeerGoesSilent(t *testing.T) {
	t.Parallel()

	const window = 16
	addr := startSlowPeer(t, 0) // binds, then swallows every submit_sm without answering

	start := time.Now()
	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: addr, Binds: 1, SystemID: "esme", Password: "pw",
		Hold:   300 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Window: window},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("a silent peer must not hold the run open, took %s", elapsed)
	}
	if rep.Submitted != window {
		t.Errorf("Submitted = %d, want exactly %d: the window did not cap what went on the wire",
			rep.Submitted, window)
	}
	if rep.Unanswered != window {
		t.Errorf("Unanswered = %d, want %d", rep.Unanswered, window)
	}
	if rep.Accepted != 0 || rep.Rejected != 0 {
		t.Errorf("Accepted=%d Rejected=%d, want 0 and 0 — the peer answered nothing", rep.Accepted, rep.Rejected)
	}
}
