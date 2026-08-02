package bindgen_test

import (
	"bytes"
	"context"
	"net"
	"strings"
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
//
// The Window=0 case pins the package default, and it is not decoration: a default of 1 would degrade
// every caller that does not name a window — the command-line tool included — into the turn-by-turn
// injector this whole mode exists to avoid, while every other test here names its window explicitly
// and would keep passing.
func TestRunWindowsSubmitsInFlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		window         int // SubmitConfig.Window, 0 to take the package default
		effective      int // the window the injector must then hold
		wantMaxAtMost  int
		wantMinAtLeast int
	}{
		{"turn by turn", 1, 1, 1, 1},
		{"windowed", 32, 32, 32, 16},
		{"package default", 0, 32, 32, 16},
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
			if rep.Submitted < tc.effective {
				t.Fatalf("Submitted = %d, under the %d-wide window: this run proves nothing",
					rep.Submitted, tc.effective)
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
// It also keeps every submission it received, sequence number included, which is the only way to
// assert on what the injector actually put on the wire when the caller named none of it.
type pacingPeer struct {
	addr string

	mu      sync.Mutex
	live    int
	peak    int
	submits []recordedSubmit
}

// recordedSubmit is one submit_sm as the peer saw it: the PDU header's sequence number and the body.
type recordedSubmit struct {
	seq uint32
	sm  smpp.SubmitSM
}

func (p *pacingPeer) record(seq uint32, sm smpp.SubmitSM) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.submits = append(p.submits, recordedSubmit{seq: seq, sm: sm})
}

func (p *pacingPeer) recorded() []recordedSubmit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedSubmit(nil), p.submits...)
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
		switch body := pdu.Body.(type) {
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
			p.record(pdu.Sequence, *body)
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

// Each rejection must name the field it is about. "an error was returned" is satisfied by a validate
// that answers the same thing to all five conditions, and the operator of a load run then reads
// "invalid config" against a six-field struct they have to bisect by hand.
func TestRunRejectsInvalidSubmitConfig(t *testing.T) {
	t.Parallel()

	base := bindgen.Config{
		Addr: "127.0.0.1:1", Binds: 1, SystemID: "esme", Password: "pw",
		Hold: time.Second, Submit: &bindgen.SubmitConfig{},
	}

	tests := []struct {
		name         string
		patch        func(*bindgen.Config)
		wantContains string
	}{
		// The injection window IS the hold window: accepting this would bind, submit nothing, and
		// report a clean zero — indistinguishable from an injector that is broken.
		{"no hold window", func(c *bindgen.Config) { c.Hold = 0 }, "Hold"},
		{"negative window", func(c *bindgen.Config) { c.Submit.Window = -1 }, "Submit.Window"},
		{"negative count", func(c *bindgen.Config) { c.Submit.Count = -1 }, "Submit.Count"},
		// SMPP v3.4 §4.4.1: source_addr and destination_addr are 21-byte C-octet strings, short_message
		// at most 254 octets. Over the limit, every single write would fail on the wire.
		{"source addr over 20 chars", func(c *bindgen.Config) {
			c.Submit.SourceAddr = strings.Repeat("1", 21)
		}, "Submit.SourceAddr"},
		{"dest addr over 20 chars", func(c *bindgen.Config) {
			c.Submit.DestAddr = strings.Repeat("2", 21)
		}, "Submit.DestAddr"},
		{"body over 254 octets", func(c *bindgen.Config) {
			c.Submit.ShortMessage = make([]byte, 255)
		}, "Submit.ShortMessage"},
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
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Errorf("Run error = %q, want it to name %q", err, tc.wantContains)
			}
			if rep.Requested != 0 {
				t.Errorf("a refused config must report nothing, got %+v", rep)
			}
		})
	}
}

// The positive control the rejection table needs. Without it the limits are pinned from one side
// only: shifted from 20 to 19 characters, maxAddrLen still rejects everything the table feeds it,
// and a caller submitting from a legitimate 20-character MSISDN loses the run to a config error.
//
// The run itself cannot bind — nothing listens on port 1 — which is the point: what is under test is
// that Run got past validation and treated the peer as an outcome rather than the config as an error.
func TestRunAcceptsASubmitConfigAtItsLimits(t *testing.T) {
	t.Parallel()

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: "127.0.0.1:1", Binds: 1, SystemID: "esme", Password: "pw",
		Hold: time.Millisecond, DialTimeout: 2 * time.Second,
		Submit: &bindgen.SubmitConfig{
			SourceAddr:   strings.Repeat("1", 20),
			DestAddr:     strings.Repeat("2", 20),
			ShortMessage: bytes.Repeat([]byte("x"), 254),
		},
	})
	if err != nil {
		t.Fatalf("Run: a config at the SMPP limits must be accepted, got %v", err)
	}
	if rep.Requested != 1 {
		t.Errorf("Requested = %d, want 1", rep.Requested)
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

// A run that went perfectly must report zero errors. The pump waits on a free slot and on the end of
// the window in the same select, and Go picks uniformly among the cases that are ready: the instant
// the window closes, a session still holding a free slot — which is every session against a peer fast
// enough to keep the window unsaturated — has both ready, so one time in two the pump takes the slot
// and writes on a connection whose write deadline has just expired. The run then reports i/o timeouts
// it did not suffer, in a tool whose entire value is that its numbers can be trusted.
//
// The window is closed before the pump starts, on purpose. The race is otherwise armed once per run,
// at an instant no test can aim for; here it is armed on the first iteration of every session, which
// is the same select with the same two ready cases. Each session is an independent coin flip, so the
// 20 below leave a 2^-20 chance — about one in a million — of a false green.
func TestRunDoesNotSubmitAfterTheWindowClosed(t *testing.T) {
	t.Parallel()

	const binds = 20
	s := fakesmsc.Start(t, fakesmsc.Config{})

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: s.Addr(), Binds: binds, SystemID: "esme", Password: "pw",
		Hold:   time.Nanosecond,
		Submit: &bindgen.SubmitConfig{},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != binds {
		t.Fatalf("Bound: got %d want %d (errors: %v)", rep.Bound, binds, rep.Errors)
	}
	if rep.SubmitErrors != 0 {
		t.Errorf("SubmitErrors = %d, want 0: the window was already closed, nothing should have been "+
			"written (first: %v)", rep.SubmitErrors, rep.SubmitErr)
	}
	if rep.Submitted != 0 {
		t.Errorf("Submitted = %d, want 0 on a window that was closed before the first slot was taken",
			rep.Submitted)
	}
}

// Submitting is the denominator of the submitted rate, and the only figure telling a caller when the
// injection actually happened — whether its own readings of the peer were taken inside that window.
// Measured around holdAndWatch it is the WATCH window instead, which is a different interval: the
// watchers outlive the writers on every run, and on a Count-bounded one they outlive them by the
// whole idle tail. A rate divided by that tail is wrong by the ratio between the two.
//
// Both directions are asserted. The bounded run rules out the watch window; the unbounded one rules
// out an arbitrarily small constant, which would satisfy the bounded case alone.
func TestReportSubmittingMeasuresTheInjectionWindow(t *testing.T) {
	t.Parallel()

	const hold = 600 * time.Millisecond

	t.Run("bounded run stops the clock with the last writer", func(t *testing.T) {
		t.Parallel()

		s := fakesmsc.Start(t, fakesmsc.Config{})
		rep, err := bindgen.Run(t.Context(), bindgen.Config{
			Addr: s.Addr(), Binds: 1, SystemID: "esme", Password: "pw",
			Hold:   hold,
			Submit: &bindgen.SubmitConfig{Count: 2},
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		// Guards the fixture: without a hold window far longer than the injection, the two intervals
		// are indistinguishable and the assertion below proves nothing.
		if rep.Elapsed < hold {
			t.Fatalf("Elapsed = %s, under the %s hold: the watch window did not outlive the injection here",
				rep.Elapsed, hold)
		}
		if rep.Submitted != 2 {
			t.Fatalf("Submitted = %d, want 2", rep.Submitted)
		}
		if rep.Submitting >= hold/4 {
			t.Errorf("Submitting = %s for two submissions, want well under %s: this is the watch window, "+
				"not the injection window", rep.Submitting, hold/4)
		}
		if rep.Submitting <= 0 {
			t.Errorf("Submitting = %s, want a positive injection window", rep.Submitting)
		}
	})

	t.Run("unbounded run stays open for the whole window", func(t *testing.T) {
		t.Parallel()

		s := fakesmsc.Start(t, fakesmsc.Config{})
		rep, err := bindgen.Run(t.Context(), bindgen.Config{
			Addr: s.Addr(), Binds: 1, SystemID: "esme", Password: "pw",
			Hold:   hold,
			Submit: &bindgen.SubmitConfig{},
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		// Guards the fixture: a window nobody wrote into is not the window being measured.
		if rep.Submitted == 0 {
			t.Fatalf("the injector pushed nothing (%d errors, first: %v)", rep.SubmitErrors, rep.SubmitErr)
		}
		// A writer blocked in WritePDU until the deadline is still injecting, and against a peer that
		// answers in line it is the normal end of a saturating run — hence no assertion on SubmitErrors
		// here, only on where the window ended. The errors are reported below so that a session the peer
		// really did tear down early cannot masquerade as a broken measurement.
		if want := hold * 3 / 4; rep.Submitting < want {
			t.Errorf("Submitting = %s, want at least %s: the writers pushed for the whole hold window "+
				"(%d submit errors, first: %v)", rep.Submitting, want, rep.SubmitErrors, rep.SubmitErr)
		}
		if rep.Submitting > rep.Elapsed {
			t.Errorf("Submitting = %s, over the %s the whole run took", rep.Submitting, rep.Elapsed)
		}
	})
}

// minDefaultBodyLen is the floor the injector's filler body must clear. Its content is irrelevant —
// the peer is being asked how many PDUs per second it absorbs, not what they say — but its length is
// not: a peer measured with a one-octet body reports a ceiling it would never reach on the 140-octet
// PDUs of real traffic.
const minDefaultBodyLen = 64

// Nothing the injector defaults to is visible to its caller, and all of it is load-bearing: an empty
// destination_addr is refused by every real SMSC, and a sequence starting where the bind's did makes
// the peer's answer ambiguous between two exchanges. A run naming nothing but a count must still put
// a usable submit_sm on the wire, and the peer's own record is the only proof of it.
func TestRunSubmitsWithUsableDefaults(t *testing.T) {
	t.Parallel()

	p := startPacingPeer(t, time.Millisecond)
	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: p.addr, Binds: 1, SystemID: "esme", Password: "pw",
		Hold:   500 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Count: 1},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Submitted != 1 {
		t.Fatalf("Submitted = %d, want 1", rep.Submitted)
	}
	got := p.recorded()
	if len(got) != 1 {
		t.Fatalf("the peer recorded %d submit_sm, want 1", len(got))
	}
	sub := got[0]

	// Sequence 1 is the bind's and 2 the unbind's; a submission reusing either could not be paired.
	if sub.seq != 3 {
		t.Errorf("first submit_sm sequence = %d, want 3 (1 is the bind, 2 the unbind)", sub.seq)
	}
	if n := len(sub.sm.ShortMessage); n < minDefaultBodyLen {
		t.Errorf("default short_message = %d octets, want at least %d: a shorter body understates the "+
			"wire cost of the SMS-sized PDUs being measured", n, minDefaultBodyLen)
	}
	if !isDialable(sub.sm.DestinationAddr) {
		t.Errorf("default destination_addr = %q, want a dialable MSISDN", sub.sm.DestinationAddr)
	}
}

// isDialable reports whether addr is a non-empty run of digits, i.e. something an SMSC would accept
// as a destination_addr.
func isDialable(addr string) bool {
	if addr == "" {
		return false
	}
	return strings.IndexFunc(addr, func(r rune) bool { return r < '0' || r > '9' }) < 0
}

// The pump's write deadline is the only thing standing between the harness and a permanent hang: a
// peer that binds and then stops reading its socket blocks WritePDU as soon as the kernel buffers
// fill, and no channel the pump also selects on is ever consulted from inside a blocked write.
//
// Both stop conditions are asserted, one per case, because they cover different failures. The
// absolute deadline armed at start is what ends a run whose peer went deaf; the context hook is what
// ends one the operator interrupted. Either alone leaves the other case hanging.
//
// The fixture proves itself through the outcome it demands: a submission can only be cut short if a
// WritePDU was still running when the window closed under it, since nothing else ever enters a write
// with the window already shut. A pump that had not blocked would leave the select on its own and cut
// nothing short, and the assertion below would say so.
//
// And that outcome is NOT an error. A writer torn out of a blocked write by the end of its own window
// is how a saturating run normally ends; counting it in SubmitErrors puts an "i/o timeout" under the
// eyes of whoever reads the run, on a run that did exactly what it was asked to.
func TestRunEndsAWriterBlockedByADeafPeer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		hold        time.Duration
		cancelAfter time.Duration // 0 leaves the context alone: the absolute write deadline must do it
		wantWithin  time.Duration
	}{
		{"the window closes", 700 * time.Millisecond, 0, 5 * time.Second},
		{"the run is cancelled", 30 * time.Second, 300 * time.Millisecond, 5 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			addr := startDeafPeer(t, 0)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			cfg := bindgen.Config{
				Addr: addr, Binds: 1, SystemID: "esme", Password: "pw",
				Hold: tc.hold, RespTimeout: 200 * time.Millisecond,
				Submit: &bindgen.SubmitConfig{
					// Wide enough that the window is never what stops the writer, and a full-size body so
					// the kernel buffers fill in a few hundred PDUs rather than a few hundred thousand.
					Window:       1 << 18,
					ShortMessage: bytes.Repeat([]byte("x"), 254),
				},
			}
			if tc.cancelAfter > 0 {
				cfg.OnAllBound = func() { time.AfterFunc(tc.cancelAfter, cancel) }
			}

			type result struct {
				rep bindgen.Report
				err error
			}
			done := make(chan result, 1)
			start := time.Now()
			go func() {
				rep, err := bindgen.Run(ctx, cfg)
				done <- result{rep: rep, err: err}
			}()

			var got result
			select {
			case got = <-done:
			case <-time.After(tc.wantWithin):
				t.Fatalf("Run did not return within %s: the writer blocked against a peer that stopped "+
					"reading has no stop condition", tc.wantWithin)
			}
			if got.err != nil {
				t.Fatalf("Run: %v", got.err)
			}
			if elapsed := time.Since(start); elapsed > tc.wantWithin {
				t.Errorf("Run took %s, over the %s it had", elapsed, tc.wantWithin)
			}
			if got.rep.SubmitCutShort != 1 {
				t.Fatalf("SubmitCutShort = %d, want 1: the writer was expected to be blocked in WritePDU "+
					"and torn out of it by the closing window (submitted %d, %d errors, first %v)",
					got.rep.SubmitCutShort, got.rep.Submitted, got.rep.SubmitErrors, got.rep.SubmitErr)
			}
			if got.rep.SubmitErrors != 0 || got.rep.SubmitErr != nil {
				t.Errorf("SubmitErrors = %d (first: %v), want 0 and nil: the window closing on a writer "+
					"is the end of a healthy run, not a failure of the peer",
					got.rep.SubmitErrors, got.rep.SubmitErr)
			}
		})
	}
}

// The other half of the same distinction, and the half that keeps it honest: a write that failed
// while the window was still open is a real failure and must stay in SubmitErrors. Without this run,
// "never count a write failure" satisfies every other assertion in this file — and the injector would
// go quiet about a peer that walked out on it mid-window, which is the loudest thing a ceiling probe
// can be asked to report.
func TestRunCountsAWriteErrorTheWindowDidNotCause(t *testing.T) {
	t.Parallel()

	const hold = 5 * time.Second
	// The same deaf peer as above, and for the same reason: it is the one fixture that puts the writer
	// inside a blocked WritePDU at a moment the test controls. Here it then walks out of the run, far
	// from the end of the window — so the write fails for the peer's reason, not the window's.
	addr := startDeafPeer(t, 300*time.Millisecond)

	rep, err := bindgen.Run(t.Context(), bindgen.Config{
		Addr: addr, Binds: 1, SystemID: "esme", Password: "pw",
		Hold: hold, RespTimeout: 200 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{
			Window:       1 << 18,
			ShortMessage: bytes.Repeat([]byte("x"), 254),
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Guards the fixture: the writer has to have died with the window still wide open, otherwise this
	// run is the deaf-peer case above under another name.
	if rep.Submitting >= hold/2 {
		t.Fatalf("Submitting = %s of a %s window: the writer survived to the end and this run says "+
			"nothing about a mid-window failure", rep.Submitting, hold)
	}
	if rep.Submitted == 0 {
		t.Fatalf("the injector pushed nothing before the peer left (%d errors, first: %v)",
			rep.SubmitErrors, rep.SubmitErr)
	}

	if rep.SubmitErrors != 1 {
		t.Errorf("SubmitErrors = %d, want 1: the peer went away mid-window and the write that hit the "+
			"closed socket is a failure (cut short %d, first: %v)",
			rep.SubmitErrors, rep.SubmitCutShort, rep.SubmitErr)
	}
	if rep.SubmitErr == nil {
		t.Error("SubmitErr = nil, want the cause of the failed write kept for diagnosis")
	}
	if rep.SubmitCutShort != 0 {
		t.Errorf("SubmitCutShort = %d, want 0: the window was still open, nothing was cut short by it",
			rep.SubmitCutShort)
	}
}

// startDeafPeer runs an SMPP peer that completes one bind_transceiver per connection and then never
// reads that socket again — the shape of a peer whose reader thread wedged, which is what a write
// deadline exists for. Its receive buffer is deliberately small so the writer blocks quickly.
//
// A non-zero walkOut then tears the connection down that long after the bind, turning the same peer
// into one that walks out on a blocked writer: same blocked write, a different reason for it to end.
//
// The connections are held open until the test ends, then closed: a writer still blocked on one when
// the test gives up is released by that close instead of leaking for the rest of the binary's life.
func startDeafPeer(t *testing.T, walkOut time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, nc := range conns {
			_ = nc.Close()
		}
	})

	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return // listener closed with the test
			}
			mu.Lock()
			conns = append(conns, nc)
			mu.Unlock()

			go func() {
				if tc, ok := nc.(*net.TCPConn); ok {
					_ = tc.SetReadBuffer(4096)
				}
				pdu, err := smpp.ReadPDU(nc)
				if err != nil {
					return
				}
				if _, ok := pdu.Body.(*smpp.BindTransceiver); !ok {
					return
				}
				if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransceiverResp{
					BindRespFields: smpp.BindRespFields{SystemID: "deaf-peer"},
				}}); err != nil {
					return
				}
				// And that is the last read this connection will ever get.
				if walkOut > 0 {
					time.AfterFunc(walkOut, func() { _ = nc.Close() })
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// TestReportSpreadsTheQuietestSessionApart is the signal a sweep needs to tell a stalled bind from a
// healthy one. Unanswered cannot do it: a windowed injector ends every run with its whole window in
// flight on every session, healthy or not, so the tail is binds*Window either way. What separates
// them is how much each session got through — a session the peer stopped serving submits a fraction
// of what its siblings do, and only a per-session figure exposes that.
func TestReportSpreadsTheQuietestSessionApart(t *testing.T) {
	t.Parallel()

	p := startPacingPeer(t, time.Millisecond)
	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr: p.addr, Binds: 4, SystemID: "loadgen", Password: "pw",
		Hold:   400 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Window: 8},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != 4 {
		t.Fatalf("Bound = %d, want 4 — the fixture never reached the condition", rep.Bound)
	}
	if rep.Submitted == 0 {
		t.Fatalf("Submitted = 0, the injector pushed nothing")
	}
	if rep.SubmittedMin <= 0 || rep.SubmittedMax <= 0 {
		t.Errorf("SubmittedMin/Max = %d/%d, want both above zero", rep.SubmittedMin, rep.SubmittedMax)
	}
	if rep.SubmittedMin > rep.SubmittedMax {
		t.Errorf("SubmittedMin = %d > SubmittedMax = %d", rep.SubmittedMin, rep.SubmittedMax)
	}
	if rep.SubmittedMax > rep.Submitted {
		t.Errorf("SubmittedMax = %d > Submitted = %d, a single session cannot exceed the total",
			rep.SubmittedMax, rep.Submitted)
	}
	// Every session faced the same peer, so the spread must be narrow. A guard that fires here would
	// be useless in the field.
	if rep.SubmittedMin*2 < rep.SubmittedMax {
		t.Errorf("SubmittedMin = %d, SubmittedMax = %d: sessions facing an identical peer diverged by more than 2x",
			rep.SubmittedMin, rep.SubmittedMax)
	}
}

// TestReportSeparatesMinFromMaxAcrossUnevenSessions pins what a uniform peer cannot: that the two
// figures really are the smallest and the largest, and not the same number twice. Every session facing
// an identical peer submits roughly the same amount, so swapping the comparison — or reporting the
// total on both — survives a test written against one. The sweep refuses tiers on the gap between
// them, so the gap has to be measured against a peer that actually produces one.
func TestReportSeparatesMinFromMaxAcrossUnevenSessions(t *testing.T) {
	t.Parallel()

	// Half the sessions are served ten times slower than the other half.
	p := startUnevenPeer(t, time.Millisecond, 10*time.Millisecond)
	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr: p.addr, Binds: 4, SystemID: "loadgen", Password: "pw",
		Hold:   700 * time.Millisecond,
		Submit: &bindgen.SubmitConfig{Window: 4},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != 4 {
		t.Fatalf("Bound = %d, want 4 — the fixture never reached the condition", rep.Bound)
	}
	// Fixture guard: the peer must really have produced an uneven run, or this proves nothing.
	if rep.SubmittedMin*2 > rep.SubmittedMax {
		t.Fatalf("min %d, max %d: either the slow sessions were not slow enough (fixture), or the two figures are not the smallest and the largest (code)",
			rep.SubmittedMin, rep.SubmittedMax)
	}
	if rep.SubmittedMin >= rep.SubmittedMax {
		t.Errorf("SubmittedMin = %d, SubmittedMax = %d: the two are not ordered",
			rep.SubmittedMin, rep.SubmittedMax)
	}
	if rep.SubmittedMin+rep.SubmittedMax > rep.Submitted {
		t.Errorf("SubmittedMin+SubmittedMax = %d > Submitted = %d: neither can be the total",
			rep.SubmittedMin+rep.SubmittedMax, rep.Submitted)
	}
}

// startUnevenPeer answers on every connection, but paces the odd-numbered ones far slower than the
// even ones — the shape a peer that stopped serving a subset of its binds produces.
func startUnevenPeer(t *testing.T, fast, slow time.Duration) *pacingPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	p := &pacingPeer{addr: ln.Addr().String()}
	var n int
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			pace := fast
			if n%2 == 1 {
				pace = slow
			}
			n++
			t.Cleanup(func() { _ = c.Close() })
			go p.serve(c, pace)
		}
	}()
	return p
}
