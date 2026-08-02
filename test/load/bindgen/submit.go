package bindgen

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// Defaults applied when SubmitConfig leaves a field at its zero value.
const (
	defaultWindow   = 32
	defaultDestAddr = "33600000000"
)

// defaultShortMessage is the filler body used when SubmitConfig.ShortMessage is empty. Its content is
// irrelevant — the peer under measurement is being asked how many PDUs per second it can absorb, not
// what they say — but its length is not: a one-octet body would understate the wire cost of a real
// 140-octet SMS.
var defaultShortMessage = []byte(
	"bindgen load: 0123456789 0123456789 0123456789 0123456789 0123456789 0123456789 0123456789")

// SMPP v3.4 §4.4.1 sizes source_addr and destination_addr as 21-byte C-octet strings, and
// short_message as at most 254 octets (a larger body would need a message_payload TLV, which a
// throughput probe has no use for).
const (
	maxAddrLen         = 20
	maxShortMessageLen = 254
)

// SubmitConfig turns a run into a submit_sm injector: instead of holding the bound sessions idle, the
// generator pushes submit_sm on every one of them for the whole hold window and matches the responses
// back. It exists to find a peer's throughput ceiling, so emission is windowed rather than
// turn-by-turn — a request/response ping-pong measures the peer's latency, not its ceiling.
//
// The counters it feeds into Report are a diagnostic ("did the injector actually push?"), not the
// measurement: the reference figure is read from the peer's own instrumentation.
type SubmitConfig struct {
	// Window is how many submit_sm may be in flight at once on ONE session, i.e. written but not yet
	// answered. Zero means 32. One degrades the injector to turn-by-turn on purpose.
	Window int
	// Count is how many submit_sm to send per session. Zero means "keep sending until the hold window
	// closes", which is what a ceiling measurement wants; a small Count is for tests and smoke runs.
	Count int
	// SourceAddr is the submit_sm source_addr, at most 20 characters. May be empty.
	SourceAddr string
	// DestAddr is the submit_sm destination_addr, at most 20 characters. Empty means a fixed filler
	// MSISDN: every submission carries the same one, since routing is not what is being measured.
	DestAddr string
	// ShortMessage is the message body, at most 254 octets. Empty means a fixed ~90-octet filler.
	ShortMessage []byte
}

func (s SubmitConfig) window() int {
	if s.Window <= 0 {
		return defaultWindow
	}
	return s.Window
}

func (s SubmitConfig) destAddr() string {
	if s.DestAddr == "" {
		return defaultDestAddr
	}
	return s.DestAddr
}

func (s SubmitConfig) shortMessage() []byte {
	if len(s.ShortMessage) == 0 {
		return defaultShortMessage
	}
	return s.ShortMessage
}

// injector drives the submit_sm emission of one run. One session struct per bound connection, indexed
// the same way holdAndWatch indexes them, so a response read by the watcher finds its own session.
type injector struct {
	cfg      SubmitConfig
	sessions []*session
	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// session is the per-connection injection state: the in-flight window and the counters.
//
// The counters are deliberately not atomic and not locked. Each is written by exactly one goroutine —
// submitted/errors/firstErr by that session's writer, accepted/rejected by the watcher reading that
// same connection — and read only after both have been joined. inFlight is the one piece of genuinely
// shared state, and it has the mutex.
type session struct {
	slots chan struct{} // one token per free in-flight slot

	mu       sync.Mutex
	inFlight map[uint32]struct{}

	seq       uint32 // writer-only: next sequence number
	submitted int
	errors    int
	firstErr  error

	accepted int
	rejected int
}

// newInjector prepares the injection state for n bound sessions. It sends nothing until start.
func newInjector(cfg SubmitConfig, n int) *injector {
	inj := &injector{cfg: cfg, sessions: make([]*session, n), done: make(chan struct{})}
	for i := range inj.sessions {
		s := &session{
			slots:    make(chan struct{}, cfg.window()),
			inFlight: make(map[uint32]struct{}),
			// Sequences 1 and 2 belong to the bind and the unbind of this session, so submissions start
			// past them: a response cannot then be ambiguous about which exchange it answers.
			seq: 2,
		}
		for range cfg.window() {
			s.slots <- struct{}{}
		}
		inj.sessions[i] = s
	}
	return inj
}

// start opens fire on every session at once and returns immediately. Emission stops at deadline, on
// context cancellation, or when stop is called — whichever comes first.
func (inj *injector) start(ctx context.Context, conns []net.Conn, deadline time.Time) {
	for i, nc := range conns {
		inj.wg.Add(1)
		go func() {
			defer inj.wg.Done()
			inj.pump(ctx, inj.sessions[i], nc, deadline)
		}()
	}
}

// stop ends emission and waits for every writer to leave. It is safe to call more than once.
func (inj *injector) stop() {
	inj.stopOnce.Do(func() { close(inj.done) })
	inj.wg.Wait()
}

// pump writes submit_sm on one session until its budget, the deadline or a stop signal ends it.
//
// The write deadline is armed once, absolutely, before the cancellation hook — same reasoning as
// watch's read deadline: re-arming it after the hook would silently erase the cancellation. It is a
// real stop condition, not a nicety: a peer that stops reading its socket blocks WritePDU as soon as
// the kernel buffers fill, and nothing else would ever unblock it.
func (inj *injector) pump(ctx context.Context, s *session, nc net.Conn, deadline time.Time) {
	if err := nc.SetWriteDeadline(deadline); err != nil {
		s.fail(err)
		return
	}
	stop := context.AfterFunc(ctx, func() { _ = nc.SetWriteDeadline(time.Now()) })
	defer stop()

	body := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      inj.cfg.SourceAddr,
		DestinationAddr: inj.cfg.destAddr(),
		ESMClass:        smpp.ESMClassDefault,
		ShortMessage:    inj.cfg.shortMessage(),
	}}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	for inj.cfg.Count == 0 || s.submitted < inj.cfg.Count {
		select {
		case <-s.slots:
		case <-inj.done:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
			return
		}

		s.seq++
		s.mu.Lock()
		s.inFlight[s.seq] = struct{}{}
		s.mu.Unlock()

		if err := smpp.WritePDU(nc, smpp.PDU{Sequence: s.seq, Body: body}); err != nil {
			s.mu.Lock()
			delete(s.inFlight, s.seq)
			s.mu.Unlock()
			s.fail(err)
			return
		}
		s.submitted++
	}
}

// fail records a write failure. Only the count and the first cause are kept: a saturating injector
// against a dead peer produces one identical error per submission, and the count is the signal.
func (s *session) fail(err error) {
	s.errors++
	if s.firstErr == nil {
		s.firstErr = err
	}
}

// onPDU is handed to the watcher of session i. It matches a submit_sm_resp back to its submit_sm by
// sequence number and frees the window slot it was holding. Anything else on the wire — a deliver_sm,
// an enquire_link, a response to a sequence we never sent — is ignored: releasing a slot for a PDU we
// did not send would let the window drift open without bound.
func (inj *injector) onPDU(i int, pdu smpp.PDU) {
	if _, ok := pdu.Body.(*smpp.SubmitSMResp); !ok {
		return
	}
	s := inj.sessions[i]

	s.mu.Lock()
	_, ours := s.inFlight[pdu.Sequence]
	delete(s.inFlight, pdu.Sequence)
	s.mu.Unlock()
	if !ours {
		return
	}

	if pdu.Status == smpp.StatusOK {
		s.accepted++
	} else {
		s.rejected++
	}
	select {
	case s.slots <- struct{}{}:
	default: // window already full: a duplicate response, not a freed slot
	}
}

// fill folds the per-session counters into the report. It must run after both stop and the watchers
// have returned — that join is what makes the unsynchronised counters safe to read.
func (inj *injector) fill(rep *Report) {
	for _, s := range inj.sessions {
		rep.Submitted += s.submitted
		rep.Accepted += s.accepted
		rep.Rejected += s.rejected
		rep.SubmitErrors += s.errors
		rep.Unanswered += len(s.inFlight)
		if rep.SubmitErr == nil {
			rep.SubmitErr = s.firstErr
		}
	}
}
