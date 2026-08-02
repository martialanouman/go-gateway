// Package bindgen opens N concurrent SMPP sessions against a peer and reports what happened.
//
// It is a load tool, not production code. By default it answers "does this peer accept N simultaneous
// binds, and how many does it drop?", nothing more: it submits nothing and reads no MO/DLR — once a
// session is bound it is held idle until the hold window ends, then unbound.
//
// With a Config.Submit it becomes a submit_sm injector as well (step-201): every bound session pushes
// traffic for the whole hold window, windowed rather than turn-by-turn, so the peer is asked for its
// throughput ceiling rather than its round-trip latency. See SubmitConfig. The mode is opt-in and the
// idle behaviour above is what a nil Submit still gets.
//
// The dial-and-bind exchange is deliberately duplicated here rather than borrowed from
// internal/connectorpool: the production client is unexported and stays that way, and a load harness
// must not be able to break it (step-200 D4). The two are ~30 lines of the same SMPP v3.4 §4.1.1
// handshake and are expected to drift apart.
//
// The importable package holds the logic so it can be tested from the outside; the command-line
// entry point is a separate, thin main.
package bindgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// SMPP v3.4 §4.1.1 sizes system_id as a 16-byte and password as a 9-byte C-octet string, i.e. this
// many useful characters plus the NUL. The codec does not enforce it on the way out — an over-long
// value is silently truncated when the peer decodes it — so the check belongs here.
const (
	maxSystemIDLen = 15
	maxPasswordLen = 8
)

// Default timeouts, applied when Config leaves them at zero.
const (
	defaultDialTimeout = 5 * time.Second
	defaultRespTimeout = 5 * time.Second
)

// Config describes one run of the generator.
type Config struct {
	// Addr is the peer's host:port, e.g. "127.0.0.1:2775". Required.
	Addr string
	// Binds is the number of sessions to open simultaneously. Must be at least 1.
	Binds int
	// SystemID is the bind system_id, shared by every session. Required, at most 15 characters.
	SystemID string
	// Password is the bind password, at most 8 characters. May be empty if the peer allows it.
	Password string
	// SystemType is the optional bind system_type.
	SystemType string
	// Hold is how long the bound sessions stay open once every bind attempt has settled. Zero unbinds
	// immediately. Cancelling the context ends the hold early.
	Hold time.Duration
	// DialTimeout caps the TCP connect of one session. Zero means five seconds.
	DialTimeout time.Duration
	// RespTimeout caps the wait for one bind_transceiver_resp, and the unbind exchange at teardown.
	// Zero means five seconds.
	RespTimeout time.Duration
	// OnAllBound, when non-nil, runs once every bind attempt has settled and the successful sessions
	// are all still open, just before the hold window. It runs on Run's own goroutine, so a slow
	// callback delays the run.
	OnAllBound func()
	// Submit, when non-nil, turns the hold window into a submit_sm injection window: every bound
	// session pushes traffic for the whole of Config.Hold instead of staying idle. Nil — the default —
	// leaves the generator a pure bind probe that submits nothing. It requires a positive Hold.
	Submit *SubmitConfig
}

// Report is the outcome of a run.
type Report struct {
	// Requested is the number of sessions attempted, i.e. Config.Binds.
	Requested int
	// Bound is how many reached a bind_transceiver_resp with command_status ESME_ROK.
	Bound int
	// Failed is how many did not, for any reason: dial, write, read, timeout or rejection.
	Failed int
	// Dropped is how many sessions bound successfully and were then torn down by the peer during
	// Config.Hold. It is a subset of Bound, not of Failed: the bind itself did succeed. A peer over
	// its ceiling typically accepts every session and drops the surplus a moment later, which is
	// indistinguishable from a healthy run unless someone watches. Always zero without a hold window.
	Dropped int
	// Errors holds one error per failed session, in no particular order.
	Errors []error
	// Elapsed is the wall-clock duration of the run, hold and teardown included.
	Elapsed time.Duration

	// The fields below are the submit_sm injector's, and stay zero without a Config.Submit.
	//
	// They are a diagnostic, not the measurement: they answer "did the injector push, and did the peer
	// answer?". The throughput figure a run exists to produce is read from the peer's own
	// instrumentation, which counts what it really processed rather than what we managed to write.

	// Submitted is how many submit_sm reached the wire, across every session.
	Submitted int
	// Accepted is how many of them were answered with a submit_sm_resp carrying ESME_ROK.
	Accepted int
	// Rejected is how many were answered with a non-zero command_status — throttling, above all, which
	// is the expected answer from a peer being pushed past its ceiling.
	Rejected int
	// Unanswered is how many were still in flight when the window closed: the peer never answered them,
	// or did not answer in time. Some are normal at the end of any windowed run.
	Unanswered int
	// SubmitErrors is how many submissions failed before the peer could see them: a write error, a
	// write deadline, a session torn down mid-run.
	SubmitErrors int
	// SubmitErr is the first of those causes, kept for diagnosis. The rest are counted only: a
	// saturating injector against a dead peer produces one identical error per submission.
	SubmitErr error
	// Submitting is how long the injection window stayed open — the denominator of the submitted rate.
	// It is not Elapsed, which also covers binding and teardown.
	Submitting time.Duration
}

// Run opens Config.Binds SMPP transceiver sessions against the peer at the same time, holds them for
// Config.Hold, then unbinds them all and returns what happened.
//
// It returns an error only when the run could not be attempted at all — an invalid Config. A session
// that fails to bind is an outcome, not an error: it is counted in Report.Failed and its cause
// appended to Report.Errors. Run always tears down every session it opened before returning, so no
// goroutine and no connection outlives the call.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if err := cfg.validate(); err != nil {
		return Report{}, err
	}
	start := time.Now()

	type outcome struct {
		conn net.Conn
		err  error
	}
	outcomes := make([]outcome, cfg.Binds)

	// Every goroutine is spawned first and released together, so the binds race each other for the
	// peer instead of trickling in at goroutine-creation speed.
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range outcomes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			nc, err := bind(ctx, cfg)
			outcomes[i] = outcome{conn: nc, err: err}
		}()
	}
	close(gate)
	wg.Wait()

	rep := Report{Requested: cfg.Binds}
	conns := make([]net.Conn, 0, cfg.Binds)
	for _, o := range outcomes {
		if o.err != nil {
			rep.Failed++
			rep.Errors = append(rep.Errors, o.err)
			continue
		}
		rep.Bound++
		conns = append(conns, o.conn)
	}

	if cfg.OnAllBound != nil {
		cfg.OnAllBound()
	}

	// The injector opens fire on the barrier that is already there: every session is bound, so the
	// peer takes the whole load at once rather than a ramp that would never reach its ceiling.
	var inj *injector
	var onPDU func(int, smpp.PDU)
	if cfg.Submit != nil {
		inj = newInjector(*cfg.Submit, len(conns))
		onPDU = inj.onPDU
		inj.start(ctx, conns, time.Now().Add(cfg.Hold))
	}

	submitStart := time.Now()
	rep.Dropped, conns = holdAndWatch(ctx, conns, cfg.Hold, onPDU)
	if inj != nil {
		inj.stop()
		rep.Submitting = time.Since(submitStart)
		inj.fill(&rep)
	}

	var teardown sync.WaitGroup
	for _, nc := range conns {
		teardown.Add(1)
		go func() {
			defer teardown.Done()
			unbind(nc, cfg.respTimeout())
		}()
	}
	teardown.Wait()

	rep.Elapsed = time.Since(start)
	return rep, nil
}

// validate rejects a Config that could not produce a meaningful run, before a single packet is sent.
func (c Config) validate() error {
	switch {
	case c.Addr == "":
		return errors.New("bindgen: Addr is required")
	case c.Binds < 1:
		return fmt.Errorf("bindgen: Binds must be at least 1, got %d", c.Binds)
	case c.SystemID == "":
		return errors.New("bindgen: SystemID is required")
	case len(c.SystemID) > maxSystemIDLen:
		return fmt.Errorf("bindgen: SystemID is %d characters, SMPP allows %d", len(c.SystemID), maxSystemIDLen)
	case len(c.Password) > maxPasswordLen:
		return fmt.Errorf("bindgen: Password is %d characters, SMPP allows %d", len(c.Password), maxPasswordLen)
	}
	if c.Submit == nil {
		return nil
	}
	// The injection window IS the hold window. Accepting a submit run without one would bind, submit
	// nothing and report a clean zero — the exact shape of a bug the tool is supposed to expose.
	if c.Hold <= 0 {
		return errors.New("bindgen: Submit needs a positive Hold: the hold window is the injection window")
	}
	return c.Submit.validate()
}

// validate rejects a SubmitConfig whose submissions the codec would refuse on every single write.
func (s SubmitConfig) validate() error {
	switch {
	case s.Window < 0:
		return fmt.Errorf("bindgen: Submit.Window must not be negative, got %d", s.Window)
	case s.Count < 0:
		return fmt.Errorf("bindgen: Submit.Count must not be negative, got %d", s.Count)
	case len(s.SourceAddr) > maxAddrLen:
		return fmt.Errorf("bindgen: Submit.SourceAddr is %d characters, SMPP allows %d", len(s.SourceAddr), maxAddrLen)
	case len(s.DestAddr) > maxAddrLen:
		return fmt.Errorf("bindgen: Submit.DestAddr is %d characters, SMPP allows %d", len(s.DestAddr), maxAddrLen)
	case len(s.ShortMessage) > maxShortMessageLen:
		return fmt.Errorf("bindgen: Submit.ShortMessage is %d octets, SMPP allows %d",
			len(s.ShortMessage), maxShortMessageLen)
	}
	return nil
}

func (c Config) dialTimeout() time.Duration {
	if c.DialTimeout <= 0 {
		return defaultDialTimeout
	}
	return c.DialTimeout
}

func (c Config) respTimeout() time.Duration {
	if c.RespTimeout <= 0 {
		return defaultRespTimeout
	}
	return c.RespTimeout
}

// bind dials the peer and completes one bind_transceiver. It returns the bound connection, which the
// caller owns, or an error with the connection already closed.
func bind(ctx context.Context, cfg Config) (net.Conn, error) {
	dialer := net.Dialer{Timeout: cfg.dialTimeout()}
	nc, err := dialer.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", cfg.Addr, err)
	}
	if err := handshake(nc, cfg); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return nc, nil
}

// handshake writes a bind_transceiver and waits for an accepting response. The deadline is the stop
// condition: a peer that accepts the connection and then says nothing cannot strand the goroutine.
func handshake(nc net.Conn, cfg Config) error {
	if err := nc.SetDeadline(time.Now().Add(cfg.respTimeout())); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}
	defer func() { _ = nc.SetDeadline(time.Time{}) }()

	req := smpp.PDU{Sequence: 1, Body: &smpp.BindTransceiver{BindFields: smpp.BindFields{
		SystemID:         cfg.SystemID,
		Password:         cfg.Password,
		SystemType:       cfg.SystemType,
		InterfaceVersion: smpp.InterfaceVersion34,
	}}}
	if err := smpp.WritePDU(nc, req); err != nil {
		return fmt.Errorf("write bind_transceiver: %w", err)
	}

	resp, err := smpp.ReadPDU(nc)
	if err != nil {
		return fmt.Errorf("read bind_transceiver_resp: %w", err)
	}
	if _, ok := resp.Body.(*smpp.BindTransceiverResp); !ok {
		return fmt.Errorf("answer to bind_transceiver was %T", resp.Body)
	}
	if resp.Status != smpp.StatusOK {
		return fmt.Errorf("bind rejected with command_status %#08x", resp.Status)
	}
	return nil
}

// unbind closes one session politely — unbind, best-effort unbind_resp, close. A peer that ignores
// the unbind still gets its connection closed once the deadline fires.
func unbind(nc net.Conn, timeout time.Duration) {
	defer func() { _ = nc.Close() }()

	if err := nc.SetDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	if err := smpp.WritePDU(nc, smpp.PDU{Sequence: 2, Body: &smpp.Unbind{}}); err != nil {
		return
	}
	_, _ = smpp.ReadPDU(nc)
}

// hold keeps the bound sessions idle for d, cut short by cancellation.
func hold(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// holdAndWatch keeps the sessions open for d and reports how many the peer tore down meanwhile,
// returning those still up. Answering "how many does it drop?" requires actually watching: a peer
// over its bind ceiling accepts every session and closes half of them a moment later, and a
// handshake that returned tells nothing about what happens next.
//
// With no hold window there is nothing to observe, so every bound session is reported as held.
// onPDU, when non-nil, is called with every PDU read from session i — the seam the submit_sm injector
// uses to match its responses. It runs on that session's watcher goroutine, so it must not block.
func holdAndWatch(ctx context.Context, conns []net.Conn, d time.Duration, onPDU func(int, smpp.PDU)) (int, []net.Conn) {
	if d <= 0 {
		return 0, conns
	}
	if len(conns) == 0 {
		hold(ctx, d)
		return 0, conns
	}

	deadline := time.Now().Add(d)
	held := make([]bool, len(conns))

	var wg sync.WaitGroup
	for i, nc := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seen func(smpp.PDU)
			if onPDU != nil {
				seen = func(pdu smpp.PDU) { onPDU(i, pdu) }
			}
			held[i] = watch(ctx, nc, deadline, seen)
		}()
	}
	wg.Wait()

	dropped := 0
	live := conns[:0:0]
	for i, nc := range conns {
		if held[i] {
			live = append(live, nc)
			continue
		}
		dropped++
		_ = nc.Close()
	}

	return dropped, live
}

// watch reads until the deadline. A read timeout means the session held for the whole window;
// anything else — EOF, reset — means the peer tore it down. Traffic coming from the peer is drained
// rather than answered: this tool measures whether a bind survives, it does not impersonate a full
// ESME, so a peer that expects an enquire_link response will eventually drop the session and be
// counted as such.
func watch(ctx context.Context, nc net.Conn, deadline time.Time, onPDU func(smpp.PDU)) bool {
	// The deadline is armed ONCE, and before the AfterFunc is registered. Both halves matter.
	//
	// It is absolute and persists across reads, so re-arming it inside the loop buys nothing — and
	// costs everything: AfterFunc fires only once, so any SetReadDeadline running after it silently
	// erases the cancellation and nothing ever re-arms. Testing ctx.Err() before that Set does not
	// help either; cancellation landing in the gap between the test and the Set is still lost, and
	// the read then blocks for the entire hold window. Never setting the deadline again after the
	// AfterFunc is registered makes that whole class of race unrepresentable.
	if err := nc.SetReadDeadline(deadline); err != nil {
		return false
	}
	stop := context.AfterFunc(ctx, func() { _ = nc.SetReadDeadline(time.Now()) })
	defer stop()

	for {
		pdu, err := smpp.ReadPDU(nc)
		if err != nil {
			if ctx.Err() != nil {
				return true // we walked away; the peer did not drop us
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return true
			}
			// A PDU this codec does not model is not a teardown. data_sm, alert_notification and
			// vendor PDUs are ordinary SMSC traffic; counting them as drops would report a healthy
			// peer as dropping 100% of its sessions — and close them ourselves to prove it. That is
			// why there is no cap on how many we skip: a cap is just the same lie at a higher
			// threshold, and an SMSC delivering over data_sm would trip it in a second.
			// Rewinding is safe: ReadPDU consumes the whole PDU (its length prefix drives a ReadFull)
			// before decoding, so these two errors leave the stream aligned on the next one.
			// ErrLengthMismatch and ErrPDUTooLarge are deliberately NOT tolerated: they are raised
			// before the body is consumed, so the stream is desynchronised and rewinding is unsafe.
			if errors.Is(err, smpp.ErrUnknownCommand) || errors.Is(err, smpp.ErrMalformedBody) {
				continue
			}

			return false
		}
		if onPDU != nil {
			onPDU(pdu)
		}
	}
}
