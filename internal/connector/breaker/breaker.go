// Package breaker is the per-connector circuit breaker (§6.15): a closed → open → half_open → closed
// state machine driven by submit_sm outcomes. It reflects a connector's APPLICATION health (a burst of
// terminal send failures), distinct from the AIMD send-rate throttle (step-086) and the TCP link_status
// (step-127). It is local to one pod here; multi-pod majority aggregation arrives in step-122 and the
// router consumes the aggregate in step-123.
package breaker

import (
	"sync"
	"time"
)

// State is a connector's breaker state.
type State int

// The breaker states.
const (
	Closed   State = iota // healthy: requests flow
	Open                  // unhealthy: requests are refused until the cooldown elapses
	HalfOpen              // probing: a bounded number of trial requests decide recovery
)

// String renders a State as its schema token (breaker:state values, Appendix B).
func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// ParseState maps a schema token (breaker:state value) back to a State. ok is false for an unknown
// token — the caller decides how to treat an unreadable aggregate (this package defaults to Closed).
func ParseState(token string) (State, bool) {
	switch token {
	case "closed":
		return Closed, true
	case "open":
		return Open, true
	case "half_open":
		return HalfOpen, true
	default:
		return Closed, false
	}
}

// Config tunes the breaker. FailureRate over a rolling Window (once at least MinRequests are seen)
// trips it open; after Cooldown it half-opens and admits up to HalfOpenProbes trial requests — enough
// successes close it, one failure re-opens it.
type Config struct {
	MinRequests     int           // minimum requests in the window before the rate is evaluated
	FailureRate     float64       // open when failures/total ≥ this over the window (0..1]
	Window          time.Duration // rolling failure-rate window (tumbling)
	Cooldown        time.Duration // open → half_open wait
	HalfOpenProbes  int           // trial requests admitted while half-open; that many successes close it
	HalfOpenTimeout time.Duration // half_open → open if the probes do not resolve within this (liveness)
}

// Breaker is one connector's circuit breaker. Its methods are safe for concurrent use. The clock is
// injectable for deterministic tests; it starts no goroutine.
//
// Outcomes are attributed to the current state, not to a specific probe episode: the caller MUST feed a
// send's outcome within a bounded response window that is shorter than Cooldown, so a probe result can
// never arrive after a HalfOpenTimeout→Open→Cooldown→HalfOpen cycle and be miscredited to a later
// episode. The connector pool enforces this per-request response timeout; a request without a resp by
// then is fed as a RecordFailure, not held.
type Breaker struct {
	cfg Config
	now func() time.Time

	mu          sync.Mutex
	state       State
	openedAt    time.Time // when it entered Open (for the cooldown)
	halfOpenAt  time.Time // when it entered HalfOpen (for the half-open timeout)
	windowStart time.Time // start of the current tumbling window
	failures    int       // failures in the current window
	total       int       // requests in the current window
	probeOK     int       // successful probes while half-open
	probesOut   int       // probes admitted while half-open (bounds concurrent trials)
}

// New builds a breaker. A nil clock defaults to time.Now. Zero-value config fields fall back to sane
// defaults so a caller can set only what it cares about.
func New(cfg Config, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = 20
	}
	if cfg.FailureRate <= 0 {
		cfg.FailureRate = 0.5
	}
	if cfg.FailureRate > 1 {
		cfg.FailureRate = 1 // a rate > 1 could never trip — clamp so the breaker is never silently disabled
	}
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Second
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 1
	}
	if cfg.HalfOpenTimeout <= 0 {
		cfg.HalfOpenTimeout = cfg.Cooldown // by default, give the probes as long as a cooldown to resolve
	}
	return &Breaker{cfg: cfg, now: now, state: Closed, windowStart: now()}
}

// State returns the current state, first advancing Open → HalfOpen if the cooldown has elapsed (so a
// reader sees recovery start without a request driving it).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	return b.state
}

// Allow reports whether a request may be sent now. Closed always allows; Open refuses until the
// cooldown elapses, then admits bounded half-open probes; HalfOpen admits up to HalfOpenProbes trials.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	switch b.state {
	case Closed:
		return true
	case HalfOpen:
		if b.probesOut >= b.cfg.HalfOpenProbes {
			return false
		}
		b.probesOut++
		return true
	default: // Open (cooldown not yet elapsed)
		return false
	}
}

// RecordSuccess feeds a successful send outcome.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	switch b.state {
	case HalfOpen:
		b.probeOK++
		if b.probeOK >= b.cfg.HalfOpenProbes {
			b.toClosed()
		}
	case Closed:
		b.roll()
		b.total++
	}
}

// RecordFailure feeds a terminal (connector-health) send failure. A transient/throttle error is NOT a
// breaker failure and must not be recorded here (the caller classifies it).
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	switch b.state {
	case HalfOpen:
		b.toOpen() // a probe failed → straight back to Open for another cooldown
	case Closed:
		b.roll()
		b.total++
		b.failures++
		if b.total >= b.cfg.MinRequests && float64(b.failures)/float64(b.total) >= b.cfg.FailureRate {
			b.toOpen()
		}
	}
}

// refresh applies the two time-driven transitions before any state read/write: Open → HalfOpen once the
// cooldown elapses, and HalfOpen → Open once the half-open probes overstay HalfOpenTimeout without
// resolving. The latter is the liveness guarantee: probes that come back Transient (throttled) or are
// lost would otherwise exhaust the probe quota and never close/re-open, fencing a healthy connector
// forever. Caller holds the lock.
func (b *Breaker) refresh() {
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) >= b.cfg.Cooldown {
			b.toHalfOpen()
		}
	case HalfOpen:
		if b.now().Sub(b.halfOpenAt) >= b.cfg.HalfOpenTimeout {
			b.toOpen() // probes did not resolve in time → re-open and retry after the next cooldown
		}
	}
}

func (b *Breaker) toHalfOpen() {
	b.state = HalfOpen
	b.halfOpenAt = b.now()
	b.probeOK, b.probesOut = 0, 0
}

// roll resets the tumbling window when it has elapsed. Caller holds the lock.
func (b *Breaker) roll() {
	t := b.now()
	if t.Sub(b.windowStart) >= b.cfg.Window {
		b.windowStart = t
		b.failures, b.total = 0, 0
	}
}

func (b *Breaker) toOpen() {
	b.state = Open
	b.openedAt = b.now()
	b.probeOK, b.probesOut = 0, 0
}

func (b *Breaker) toClosed() {
	b.state = Closed
	b.windowStart = b.now()
	b.failures, b.total = 0, 0
	b.probeOK, b.probesOut = 0, 0
}
