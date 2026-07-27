package connectorpool

import (
	"context"
	"math"
	"sync"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// aimd is a per-connector adaptive throttle: the outbound send rate follows the SMSC's feedback via
// AIMD (additive-increase / multiplicative-decrease). An ESME_RTHROTTLED submit_sm_resp halves the
// rate; a success nudges it back up, bounded by the connector's throughput ceiling. It is a DYNAMIC,
// per-pod ceiling — no multi-pod coordination (M6, §10) — and it NEVER cuts the bind (that is the
// circuit breaker's job, M8): at worst it paces sends slower. It is safe for concurrent use, though the
// connector's submit path is serial.
type aimd struct {
	mu           sync.Mutex
	rate         float64   // current permitted sends/sec
	max          float64   // ceiling (throughput_limit_per_sec)
	min          float64   // floor, so a throttled connector still trickles rather than stalling to zero
	increaseStep float64   // additive increase per success
	lastSend     time.Time // pacing cursor
	now          func() time.Time
}

// aimdDecrease is the multiplicative factor applied on an ESME_RTHROTTLED (halve the rate).
const aimdDecrease = 0.5

// newAIMD builds a controller starting at the full ceiling. A success adds increaseStep = max/100 (a
// gradual recovery), and the rate never falls below min = 5% of the ceiling (at least 1/s).
func newAIMD(maxRate float64, now func() time.Time) *aimd {
	if now == nil {
		now = time.Now
	}
	floor := maxRate * 0.05
	if floor < 1 {
		floor = 1
	}
	step := maxRate / 100
	if step < 1 {
		step = 1
	}
	return &aimd{rate: maxRate, max: maxRate, min: floor, increaseStep: step, now: now}
}

// observe feeds back a submit_sm_resp command_status: ESME_RTHROTTLED decreases the rate
// multiplicatively, a success (ESME_ROK) increases it additively up to the ceiling, and any other
// status leaves it unchanged (a permanent error is not a rate signal). It returns true when the
// response was a throttle, so the caller can count the event.
func (a *aimd) observe(status uint32) (throttled bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch status {
	case errs.StatusThrottled:
		a.rate = math.Max(a.min, a.rate*aimdDecrease)
		return true
	case smpp.StatusOK:
		a.rate = math.Min(a.max, a.rate+a.increaseStep)
	}
	return false
}

// currentRate is the current permitted sends/sec (for metrics and tests).
func (a *aimd) currentRate() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rate
}

// acquire paces the caller to the current rate: it returns once at least 1/rate seconds have elapsed
// since the previous send, reserving this send's slot. It blocks at most that interval and honours ctx
// cancellation. This is what makes the AIMD rate the EFFECTIVE send rate rather than a mere counter.
func (a *aimd) acquire(ctx context.Context) error {
	a.mu.Lock()
	if a.rate <= 0 {
		a.mu.Unlock()
		return nil
	}
	interval := time.Duration(float64(time.Second) / a.rate)
	now := a.now()
	earliest := a.lastSend.Add(interval)
	if !earliest.After(now) {
		a.lastSend = now
		a.mu.Unlock()
		return nil
	}
	a.lastSend = earliest // reserve the slot so concurrent callers queue behind it
	wait := earliest.Sub(now)
	a.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop() // release the timer promptly if ctx wins the race
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
