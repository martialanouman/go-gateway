// Package ceiling establishes the submit_sm throughput ceiling of the SMPP peer used for load runs
// (step-201, D3). It sweeps the number of concurrent binds, holds each tier long enough to be a
// measurement rather than a burst, and reads the absorbed rate from the peer's own /metrics.
//
// It answers one question: how fast can the test peer take submit_sm, so that a later reference run
// can be placed BELOW that figure. A reference run at the peer's ceiling measures the peer, not the
// gateway — and every capacity lever tuned against it would be tuned against an artefact.
//
// Two figures come out, not one: the curve of rate-versus-binds (Result.Tiers), and the rate at the
// bind count the reference run will use (Result.ReferenceCeiling).
//
// # A ceiling only when the sweep found one
//
// The highest rate over the counted tiers is a CEILING only when some tier actually reached the peer's
// limit — Result.Saturated. A sweep whose every tier scaled with the binds it was handed measured the
// largest load it was ASKED to produce, and calling that a ceiling points a capacity plan at a
// constraint nobody has evidence for. Absent saturation, Result.Ceiling is a lower bound and says so.
//
// Two signals, and only two, say the peer reached its limit: an outcome other than success in the
// window, and the curve failing to scale with the binds (minScalingFraction). The served-latency
// histogram is NOT one of them — the simulator observes the latency its scenario decided rather than a
// duration it measured, so it reads its configured value however hard the peer is pushed.
//
// # What makes the number trustworthy
//
// Each of these is enforced rather than assumed:
//
//   - The rate is read from the peer (smscmetrics), never from the injector's own counters. An
//     injector that queues or retries cannot inflate it. The injector's report is kept only to answer
//     "did it push, and was it still being answered?".
//   - Both readings are taken from INSIDE the injection window, anchored on absolute instants counted
//     from the start signal, and the second one is refused if it came back too near the end of that
//     window. A reading taken after the injection stopped divides by a window whose tail carried no
//     load, and understates the rate with every counter still looking healthy.
//   - A tier whose window carried any non-success outcome is DISQUALIFIED: the peer was shedding, so
//     what it absorbed is not a rate it sustained. It stays in the curve, marked, and out of the
//     ceiling.
//   - A tier the peer accepted materially more of than it served is DISQUALIFIED too. The rate is
//     derived from smsc_submit_sm_received_total, which counts PDUs taken off the wire — acceptance,
//     not throughput. A peer dropping its queue overflow silently moves that counter and no outcome.
//   - A tier whose peer-side bind gauge disagrees with the bind count asked for is REFUSED, and so is
//     one whose sessions got wildly unequal amounts through (maxSubmitSpread): a session that stopped
//     being served neither fails nor drops, and the tier would be filed under a bind count nobody
//     actually ran on. The spread is the signal, not the unanswered tail — a windowed injector ends
//     every honest run with its whole window outstanding on every session.
//
// The sweep drives the peer through two seams — a Load that runs one tier and a Scraper that takes one
// reading — so the logic is testable without a simulator. It never launches the peer: the address and
// the metrics URL are inputs, which is also what lets it point at a remote simulator later.
package ceiling

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
	"github.com/martialanouman/go-gateway/test/load/smscmetrics"
)

// defaultBinds is the sweep of D3.
var defaultBinds = []int{10, 20, 40, 80}

// Defaults applied when Config leaves a duration at zero.
const (
	// defaultMeasure is the length of one tier's measurement window.
	defaultMeasure = MinRecordableMeasure

	// defaultWarmup is the head of the injection window left out of the measurement — the time the
	// sessions need to fill their in-flight windows and the previous tier's sockets to be reaped.
	defaultWarmup = 10 * time.Second

	// defaultSettle is the margin kept between the second reading and the end of the injection. It is
	// slack, not measured time: it guarantees the injection is still running while that reading is in
	// flight, which is the whole difference between measuring under load and measuring after it.
	defaultSettle = 5 * time.Second

	// defaultCooldown is the pause between two tiers, so the peer's binds are gone before the next
	// tier's are counted.
	defaultCooldown = 5 * time.Second

	// maxReferenceBinds is the largest bind count a single connector pool can be configured for:
	// bind_pool_size is bounded to 1..32 by the control-plane schema. It only picks a default
	// Reference — the largest tier a lone pool could actually reproduce.
	maxReferenceBinds = 32
)

// MinRecordableMeasure is the shortest per-tier measurement window whose figures are worth recording,
// and the floor D3 sets. Below it what is measured is partly the peer's buffers draining rather than a
// rate it held, so a shorter sweep proves the instrument runs and nothing else. See Result.Recordable.
const MinRecordableMeasure = 60 * time.Second

// minScalingFraction is the share of the extra throughput a tier's extra binds should have bought,
// under which the curve is read as having bent. A peer scaling perfectly buys the whole proportional
// increase (fraction 1); a peer at its limit buys none of it (fraction 0). Half sits between the two.
//
// What the sweep of 02/08 actually returned, doubling by doubling: 0.98, 1.09, 0.92, 0.79, 0.48. So the
// bar sits below the four tiers that scaled and above the one that did not — but the margin on the
// deciding tier is 1.6%, smaller than the ~4% spread between neighbouring tiers of that same run. This
// threshold separates the cases it was checked against; it does not make the verdict robust, and a
// saturation verdict resting on it should be confirmed by a second sweep rather than published alone.
//
// This is the second of the two saturation signals, and the only one available against a peer that
// never refuses anything — it just stops going faster.
const minScalingFraction = 0.5

// Bars a tier has to clear beyond the peer's own outcome counter.
const (
	// maxUnservedFraction is how much of what the peer ACCEPTED during the window may still be
	// unserved by the end of it. The two are different counters: smsc_submit_sm_received_total counts
	// PDUs taken off the wire, the served histogram counts the ones the peer answered for. A peer that
	// answers submit_sm_resp OK and then drops the message moves the first and not the second, and its
	// absorbed rate would then be an acceptance rate — a figure nobody can reproduce.
	//
	// It has to be a peer that still ANSWERS. One that swallows PDUs without responding holds the
	// injector's window slots instead, so the injector stalls after binds*Window losses — long before
	// this fraction of a 60s window is reached — and maxSubmitSpread is what catches that shape.
	//
	// It is a fraction rather than a count because what legitimately sits between the two is the
	// change in the peer's own backlog over the window, and in steady state that is a handful of PDUs
	// either way. Two percent is well above that and well below any real drop.
	maxUnservedFraction = 0.02

	// maxSubmitSpread bounds how far the quietest session may fall behind the busiest before the tier
	// is refused. A session the peer stopped serving produces no error at all — the connection stays
	// open, so nothing lands in Failed or Dropped, and the peer's bind gauge still counts it — but it
	// gets through a fraction of what its siblings do, and the rate is then filed under a bind count
	// nobody actually ran on.
	//
	// The spread is the only figure that shows this. The in-flight tail cannot: a windowed injector
	// ends EVERY run with close to its whole window outstanding on every session, healthy or frozen —
	// measured at exactly binds*32 against the simulator — because a slot is only freed by a response
	// and is re-consumed at once. A threshold on Unanswered refuses every honest tier instead, which
	// is what an earlier version of this guard did.
	//
	// Four is slack: sessions facing one peer measured inside a factor of two of each other, and a
	// genuinely stalled session sits near zero. The bar is deliberately one a very uneven link could
	// trip — a tier wrongly refused is loud and costs a re-run, a tier wrongly counted is a number
	// somebody tunes a system against.
	//
	// What it does NOT catch, by construction: a stall that hits every session equally (each one is
	// then as quiet as the next, so there is no spread), and a stall late enough in the window that the
	// session still got a comparable share through. maxUnansweredFraction covers the first; nothing
	// covers the second, which is why the recorded figures carry that reserve rather than claiming it
	// away.
	maxSubmitSpread = 4

	// maxUnansweredFraction is how much of what the injector sent may still be outstanding when the
	// window closes. It is the only guard that sees a peer which ACCEPTS submit_sm and never answers
	// any: its receive counter moves, so the rate looks real and scales perfectly with the binds; it
	// reports no outcome, so the tier stays qualified; it stalls every session identically, so the
	// spread is 1; and its served histogram never moves, which disables the served-versus-accepted
	// guard instead of tripping it. Such a peer produces a publishable figure having answered nothing.
	//
	// A quarter is two orders of magnitude of slack. A healthy tier leaves its in-flight window and
	// nothing more: measured at 0.27%–0.41% of what was sent, from 20 to 320 binds over 60s windows.
	// A peer that answers nothing leaves 100%.
	maxUnansweredFraction = 0.25
)

// Load runs one tier of the sweep: it opens the binds, injects submit_sm for the whole hold window,
// and reports what its own side saw. BindgenLoad implements it against test/load/bindgen.
type Load interface {
	// Inject blocks for the whole injection window and returns the injector's report. It must call
	// p.OnStart once, when injection actually begins, and honour cancellation by returning early.
	Inject(ctx context.Context, p LoadParams) (bindgen.Report, error)
}

// LoadParams is one tier's injection order.
type LoadParams struct {
	// Binds is how many concurrent sessions to open and push on.
	Binds int
	// Hold is how long injection must last: warmup, measurement window and settle margin together.
	Hold time.Duration
	// OnStart is called once, on the injector's own goroutine, the moment injection begins. It is the
	// sweep's start-of-window signal — everything it waits for is counted from that call, never from
	// the instant it asked for the tier.
	OnStart func()
}

// Scraper takes one reading of the peer's metrics endpoint. *smscmetrics.Client implements it.
type Scraper interface {
	Scrape(ctx context.Context) (smscmetrics.Snapshot, error)
}

// TierStatus is what became of one tier.
type TierStatus int

const (
	// TierCounted is a clean measurement: it enters the curve and may set the ceiling.
	TierCounted TierStatus = iota
	// TierDisqualified is a tier the peer shed traffic on. It is a finding, not a fault — it usually
	// means the tier sits above the ceiling — so it stays in the curve, is excluded from the ceiling,
	// and does not fail the sweep.
	TierDisqualified
	// TierFailed is a tier whose measurement is invalid: the peer never held the binds asked for, the
	// injector put nothing on the wire, a reading failed. It fails the sweep — a number nobody can
	// trust must not be published as a ceiling.
	TierFailed
)

// String renders the status for a report line.
func (s TierStatus) String() string {
	switch s {
	case TierCounted:
		return "counted"
	case TierDisqualified:
		return "disqualified"
	case TierFailed:
		return "failed"
	default:
		return fmt.Sprintf("TierStatus(%d)", int(s))
	}
}

// Tier is one step of the sweep: what was asked for, what the peer absorbed, and what the injector saw.
type Tier struct {
	// Binds is the number of concurrent sessions this tier asked for.
	Binds int
	// Status says whether the tier counts.
	Status TierStatus
	// Reason explains a status other than TierCounted, in one line fit for a report.
	Reason string
	// Err is the underlying cause of a TierFailed tier, nil otherwise.
	Err error
	// Throughput is what the peer absorbed during the measurement window. Zero when the tier failed
	// before both readings were taken.
	Throughput smscmetrics.Throughput
	// Report is the injector's own view — a diagnostic, never the measurement.
	Report bindgen.Report
}

// Result is the outcome of a sweep: the curve, and the two figures D3 asks to be recorded.
type Result struct {
	// Tiers is every tier attempted, in sweep order — the curve, disqualified and failed tiers
	// included and marked as such.
	Tiers []Tier
	// Ceiling is the highest absorbed rate over the counted tiers, in submit_sm per second.
	//
	// It is only a CEILING when Saturated is true. Otherwise it is a lower bound: the sweep proved the
	// peer takes at least this much and never found where it stops, so its limit is somewhere above
	// every tier that ran. Read the two apart before quoting the number — a lower bound quoted as a
	// ceiling points a capacity plan at a constraint that does not exist.
	Ceiling float64
	// CeilingBinds is the bind count that produced Ceiling.
	CeilingBinds int
	// Saturated reports whether any tier actually reached the peer's limit: the peer shed traffic, or
	// the curve stopped scaling with the binds thrown at it. False means the sweep was never big
	// enough to find the limit, which is a result about the sweep and not about the peer.
	Saturated bool
	// SaturationReason names the tier and the signal that showed the limit, in one line fit for a
	// report. Empty when Saturated is false.
	SaturationReason string
	// ReferenceBinds is the bind count the reference run will use (Config.Reference).
	ReferenceBinds int
	// ReferenceCeiling is the absorbed rate at ReferenceBinds — the figure a reference run must stay
	// under. Zero when that tier did not count, which is an error rather than a result.
	ReferenceCeiling float64
	// Measure is the window each tier was scored over. It travels with the figures so a smoke run can
	// be told from a measurement by whoever reads the Result, not only by whoever typed the flags.
	Measure time.Duration

	// saturatedAt and saturationFromBend carry what is needed to withdraw a bend a later tier
	// disproves. Unexported: how the mark can be taken back is this type's business, not a caller's.
	saturatedAt        int
	saturationFromBend bool
}

// Recordable reports whether the tiers were measured long enough (MinRecordableMeasure) for their
// figures to be worth writing down.
func (r Result) Recordable() bool { return r.Measure >= MinRecordableMeasure }

// markSaturation records evidence that the peer reached its limit. The first evidence is kept rather
// than the last: it is the lowest tier at which the sweep can say the peer stopped scaling.
//
// fromBend says whether the evidence is an inferred bend in the curve rather than shedding the peer
// itself reported. Only a bend can later be withdrawn — see unmarkSaturationIfDisproved.
//
// "First" means first of its kind: shedding SUPERSEDES a bend recorded earlier, because the peer
// reported it rather than leaving it to be inferred, and because it is the reason the reader has to
// see. Without that, the withdrawal below would take the shed away with the bend and the tool would
// report "no tier shed" over a curve holding a tier disqualified for exactly that.
func (r *Result) markSaturation(binds int, fromBend bool, reason string) {
	supersedes := !fromBend && r.Saturated && r.saturationFromBend
	if !r.Saturated || supersedes {
		r.Saturated = true
		r.SaturationReason = reason
		r.saturatedAt = binds
		r.saturationFromBend = fromBend
	}
}

// unmarkSaturationIfDisproved withdraws a bend the tiers above went on to disprove. A single dip — one
// tier losing CPU to something else on a shared host — bends the curve without the peer having any
// limit; if a later tier then absorbs more, the dip was noise. Leaving the mark would print a CEILING
// over a sweep that was still scaling, which is the very claim the flag exists to prevent.
//
// Shedding is never withdrawn: a peer that threw PDUs away showed a real limit, whatever the tiers
// above do. Only a bend is provisional, because only a bend is inferred from a rate rather than
// reported by the peer.
func (r *Result) unmarkSaturationIfDisproved(binds int, rate float64) {
	if r.Saturated && r.saturationFromBend && binds > r.saturatedAt && rate > r.Ceiling {
		r.Saturated = false
		r.SaturationReason = ""
		r.saturationFromBend = false
		r.saturatedAt = 0
	}
}

// Tier returns the tier measured at the given bind count, and whether it was attempted.
func (r Result) Tier(binds int) (Tier, bool) {
	for _, t := range r.Tiers {
		if t.Binds == binds {
			return t, true
		}
	}
	return Tier{}, false
}

// Config describes one sweep. A duration left at zero takes the package default; a negative one is
// rejected.
type Config struct {
	// Binds is the sweep, in concurrent sessions per tier. Zero-length means 10, 20, 40, 80 (D3).
	// Duplicates are rejected and the tiers always run in ascending order — a sweep is a ramp.
	Binds []int
	// Reference is the bind count the reference run will use, and must be one of Binds. Zero means the
	// largest tier a single connector pool could reproduce (bind_pool_size is capped at 32).
	Reference int
	// Warmup is the head of each injection window left out of the measurement.
	Warmup time.Duration
	// Measure is the measurement window itself. It must be at least smscmetrics.MinWindow, and D3
	// requires at least 60 seconds for a figure worth recording.
	Measure time.Duration
	// Settle is the margin between the second reading and the end of the injection window. Half of it
	// is the bar the second reading has to come back inside: spend more than that and the tier is
	// refused rather than counted over a window the injection may already have left.
	Settle time.Duration
	// Cooldown is the pause between two tiers.
	Cooldown time.Duration
	// VirtualSMSC narrows every reading to one virtual SMSC of the peer. Empty aggregates them all,
	// which is right when the sweep is the peer's only client.
	VirtualSMSC string
	// OnTier, when non-nil, is called with each tier as soon as it is measured, so a long sweep can
	// report progress instead of going quiet for minutes. It runs on Run's goroutine.
	OnTier func(Tier)
}

// Hold is how long one tier's injection must last: warmup, measurement window and settle margin.
func (c Config) Hold() time.Duration { return c.Warmup + c.Measure + c.Settle }

// Sweeper runs a sweep against one peer. Build it with New.
type Sweeper struct {
	load    Load
	scraper Scraper
	cfg     Config
}

// New validates the configuration, fills in the defaults and returns the sweeper. It never touches the
// peer: everything it can refuse, it refuses before a single bind is opened.
func New(load Load, scraper Scraper, cfg Config) (*Sweeper, error) {
	if load == nil {
		return nil, errors.New("ceiling: a Load is required")
	}
	if scraper == nil {
		return nil, errors.New("ceiling: a Scraper is required")
	}
	normalised, err := cfg.normalise()
	if err != nil {
		return nil, err
	}
	return &Sweeper{load: load, scraper: scraper, cfg: normalised}, nil
}

// Config reports the configuration actually in force, defaults included.
func (s *Sweeper) Config() Config { return s.cfg }

// normalise checks the configuration and returns it with the defaults applied.
func (c Config) normalise() (Config, error) {
	out := c

	if len(out.Binds) == 0 {
		out.Binds = slices.Clone(defaultBinds)
	} else {
		out.Binds = slices.Clone(out.Binds)
	}
	slices.Sort(out.Binds)
	for i, b := range out.Binds {
		if b < 1 {
			return Config{}, fmt.Errorf("ceiling: bind count %d is not a tier, it must be at least 1", b)
		}
		if i > 0 && out.Binds[i-1] == b {
			return Config{}, fmt.Errorf("ceiling: bind count %d appears twice in the sweep", b)
		}
	}

	switch {
	case out.Warmup < 0:
		return Config{}, fmt.Errorf("ceiling: Warmup must not be negative, got %v", out.Warmup)
	case out.Measure < 0:
		return Config{}, fmt.Errorf("ceiling: Measure must not be negative, got %v", out.Measure)
	case out.Settle < 0:
		return Config{}, fmt.Errorf("ceiling: Settle must not be negative, got %v", out.Settle)
	case out.Cooldown < 0:
		return Config{}, fmt.Errorf("ceiling: Cooldown must not be negative, got %v", out.Cooldown)
	}
	if out.Warmup == 0 {
		out.Warmup = defaultWarmup
	}
	if out.Measure == 0 {
		out.Measure = defaultMeasure
	}
	if out.Settle == 0 {
		out.Settle = defaultSettle
	}
	if out.Cooldown == 0 {
		out.Cooldown = defaultCooldown
	}
	// Below MinWindow the reader refuses the division anyway; catching it here fails the sweep before
	// it spends a tier's worth of load producing nothing.
	if out.Measure < smscmetrics.MinWindow {
		return Config{}, fmt.Errorf("ceiling: Measure is %v, the metrics reader needs at least %v",
			out.Measure, smscmetrics.MinWindow)
	}

	if out.Reference == 0 {
		for _, b := range out.Binds {
			if b <= maxReferenceBinds {
				out.Reference = b
			}
		}
		if out.Reference == 0 {
			return Config{}, fmt.Errorf(
				"ceiling: no tier is at most %d binds, so none can be the reference: set Reference explicitly",
				maxReferenceBinds)
		}
	}
	if !slices.Contains(out.Binds, out.Reference) {
		return Config{}, fmt.Errorf("ceiling: Reference is %d binds, which is not one of the sweep's tiers %v",
			out.Reference, out.Binds)
	}

	return out, nil
}

// Run sweeps every tier and returns the curve with the two ceiling figures.
//
// The Result is always usable, error or not: a tier that failed costs its own reading, not the whole
// sweep's. The error is non-nil as soon as one tier failed to produce a trustworthy measurement, no
// counted tier came out of the sweep, or the reference tier is not among the counted ones — a sweep
// that lost the figure it exists to produce must never read as a success.
func (s *Sweeper) Run(ctx context.Context) (Result, error) {
	res := Result{ReferenceBinds: s.cfg.Reference, Measure: s.cfg.Measure}
	var errs []error
	// The last tier that produced a usable rate, which is what the next one's scaling is judged against.
	var prev *Tier

	for i, binds := range s.cfg.Binds {
		if i > 0 {
			if err := wait(ctx, s.cfg.Cooldown); err != nil {
				errs = append(errs, fmt.Errorf("ceiling: sweep interrupted before the %d-bind tier: %w", binds, err))
				break
			}
		}

		tier := s.runTier(ctx, binds)
		res.Tiers = append(res.Tiers, tier)
		if s.cfg.OnTier != nil {
			s.cfg.OnTier(tier)
		}

		switch tier.Status {
		case TierCounted:
			// Withdraw before marking, and before the ceiling moves: a tier that outruns the peak is
			// what disproves an earlier bend, and it must be judged against the peak as it stood.
			res.unmarkSaturationIfDisproved(binds, tier.Throughput.SubmitPerSecond)
			if prev != nil {
				if scaling, isBent := bent(*prev, tier); isBent {
					res.markSaturation(binds, true, fmt.Sprintf(
						"the curve bent at %d binds: %.0f/s against %.0f/s at %d binds, %.0f%% of what the extra binds should have bought",
						binds, tier.Throughput.SubmitPerSecond, prev.Throughput.SubmitPerSecond, prev.Binds, 100*scaling))
				}
			}
			counted := tier
			prev = &counted

			if tier.Throughput.SubmitPerSecond > res.Ceiling {
				res.Ceiling = tier.Throughput.SubmitPerSecond
				res.CeilingBinds = binds
			}
			if binds == s.cfg.Reference {
				res.ReferenceCeiling = tier.Throughput.SubmitPerSecond
			}
		case TierFailed:
			errs = append(errs, fmt.Errorf("ceiling: the %d-bind tier failed: %s: %w", binds, tier.Reason, tier.Err))
		case TierDisqualified:
			// A finding, not a fault: the peer shed at this tier, which is what a ceiling looks like —
			// and it is the plainest evidence the sweep can get that the limit was reached.
			res.markSaturation(binds, false, fmt.Sprintf("the peer shed traffic at %d binds: %s", binds, tier.Reason))
		}

		if ctx.Err() != nil {
			errs = append(errs, fmt.Errorf("ceiling: sweep interrupted after the %d-bind tier: %w", binds, ctx.Err()))
			break
		}
	}

	if res.CeilingBinds == 0 {
		errs = append(errs, errors.New("ceiling: no tier counted, the sweep produced no ceiling"))
	}
	if res.ReferenceCeiling == 0 {
		errs = append(errs, fmt.Errorf(
			"ceiling: the reference tier (%d binds) did not count, so there is no figure for the reference run to stay under",
			s.cfg.Reference))
	}
	return res, errors.Join(errs...)
}

// errNotStarted reports an injection that ended before it signalled its start — there was never a
// window to measure inside.
var errNotStarted = errors.New("ceiling: the injection ended before it began")

// errLateReading reports a second reading that came back too close to the end of the injection window
// to be sure the load was still running while the peer served it.
var errLateReading = errors.New("ceiling: the second reading was taken too late")

// runTier drives one tier: it starts the injection, takes its two readings from inside the injection
// window, then stops the injection and judges what came back.
func (s *Sweeper) runTier(ctx context.Context, binds int) Tier {
	tier := Tier{Binds: binds}

	// The tier owns a derived context so the injection stops the moment the tier is over — on the
	// second reading as much as on a failure. Nothing keeps hammering the peer while the next tier
	// tries to measure it.
	tierCtx, cancel := context.WithCancel(ctx)

	started := make(chan struct{})
	done := make(chan struct{})
	var (
		rep    bindgen.Report
		runErr error
	)
	go func() {
		defer close(done)
		var once sync.Once
		rep, runErr = s.load.Inject(tierCtx, LoadParams{
			Binds:   binds,
			Hold:    s.cfg.Hold(),
			OnStart: func() { once.Do(func() { close(started) }) },
		})
	}()

	before, after, measErr := s.measure(tierCtx, started, done)

	cancel()
	<-done // the report and the error are only ours to read once the injector has left
	tier.Report = rep

	switch {
	case errors.Is(measErr, errNotStarted):
		cause := runErr
		if cause == nil {
			cause = measErr
		}
		return tier.fail("the injection never opened its window", cause)
	case errors.Is(measErr, errLateReading):
		return tier.fail("the measurement ran past the end of the injection", measErr)
	case measErr != nil:
		return tier.fail("a reading failed", measErr)
	// A cancellation here is the one this tier issued itself, three lines above.
	case runErr != nil && !errors.Is(runErr, context.Canceled):
		return tier.fail("the injection failed", runErr)
	case rep.Failed > 0:
		return tier.fail(fmt.Sprintf("%d of the %d binds never bound (first: %s)",
			rep.Failed, rep.Requested, firstCause(rep)), nil)
	case rep.Dropped > 0:
		return tier.fail(fmt.Sprintf("the peer dropped %d of the %d bound sessions mid-window",
			rep.Dropped, rep.Bound), nil)
	case rep.Submitted > 0 && float64(rep.Unanswered) > maxUnansweredFraction*float64(rep.Submitted):
		// A peer that took the PDUs and answered none of them. Every other guard is blind to it.
		return tier.fail(fmt.Sprintf(
			"%d of the %d submit_sm sent were still unanswered, %.0f%% of them: the peer accepted without answering",
			rep.Unanswered, rep.Submitted, 100*float64(rep.Unanswered)/float64(rep.Submitted)), nil)
	case binds > 1 && rep.SubmittedMin*maxSubmitSpread < rep.SubmittedMax:
		// Sessions that stopped being served, which no aggregate above can show: they neither fail nor
		// drop, and the peer's gauge still counts their connections. The rate would then be filed under
		// a bind count nobody actually ran on.
		return tier.fail(fmt.Sprintf(
			"the quietest session submitted %d against the busiest %d, over the %dx spread a healthy %d-bind tier stays within: sessions stopped being served",
			rep.SubmittedMin, rep.SubmittedMax, maxSubmitSpread, binds), nil)
	case rep.Submitted == 0:
		// Both counters, because a writer blocked on its very first write and then cut off by
		// the closing window lands in SubmitCutShort with no error at all: reporting only
		// SubmitErrors would say "(0 errors)" with a nil cause on the one tier that most needs
		// diagnosing.
		return tier.fail(fmt.Sprintf("the injector put no submit_sm on the wire (%d errors, %d cut short)",
			rep.SubmitErrors, rep.SubmitCutShort), rep.SubmitErr)
	}

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		return tier.fail("the two readings do not make a rate", err)
	}
	tier.Throughput = tp

	switch {
	case tp.ActiveBinds != float64(binds):
		// Peer-side truth against what was asked for. Without it, a tier where a third of the binds
		// were refused would be filed under a bind count nobody ever ran.
		return tier.fail(fmt.Sprintf("the peer held %.0f binds during the window, not the %d asked for",
			tp.ActiveBinds, binds), nil)
	case tp.Submitted == 0:
		return tier.fail("the peer's submit_sm counter did not move during the window", nil)
	case !tp.Qualified():
		tier.Status = TierDisqualified
		tier.Reason = fmt.Sprintf("the peer shed %.0f of the %.0f submit_sm it took (%s)",
			tp.NonSuccess, tp.Submitted, outcomes(tp))
		return tier
	case tp.Served > 0 && tp.Submitted-tp.Served > maxUnservedFraction*tp.Submitted:
		// Accepted is not served. A peer draining its receive queue and dropping the overflow silently
		// moves the received counter and no outcome at all, so the guard above sees a clean tier.
		tier.Status = TierDisqualified
		tier.Reason = fmt.Sprintf("the peer served only %.0f of the %.0f submit_sm it accepted (%.1f%% never came out)",
			tp.Served, tp.Submitted, 100*(tp.Submitted-tp.Served)/tp.Submitted)
		return tier
	}

	tier.Status = TierCounted
	return tier
}

// measure waits for the injection to begin, then takes the two readings inside its window.
//
// Both instants are absolute, counted from the start signal. Sleeping a duration AFTER each reading
// instead would add the scrapes' own latency to a tier whose end the injector already fixed, at
// start+Hold, before any of this ran: the injection would stop while the second reading was still in
// flight, and the rate would be divided by a window whose tail carried no load. Nothing about that is
// visible afterwards — every counter moved, every bind was up — which is why it is prevented here and
// then checked rather than assumed.
func (s *Sweeper) measure(ctx context.Context, started, done <-chan struct{}) (before, after smscmetrics.Snapshot, err error) {
	select {
	case <-started:
	case <-done:
		return before, after, errNotStarted
	case <-ctx.Done():
		return before, after, ctx.Err()
	}
	start := time.Now()

	if err := waitUntil(ctx, start.Add(s.cfg.Warmup)); err != nil {
		return before, after, fmt.Errorf("ceiling: warmup: %w", err)
	}
	if before, err = s.scrape(ctx); err != nil {
		return before, after, err
	}
	if err := waitUntil(ctx, start.Add(s.cfg.Warmup+s.cfg.Measure)); err != nil {
		return before, after, fmt.Errorf("ceiling: measurement window: %w", err)
	}
	if after, err = s.scrape(ctx); err != nil {
		return before, after, err
	}

	// The reading is timed on this side rather than by after.At: what must fall inside the injection is
	// the instant the peer served the request, and the only bound on it this process can vouch for is
	// when the call came back. Snapshot.At is a convention of the metrics reader — it has already
	// moved once — so a guard resting on it would be a guard resting on somebody else's comment.
	if took := time.Since(start); took > s.readingDeadline() {
		return before, after, fmt.Errorf(
			"%w: it came back %v after the injection began, past the %v bar (hold %v, settle %v)",
			errLateReading, took.Round(time.Millisecond), s.readingDeadline(), s.cfg.Hold(), s.cfg.Settle)
	}
	return before, after, nil
}

// readingDeadline is how long after the start signal the second reading may still come back. Half of
// Settle is spent rather than all of it so a tier is refused while there is slack left, not at the
// instant the guarantee is already broken.
func (s *Sweeper) readingDeadline() time.Duration {
	return s.cfg.Warmup + s.cfg.Measure + s.cfg.Settle/2
}

// scrape takes one reading, narrowed to the configured virtual SMSC.
func (s *Sweeper) scrape(ctx context.Context) (smscmetrics.Snapshot, error) {
	snap, err := s.scraper.Scrape(ctx)
	if err != nil {
		return smscmetrics.Snapshot{}, err
	}
	if s.cfg.VirtualSMSC == "" {
		return snap, nil
	}
	// Selecting an absent name yields an empty reading, whose deltas are all zero — a rate of 0/s that
	// reads exactly like a peer absorbing nothing. Name the typo instead.
	narrowed := snap.Select(s.cfg.VirtualSMSC)
	if len(narrowed.SMSCs) == 0 {
		return smscmetrics.Snapshot{}, fmt.Errorf("ceiling: the peer exposes no virtual SMSC %q, only %v",
			s.cfg.VirtualSMSC, snap.Names())
	}
	return narrowed, nil
}

// fail marks a tier failed with its reason and cause, and returns it so a caller can `return t.fail(…)`.
func (t Tier) fail(reason string, cause error) Tier {
	t.Status = TierFailed
	t.Reason = reason
	t.Err = cause
	if t.Err == nil {
		t.Err = errors.New(reason)
	}
	return t
}

// firstCause renders the injector's first bind failure, for a tier that failed to bind.
func firstCause(rep bindgen.Report) string {
	if len(rep.Errors) == 0 {
		return "no cause reported"
	}
	return rep.Errors[0].Error()
}

// bent reports whether the curve stopped scaling between two consecutive counted tiers, and returns
// the share of the proportional throughput increase the extra binds actually bought — 1 for a peer
// still scaling perfectly, 0 for one that went no faster at all, negative for one that went slower.
//
// It compares growth against growth rather than rate against rate on purpose: a peer whose absorbed
// rate still rises with every tier is not necessarily still scaling, and a sweep that only looked for
// the rate turning back down would need the peer to actually collapse before it noticed anything.
func bent(prev, cur Tier) (scaling float64, ok bool) {
	prevRate := prev.Throughput.SubmitPerSecond
	if prevRate <= 0 || prev.Binds <= 0 || cur.Binds <= prev.Binds {
		return 0, false
	}
	bindGrowth := float64(cur.Binds)/float64(prev.Binds) - 1
	rateGrowth := cur.Throughput.SubmitPerSecond/prevRate - 1
	scaling = rateGrowth / bindGrowth
	return scaling, scaling < minScalingFraction
}

// outcomes renders a window's per-outcome breakdown, sorted, for a disqualification line.
func outcomes(tp smscmetrics.Throughput) string {
	names := make([]string, 0, len(tp.Outcomes))
	for name := range tp.Outcomes {
		names = append(names, name)
	}
	slices.Sort(names)

	var out string
	for _, name := range names {
		if out != "" {
			out += ", "
		}
		out += fmt.Sprintf("%s=%.0f", name, tp.Outcomes[name])
	}
	return out
}

// waitUntil sleeps until the given instant, cut short by cancellation. An instant already past returns
// at once — a reading that cost more than the window it was supposed to fit in is not made good by
// waiting, it is caught by the deadline.
func waitUntil(ctx context.Context, t time.Time) error {
	return wait(ctx, time.Until(t))
}

// wait sleeps for d, cut short by cancellation.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
