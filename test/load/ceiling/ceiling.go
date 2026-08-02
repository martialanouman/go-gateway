// Package ceiling establishes the submit_sm throughput ceiling of the SMPP peer used for load runs
// (step-201, D3). It sweeps the number of concurrent binds, holds each tier long enough to be a
// measurement rather than a burst, and reads the absorbed rate from the peer's own /metrics.
//
// It answers one question: how fast can the test peer take submit_sm, so that a later reference run
// can be placed BELOW that figure. A reference run at the peer's ceiling measures the peer, not the
// gateway — and every capacity lever tuned against it would be tuned against an artefact.
//
// Two figures come out, not one: the curve of ceiling-versus-binds (Result.Tiers), and the ceiling at
// the bind count the reference run will use (Result.ReferenceCeiling). The curve says where the peer
// stops scaling; the reference figure is the one a reference run has to stay under.
//
// Three properties are what make the number trustworthy, and each is enforced rather than assumed:
//
//   - The rate is read from the peer (smscmetrics), never from the injector's own counters. An
//     injector that queues or retries cannot inflate it. The injector's report is kept only to answer
//     "did it push at all?".
//   - A tier whose window carried any non-success outcome is DISQUALIFIED: the peer was shedding, so
//     what it absorbed is not a rate it sustained. It stays in the curve, marked, and out of the
//     ceiling.
//   - A tier whose peer-side bind gauge disagrees with the bind count asked for is REFUSED. A sweep
//     that believes it has 40 binds while 12 were turned away measures something nobody named.
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
	// defaultMeasure is the length of one tier's measurement window. Sixty seconds is the floor D3
	// sets: shorter and the figure is a burst the peer's buffers absorbed, not a rate it held.
	defaultMeasure = 60 * time.Second

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
	Ceiling float64
	// CeilingBinds is the bind count that produced Ceiling.
	CeilingBinds int
	// ReferenceBinds is the bind count the reference run will use (Config.Reference).
	ReferenceBinds int
	// ReferenceCeiling is the absorbed rate at ReferenceBinds — the figure a reference run must stay
	// under. Zero when that tier did not count, which is an error rather than a result.
	ReferenceCeiling float64
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
	// Settle is the margin between the second reading and the end of the injection window.
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
	res := Result{ReferenceBinds: s.cfg.Reference}
	var errs []error

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
			// A finding, not a fault: the peer shed at this tier, which is what a ceiling looks like.
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
	case rep.Submitted == 0:
		return tier.fail(fmt.Sprintf("the injector put no submit_sm on the wire (%d errors)",
			rep.SubmitErrors), rep.SubmitErr)
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
	}

	tier.Status = TierCounted
	return tier
}

// measure waits for the injection to begin, then takes the two readings inside its window.
func (s *Sweeper) measure(ctx context.Context, started, done <-chan struct{}) (before, after smscmetrics.Snapshot, err error) {
	select {
	case <-started:
	case <-done:
		return before, after, errNotStarted
	case <-ctx.Done():
		return before, after, ctx.Err()
	}

	if err := wait(ctx, s.cfg.Warmup); err != nil {
		return before, after, fmt.Errorf("ceiling: warmup: %w", err)
	}
	if before, err = s.scrape(ctx); err != nil {
		return before, after, err
	}
	if err := wait(ctx, s.cfg.Measure); err != nil {
		return before, after, fmt.Errorf("ceiling: measurement window: %w", err)
	}
	if after, err = s.scrape(ctx); err != nil {
		return before, after, err
	}
	return before, after, nil
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
