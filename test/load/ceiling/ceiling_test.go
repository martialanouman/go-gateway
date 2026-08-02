package ceiling_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
	"github.com/martialanouman/go-gateway/test/load/ceiling"
	"github.com/martialanouman/go-gateway/test/load/smscmetrics"
)

// window is the synthetic distance between a tier's two readings. The fake peer stamps its snapshots
// itself, so every rate below is computed over exactly this window whatever the test really slept —
// the sweep divides by the readings' own timestamps, never by a duration it thinks it waited.
const window = 60 * time.Second

// The virtual SMSCs the fake peer exposes. A second one exists so that narrowing the readings to one
// of them is a measurable behaviour and not a code path nobody ever walks.
const (
	carrier   = "carrier"
	neighbour = "neighbour"
)

// startLag is how long the fake peer waits before signalling that its injection has begun. It is far
// longer than fastConfig's warmup on purpose: a sweep that scraped on its own schedule instead of
// waiting for that signal would take its first reading while nothing was being injected, and the
// fixture has to be able to catch it.
const startLag = 30 * time.Millisecond

// counters is one virtual SMSC's cumulative state at a point in time, as the fake peer would expose it.
type counters struct {
	submitted float64
	success   float64
	throttled float64
	binds     float64
	latSum    float64
	latCount  float64
}

func (c counters) smsc() smscmetrics.SMSC {
	smsc := smscmetrics.SMSC{
		SubmitReceived: c.submitted,
		Outcomes:       map[string]float64{"success": c.success},
		ActiveBinds:    map[string]float64{"transceiver": c.binds},
		LatencySum:     c.latSum,
		LatencyCount:   c.latCount,
	}
	if c.throttled > 0 {
		smsc.Outcomes["throttled"] = c.throttled
	}
	return smsc
}

// tierScript is what the fake peer plays for one bind count: the counters before and after the
// measurement window, the injector's own report, and the ways a tier can go wrong.
type tierScript struct {
	before    counters
	after     counters
	report    bindgen.Report
	injectErr error
	scrapeErr error

	// neighbourBefore and neighbourAfter are a second virtual SMSC's counters on the same endpoint,
	// served only when hasNeighbour is set.
	neighbourBefore counters
	neighbourAfter  counters
	hasNeighbour    bool

	// neverStarts makes the injection return without ever signalling its start — a peer that gave up
	// while binding, which leaves no window to measure inside.
	neverStarts bool

	// slowSecondRead delays the second reading's return, so a tier whose measurement runs past the end
	// of the injection can be reproduced.
	slowSecondRead time.Duration
}

// snapshot renders the reading the fake peer serves: the carrier's counters, plus the neighbour's when
// the script configures one.
func (s tierScript) snapshot(second bool) smscmetrics.Snapshot {
	c, n, at := s.before, s.neighbourBefore, time.Unix(0, 0)
	if second {
		c, n, at = s.after, s.neighbourAfter, time.Unix(0, 0).Add(window)
	}
	snap := smscmetrics.Snapshot{At: at, SMSCs: map[string]smscmetrics.SMSC{carrier: c.smsc()}}
	if s.hasNeighbour {
		snap.SMSCs[neighbour] = n.smsc()
	}
	return snap
}

// absorbed builds a script for a tier that took n submit_sm over the window with no shedding, served
// every one of them, and whose peer really held the binds asked for.
func absorbed(binds int, n float64) tierScript {
	before := counters{submitted: 1000, success: 1000, binds: float64(binds), latSum: 5, latCount: 1000}
	after := counters{
		submitted: before.submitted + n,
		success:   before.success + n,
		binds:     float64(binds),
		latSum:    before.latSum + n*0.005,
		latCount:  before.latCount + n,
	}
	// A healthy windowed run ends with its WHOLE window outstanding on every session — measured at
	// exactly binds*32 against the real simulator — because a slot is only freed by a response and is
	// re-consumed at once. Anything less here would make the fixture describe a run that cannot happen.
	submitted := int(n) + 10
	per := submitted / binds
	return tierScript{
		before: before,
		after:  after,
		report: bindgen.Report{
			Requested: binds, Bound: binds, Submitted: submitted, Accepted: int(n),
			Unanswered:   binds * 32,
			SubmittedMin: per, SubmittedMax: per,
		},
	}
}

// peer is a fake SMSC seen through the two seams the sweep uses: it runs the load and serves the
// readings. The injection of a tier deliberately outlives its two readings — the peer only lets Inject
// return once the second scrape has been taken — so the ordering the sweep must respect (measure INSIDE
// the injection window) is what the fixture reproduces, not something the test hopes for.
type peer struct {
	scripts map[int]tierScript
	gate    chan struct{}

	mu      sync.Mutex
	current int
	reads   int
	calls   []ceiling.LoadParams
	// startedBeforeFirstRead records whether injection had begun when the first reading was taken. The
	// flag is raised by the OnStart callback itself, startLag after Inject was entered, so that
	// "the injector was called" and "the injection began" are two distinguishable instants.
	startedBeforeFirstRead bool
	started                bool
}

func newPeer(scripts map[int]tierScript) *peer {
	return &peer{scripts: scripts, gate: make(chan struct{}, 1)}
}

func (p *peer) Inject(ctx context.Context, lp ceiling.LoadParams) (bindgen.Report, error) {
	p.mu.Lock()
	p.current = lp.Binds
	p.reads = 0
	p.started = false
	p.calls = append(p.calls, lp)
	s := p.scripts[lp.Binds]
	p.mu.Unlock()

	if s.neverStarts {
		return s.report, s.injectErr
	}

	// Binding takes time before a single submit_sm goes out. Everything the sweep waits for has to be
	// counted from OnStart, not from the call that asked for the tier.
	select {
	case <-time.After(startLag):
	case <-ctx.Done():
		return bindgen.Report{}, ctx.Err()
	}

	if lp.OnStart != nil {
		p.mu.Lock()
		p.started = true
		p.mu.Unlock()
		lp.OnStart()
	}

	// The gate is preferred over cancellation rather than raced against it: the sweep cancels the tier
	// the instant its second reading is in hand, so both are ready at once and a bare select would pick
	// between them at random — a fixture that plays a different script one run in two.
	select {
	case <-p.gate:
	default:
		select {
		case <-p.gate:
		case <-ctx.Done():
			return bindgen.Report{}, ctx.Err()
		}
	}

	return s.report, s.injectErr
}

func (p *peer) Scrape(_ context.Context) (smscmetrics.Snapshot, error) {
	p.mu.Lock()
	s, ok := p.scripts[p.current]
	p.reads++
	n := p.reads
	if n == 1 {
		p.startedBeforeFirstRead = p.started
	}
	p.mu.Unlock()

	if n >= 2 {
		select {
		case p.gate <- struct{}{}:
		default:
		}
	}
	if !ok {
		return smscmetrics.Snapshot{}, fmt.Errorf("fake peer: no script for %d binds", p.current)
	}
	if s.scrapeErr != nil {
		return smscmetrics.Snapshot{}, s.scrapeErr
	}
	if n >= 2 && s.slowSecondRead > 0 {
		time.Sleep(s.slowSecondRead)
	}
	return s.snapshot(n >= 2), nil
}

func (p *peer) params() []ceiling.LoadParams {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ceiling.LoadParams(nil), p.calls...)
}

// fastConfig is a sweep whose real waits are as short as the package allows: every rate comes from the
// fake's own timestamps, so the wall clock only has to not slow the test down.
//
// Settle is the exception. The sweep never sleeps it — it cancels the tier as soon as the second
// reading is in — so a whole second here costs nothing, and it is what leaves the reading-deadline
// guard enough room not to fire on scheduling jitter alone.
func fastConfig(binds []int, reference int) ceiling.Config {
	return ceiling.Config{
		Binds:     binds,
		Reference: reference,
		Warmup:    time.Millisecond,
		Measure:   smscmetrics.MinWindow,
		Settle:    time.Second,
		Cooldown:  time.Millisecond,
	}
}

// fastHold is the injection window fastConfig asks each tier for: warmup + measure + settle.
const fastHold = time.Millisecond + smscmetrics.MinWindow + time.Second

func run(t *testing.T, p *peer, cfg ceiling.Config) (ceiling.Result, error) {
	t.Helper()
	s, err := ceiling.New(p, p, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.Run(ctx)
}

func tierByBinds(t *testing.T, res ceiling.Result, binds int) ceiling.Tier {
	t.Helper()
	for _, tier := range res.Tiers {
		if tier.Binds == binds {
			return tier
		}
	}
	t.Fatalf("no tier for %d binds in %d tiers", binds, len(res.Tiers))
	return ceiling.Tier{}
}

func TestSweepBuildsTheCurveAndPicksTheCeiling(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: absorbed(20, 24_000), // 400/s
		40: absorbed(40, 26_000), // 433/s
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(res.Tiers) != 3 {
		t.Fatalf("tiers = %d, want %d", len(res.Tiers), 3)
	}
	for _, want := range []struct {
		binds int
		rate  float64
	}{{10, 200}, {20, 400}, {40, 26_000.0 / 60}} {
		tier := tierByBinds(t, res, want.binds)
		if tier.Status != ceiling.TierCounted {
			t.Errorf("tier %d status = %v, want %v (%s)", want.binds, tier.Status, ceiling.TierCounted, tier.Reason)
		}
		if got := tier.Throughput.SubmitPerSecond; !nearly(got, want.rate) {
			t.Errorf("tier %d rate = %v, want %v", want.binds, got, want.rate)
		}
	}

	if !nearly(res.Ceiling, 26_000.0/60) {
		t.Errorf("Ceiling = %v, want %v", res.Ceiling, 26_000.0/60)
	}
	if res.CeilingBinds != 40 {
		t.Errorf("CeilingBinds = %d, want %d", res.CeilingBinds, 40)
	}
	// The second figure: the ceiling AT the reference bind count, not the best of the sweep.
	if !nearly(res.ReferenceCeiling, 400) {
		t.Errorf("ReferenceCeiling = %v, want %v", res.ReferenceCeiling, 400)
	}
	if res.ReferenceBinds != 20 {
		t.Errorf("ReferenceBinds = %d, want %d", res.ReferenceBinds, 20)
	}

	// The readings must be taken inside the injection window, and the injection must be asked to last
	// long enough to contain them. A sweep that scrapes around the run measures binding and teardown.
	if !p.startedBeforeFirstRead {
		t.Error("the first reading was taken before injection began")
	}
	calls := p.params()
	if len(calls) != 3 {
		t.Fatalf("Inject calls = %d, want %d", len(calls), 3)
	}
	for _, c := range calls {
		if c.Hold != fastHold {
			t.Errorf("tier %d Hold = %v, want %v", c.Binds, c.Hold, fastHold)
		}
	}
}

// TestSweepCeilingIsThePeakNotTheLastTier covers the shape a sweep is run to find: a curve that stops
// rising and turns back down. The peer's own contention makes 40 binds slower than 20 here, and the
// ceiling is the peak — a sweep that simply kept the last counted tier would publish the wrong figure
// AND the wrong bind count, and the reference run would be placed above a rate the peer never held.
func TestSweepCeilingIsThePeakNotTheLastTier(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: absorbed(20, 24_000), // 400/s — the peak
		40: absorbed(40, 18_000), // 300/s — more binds, less absorbed
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 10))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	last := tierByBinds(t, res, 40)
	if last.Status != ceiling.TierCounted {
		t.Fatalf("fixture: the last tier must count for this test to say anything, got %v", last.Status)
	}
	if last.Throughput.SubmitPerSecond >= 400 {
		t.Fatalf("fixture: the last tier read %v/s, it must sit below the peak", last.Throughput.SubmitPerSecond)
	}
	if !nearly(res.Ceiling, 400) {
		t.Errorf("Ceiling = %v, want %v", res.Ceiling, 400)
	}
	if res.CeilingBinds != 20 {
		t.Errorf("CeilingBinds = %d, want %d", res.CeilingBinds, 20)
	}
	if !nearly(res.ReferenceCeiling, 200) {
		t.Errorf("ReferenceCeiling = %v, want %v (the reference tier, not the peak)", res.ReferenceCeiling, 200)
	}
}

// TestSweepMarksACeilingItNeverReachedAsALowerBound is the difference between a measurement and a
// claim. A sweep whose every tier scaled with the binds it was given never found the peer's limit: it
// found the largest load it was asked to produce. Publishing that as a ceiling aims a capacity plan at
// a constraint that does not exist — which is what happened on 02/08, where the sweep stopped at 80
// binds and the peer went on absorbing three and a half times more.
func TestSweepMarksACeilingItNeverReachedAsALowerBound(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: absorbed(20, 24_000), // 400/s — doubling the binds doubled the rate
		40: absorbed(40, 48_000), // 800/s — and again
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, tier := range res.Tiers {
		if tier.Status != ceiling.TierCounted {
			t.Fatalf("fixture: tier %d is %v, every tier must count for this sweep to say nothing bent",
				tier.Binds, tier.Status)
		}
	}
	if res.Saturated {
		t.Errorf("Saturated = true (%q), want false — no tier shed and the curve never bent",
			res.SaturationReason)
	}
	if res.SaturationReason != "" {
		t.Errorf("SaturationReason = %q, want empty", res.SaturationReason)
	}
	if !nearly(res.Ceiling, 800) {
		t.Errorf("Ceiling = %v, want %v", res.Ceiling, 800)
	}
}

// TestSweepMarksSaturationWhenTheCurveBends covers the second of the two saturation signals. The first
// — a non-success outcome — is the peer saying it shed. This one is the peer saying nothing at all and
// simply not going any faster, which is what a ceiling looks like when the peer has no way to refuse.
func TestSweepMarksSaturationWhenTheCurveBends(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: absorbed(20, 24_000), // 400/s — still scaling
		40: absorbed(40, 25_200), // 420/s — twice the binds bought 5%
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	bent := tierByBinds(t, res, 40)
	// Guard against a hollow fixture: nothing else may catch this tier. It shed nothing, it served
	// everything, and its rate is the highest of the sweep.
	if bent.Status != ceiling.TierCounted {
		t.Fatalf("fixture: the bending tier is %v (%s), it must be a clean measurement", bent.Status, bent.Reason)
	}
	if bent.Throughput.SubmitPerSecond <= 400 {
		t.Fatalf("fixture: the bending tier read %v/s, it must still be the fastest of the sweep",
			bent.Throughput.SubmitPerSecond)
	}
	if !res.Saturated {
		t.Error("Saturated = false, want true — the curve stopped scaling at 40 binds")
	}
	if !strings.Contains(res.SaturationReason, "40") {
		t.Errorf("SaturationReason = %q, want it to name the tier where the curve bent", res.SaturationReason)
	}
}

// TestSweepMarksSaturationWhenThePeerShed is the other signal, and the one the sweep can take at face
// value: the peer answered a submit_sm with something other than success.
func TestSweepMarksSaturationWhenThePeerShed(t *testing.T) {
	shedding := absorbed(40, 48_000)
	shedding.after.throttled = 17

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: absorbed(20, 24_000),
		40: shedding,
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Saturated {
		t.Error("Saturated = false, want true — the peer shed at 40 binds")
	}
	if !strings.Contains(res.SaturationReason, "40") {
		t.Errorf("SaturationReason = %q, want it to name the tier the peer shed on", res.SaturationReason)
	}
}

// TestSweepCarriesTheWindowItMeasuredOver keeps a smoke run distinguishable from a figure worth
// recording, in the Result itself rather than in whoever remembers the flags they typed.
func TestSweepCarriesTheWindowItMeasuredOver(t *testing.T) {
	p := newPeer(map[int]tierScript{10: absorbed(10, 12_000)})

	cfg := fastConfig([]int{10}, 10)
	res, err := run(t, p, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Measure != cfg.Measure {
		t.Errorf("Measure = %v, want %v", res.Measure, cfg.Measure)
	}
	if res.Recordable() {
		t.Errorf("Recordable() = true for a %v window, want false under the %v floor",
			res.Measure, ceiling.MinRecordableMeasure)
	}

	long := ceiling.Result{Measure: ceiling.MinRecordableMeasure}
	if !long.Recordable() {
		t.Errorf("Recordable() = false at exactly %v, want true", ceiling.MinRecordableMeasure)
	}
}

func TestSweepDisqualifiesATierThePeerShedOn(t *testing.T) {
	shedding := absorbed(40, 30_000) // 500/s — the fastest tier of the sweep, on purpose
	shedding.after.throttled = 17

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: absorbed(20, 24_000), // 400/s
		40: shedding,
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	shed := tierByBinds(t, res, 40)
	// Guard against a hollow fixture: this tier must really have been the fastest reading of the sweep,
	// otherwise excluding it from the curve would prove nothing.
	if shed.Throughput.SubmitPerSecond <= 400 {
		t.Fatalf("fixture: the shedding tier read %v/s, it must outrank the counted ones to be a test",
			shed.Throughput.SubmitPerSecond)
	}
	if shed.Throughput.NonSuccess == 0 {
		t.Fatal("fixture: the shedding tier carried no non-success outcome")
	}
	if shed.Status != ceiling.TierDisqualified {
		t.Errorf("shedding tier status = %v, want %v", shed.Status, ceiling.TierDisqualified)
	}
	if !nearly(res.Ceiling, 400) {
		t.Errorf("Ceiling = %v, want %v (the disqualified tier must not enter the curve)", res.Ceiling, 400)
	}
	if res.CeilingBinds != 20 {
		t.Errorf("CeilingBinds = %d, want %d", res.CeilingBinds, 20)
	}
}

// TestSweepDisqualifiesATierThePeerDidNotServe covers the gap the outcome counter cannot see. The peer
// takes every PDU off the wire — smsc_submit_sm_received_total moves, no outcome is anything but
// success — and quietly drops what its buffer could not hold. The absorbed rate would then be an
// acceptance rate: a figure nobody could reproduce, published as a throughput.
func TestSweepDisqualifiesATierThePeerDidNotServe(t *testing.T) {
	starved := absorbed(20, 30_000)
	// The peer accepted 30 000 and only ever served 26 000 of them.
	starved.after.latCount = starved.before.latCount + 26_000
	starved.after.success = starved.before.success + 26_000

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000), // 200/s
		20: starved,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tier := tierByBinds(t, res, 20)
	// Guard against a hollow fixture: nothing else may be able to catch this tier. Every outcome it
	// carried was a success, so the shedding guard is blind to it.
	if tier.Throughput.NonSuccess != 0 {
		t.Fatalf("fixture: the tier carried %v non-success outcomes, the outcome guard would catch it anyway",
			tier.Throughput.NonSuccess)
	}
	if tier.Throughput.Served >= tier.Throughput.Submitted {
		t.Fatalf("fixture: served %v of %v accepted, the tier has no gap to detect",
			tier.Throughput.Served, tier.Throughput.Submitted)
	}
	if tier.Status != ceiling.TierDisqualified {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierDisqualified)
	}
	if !strings.Contains(tier.Reason, "26000") || !strings.Contains(tier.Reason, "30000") {
		t.Errorf("tier 20 Reason = %q, want it to name the served and accepted counts", tier.Reason)
	}
	if !nearly(res.Ceiling, 200) {
		t.Errorf("Ceiling = %v, want %v (the unserved tier must not set the ceiling)", res.Ceiling, 200)
	}
}

// TestSweepCountsATierWhoseServedTailLagsSlightly is the other half of the guard above: at the edges of
// any window the peer holds a few PDUs it has taken and not yet served, and a tier must not be thrown
// away for that. Without this the guard would refuse every real measurement.
func TestSweepCountsATierWhoseServedTailLagsSlightly(t *testing.T) {
	lagging := absorbed(20, 30_000)
	lagging.after.latCount = lagging.before.latCount + 29_800 // 200 still in the peer's hands: 0.7%

	p := newPeer(map[int]tierScript{20: lagging})

	res, err := run(t, p, fastConfig([]int{20}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierCounted {
		t.Errorf("tier 20 status = %v, want %v (%s)", tier.Status, ceiling.TierCounted, tier.Reason)
	}
}

// TestSweepFailsWhenSessionsStoppedBeingServed covers the failure no aggregate reports: a subset of the
// binds stops getting answers, the connections stay open, and the peer's bind gauge still reads the
// full count. Nothing in Failed, Dropped or the gauge moves — only the injector's in-flight tail does.
func TestSweepFailsWhenSessionsStoppedBeingServed(t *testing.T) {
	frozen := absorbed(20, 9_000)
	// Five of the twenty sessions went silent early: they got a handful of submissions through while
	// the fifteen still being served kept going. The totals stay plausible; only the spread shows it.
	frozen.report.SubmittedMin = 12

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: frozen,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a tier whose sessions went silent is filed under a bind count nobody ran")
	}

	tier := tierByBinds(t, res, 20)
	if tier.Report.Failed != 0 || tier.Report.Dropped != 0 {
		t.Fatalf("fixture: Failed = %d, Dropped = %d, both must be 0 or another guard catches this tier",
			tier.Report.Failed, tier.Report.Dropped)
	}
	// ... and the peer's own gauge still exposes every one of the binds, at both readings.
	if frozen.before.binds != 20 || frozen.after.binds != 20 {
		t.Fatalf("fixture: the peer's gauge reads %v/%v binds, it must read the full count or the gauge guard catches this tier",
			frozen.before.binds, frozen.after.binds)
	}
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !strings.Contains(tier.Reason, "12") {
		t.Errorf("tier 20 Reason = %q, want it to name the quietest session's count", tier.Reason)
	}
}

// TestSweepCountsATierWithAShortUnansweredTail is the other half: a windowed injector always ends with
// a few submissions outstanding, and that is not a frozen session.
func TestSweepCountsATierWithAShortUnansweredTail(t *testing.T) {
	tail := absorbed(20, 30_000)
	tail.report.Unanswered = 20 // one per session, the bandwidth-delay product of a healthy tier

	p := newPeer(map[int]tierScript{20: tail})

	res, err := run(t, p, fastConfig([]int{20}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if tier := tierByBinds(t, res, 20); tier.Status != ceiling.TierCounted {
		t.Errorf("tier 20 status = %v, want %v (%s)", tier.Status, ceiling.TierCounted, tier.Reason)
	}
}

// TestSweepRefusesATierWhoseSecondReadingCameBackLate is the ordering the whole measurement rests on.
// The injection stops at a fixed instant the injector was told about; if the second reading comes back
// after it, part of the window it is divided by carried no load at all and the rate is understated —
// silently, since every counter still moved and every bind was still up when the peer served it.
func TestSweepRefusesATierWhoseSecondReadingCameBackLate(t *testing.T) {
	cfg := fastConfig([]int{10, 20}, 10)

	late := absorbed(20, 30_000)
	// Long enough to run past warmup + measure + half the settle margin, which is where a reading stops
	// being safely inside the injection window.
	late.slowSecondRead = cfg.Warmup + cfg.Measure + cfg.Settle

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: late,
	})

	res, err := run(t, p, cfg)
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a reading taken after the injection stopped understates the rate")
	}

	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !nearly(res.Ceiling, 200) {
		t.Errorf("Ceiling = %v, want %v (the late tier must not set the ceiling)", res.Ceiling, 200)
	}
	// The tier that read on time is untouched.
	if tier := tierByBinds(t, res, 10); tier.Status != ceiling.TierCounted {
		t.Errorf("tier 10 status = %v, want %v (%s)", tier.Status, ceiling.TierCounted, tier.Reason)
	}
}

// TestSweepMeasuresFromTheStartSignalNotFromTheFirstReading pins the anchor. Both instants are counted
// from the start of the injection, so the cost of a slow first reading is spent INSIDE the measurement
// window. A sweep that waited its measure duration after that reading returned would push the second
// one past the end of the injection instead — and be refused for it.
//
// The delay below is the shape that tells the two apart: long enough to have shifted the second reading
// out of the injection window, short enough to fit inside the measurement window it eats into.
func TestSweepMeasuresFromTheStartSignalNotFromTheFirstReading(t *testing.T) {
	cfg := ceiling.Config{
		Binds:     []int{20},
		Reference: 20,
		Warmup:    time.Millisecond,
		Measure:   400 * time.Millisecond,
		Settle:    200 * time.Millisecond,
		Cooldown:  time.Millisecond,
	}
	const firstReadCost = 250 * time.Millisecond

	p := newPeer(map[int]tierScript{20: absorbed(20, 30_000)})
	slow := &slowFirstRead{peer: p, delay: firstReadCost}
	s, err := ceiling.New(slow, slow, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v — a slow first reading must be absorbed by the window, not added to it", err)
	}
	if tier := tierByBinds(t, res, 20); tier.Status != ceiling.TierCounted {
		t.Errorf("tier 20 status = %v, want %v (%s)", tier.Status, ceiling.TierCounted, tier.Reason)
	}
}

// slowFirstRead wraps the fake peer to make its FIRST reading slow. A sweep anchored on absolute
// instants absorbs that delay inside the measurement window; one that waits its measure duration after
// the reading returns pushes the whole window past the end of the injection.
type slowFirstRead struct {
	peer  *peer
	delay time.Duration

	mu   sync.Mutex
	read int
}

func (s *slowFirstRead) Inject(ctx context.Context, lp ceiling.LoadParams) (bindgen.Report, error) {
	return s.peer.Inject(ctx, lp)
}

func (s *slowFirstRead) Scrape(ctx context.Context) (smscmetrics.Snapshot, error) {
	s.mu.Lock()
	s.read++
	first := s.read == 1
	s.mu.Unlock()
	if first {
		time.Sleep(s.delay)
	}
	return s.peer.Scrape(ctx)
}

func TestSweepRefusesATierWhosePeerBindCountDisagrees(t *testing.T) {
	short := absorbed(40, 30_000)
	short.after.binds = 28 // 12 of the 40 were refused: this reading measures 28 binds, not 40
	short.before.binds = 28

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		40: short,
	})

	res, err := run(t, p, fastConfig([]int{10, 40}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure naming the refused tier")
	}

	tier := tierByBinds(t, res, 40)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 40 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if tier.Reason == "" {
		t.Error("tier 40 Reason is empty, want the observed and requested bind counts")
	}
	if !nearly(res.Ceiling, 200) {
		t.Errorf("Ceiling = %v, want %v (the refused tier must not enter the curve)", res.Ceiling, 200)
	}
}

func TestSweepFailsWhenATierFails(t *testing.T) {
	boom := errors.New("scrape: connection refused")
	broken := absorbed(40, 30_000)
	broken.scrapeErr = boom

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		40: broken,
	})

	res, err := run(t, p, fastConfig([]int{10, 40}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — one broken tier must not read as a clean sweep")
	}
	if !errors.Is(err, boom) {
		t.Errorf("Run error = %v, want it to wrap %v", err, boom)
	}
	// The curve of the tiers that did work is still returned: a failed tier costs its own reading, not
	// the whole run's.
	if tier := tierByBinds(t, res, 10); tier.Status != ceiling.TierCounted {
		t.Errorf("tier 10 status = %v, want %v", tier.Status, ceiling.TierCounted)
	}
	if tier := tierByBinds(t, res, 40); tier.Status != ceiling.TierFailed {
		t.Errorf("tier 40 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
}

func TestSweepFailsWhenTheInjectorPushedNothing(t *testing.T) {
	silent := absorbed(20, 0)
	silent.report.Submitted = 0
	silent.report.SubmitErrors = 640
	silent.report.SubmitErr = errors.New("write: broken pipe")

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: silent,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a peer reading of zero must not be published as a ceiling")
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
}

// TestSweepFailsWhenThePeerCounterDidNotMove is the misconfiguration that costs the most time in
// practice: -addr points at one peer and -metrics at another. The injector pushes, its own counters
// look healthy, and the endpoint being read never moves.
func TestSweepFailsWhenThePeerCounterDidNotMove(t *testing.T) {
	elsewhere := absorbed(20, 0) // the peer being READ took nothing
	// ... while the injector says it pushed on every one of its binds.
	elsewhere.report = bindgen.Report{Requested: 20, Bound: 20, Submitted: 30_000, Accepted: 30_000}

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: elsewhere,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a peer that counted nothing has no rate")
	}

	tier := tierByBinds(t, res, 20)
	// Guard against a hollow fixture: the injector-side guard must not be what catches this.
	if tier.Report.Submitted == 0 {
		t.Fatal("fixture: the injector reported nothing on the wire, the injector guard would catch this tier")
	}
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !strings.Contains(tier.Reason, "peer") {
		t.Errorf("tier 20 Reason = %q, want it to name the peer's counter", tier.Reason)
	}
}

func TestSweepFailsWhenBindsNeverBound(t *testing.T) {
	refused := absorbed(20, 24_000)
	refused.report.Bound = 14
	refused.report.Failed = 6
	refused.report.Errors = []error{errors.New("bind rejected: ESME_RBINDFAIL")}

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: refused,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a tier missing a third of its binds measures another sweep")
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !strings.Contains(tier.Reason, "ESME_RBINDFAIL") {
		t.Errorf("tier 20 Reason = %q, want it to carry the first bind failure", tier.Reason)
	}
}

func TestSweepFailsWhenThePeerDroppedSessions(t *testing.T) {
	dropped := absorbed(20, 24_000)
	dropped.report.Dropped = 7

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: dropped,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — sessions torn down mid-window are the peer over its ceiling")
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !strings.Contains(tier.Reason, "7") {
		t.Errorf("tier 20 Reason = %q, want it to name the number of dropped sessions", tier.Reason)
	}
}

// TestSweepFailsWhenTheInjectionNeverStarted covers the injector that gave up before opening its
// window. There is no interval to measure inside, and a sweep that scraped anyway would divide two
// identical readings and file the zero as a rate.
func TestSweepFailsWhenTheInjectionNeverStarted(t *testing.T) {
	gaveUp := absorbed(20, 24_000)
	gaveUp.neverStarts = true
	gaveUp.injectErr = errors.New("bindgen: dial 127.0.0.1:2775: connection refused")

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: gaveUp,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — an injection that never began leaves no window to measure")
	}
	if !errors.Is(err, gaveUp.injectErr) {
		t.Errorf("Run error = %v, want it to wrap %v", err, gaveUp.injectErr)
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
}

func TestSweepFailsWhenTheInjectionFailed(t *testing.T) {
	boom := errors.New("bindgen: write: broken pipe")
	broken := absorbed(20, 24_000)
	broken.injectErr = boom

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: broken,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 10))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — the injector's own error must not be swallowed")
	}
	if !errors.Is(err, boom) {
		t.Errorf("Run error = %v, want it to wrap %v", err, boom)
	}
	if tier := tierByBinds(t, res, 20); tier.Status != ceiling.TierFailed {
		t.Errorf("tier 20 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
}

// TestSweepIgnoresTheCancellationItIssuedItself is the exemption the injection-error guard turns on.
// The sweep cancels its own tier the instant the second reading is in hand, so a well-behaved injector
// reports that cancellation back. Treating it as a failure would fail every single tier of every sweep.
func TestSweepIgnoresTheCancellationItIssuedItself(t *testing.T) {
	cancelled := absorbed(20, 24_000)
	cancelled.injectErr = fmt.Errorf("bindgen: injection stopped: %w", context.Canceled)

	p := newPeer(map[int]tierScript{20: cancelled})

	res, err := run(t, p, fastConfig([]int{20}, 20))
	if err != nil {
		t.Fatalf("Run: %v — a cancellation the sweep itself issued is not a tier failure", err)
	}
	tier := tierByBinds(t, res, 20)
	if tier.Status != ceiling.TierCounted {
		t.Errorf("tier 20 status = %v, want %v (%s)", tier.Status, ceiling.TierCounted, tier.Reason)
	}
	// Guard against a hollow fixture: the injector really did return an error.
	if tier.Report.Submitted == 0 {
		t.Fatal("fixture: the tier's report is empty, the injector's return value was not the one scripted")
	}
}

// TestSweepReadsOnlyTheConfiguredVirtualSMSC covers the narrowing. A peer serving several virtual SMSCs
// exposes them on ONE endpoint, so a sweep binding to one of them and reading the lot would fold a
// neighbour's traffic into its own rate — and the busier the neighbour, the higher the ceiling reads.
func TestSweepReadsOnlyTheConfiguredVirtualSMSC(t *testing.T) {
	s := absorbed(10, 12_000) // the carrier: 200/s
	s.hasNeighbour = true
	// The neighbour is fifty times busier and belongs to somebody else's run.
	s.neighbourBefore = counters{submitted: 5_000, success: 5_000, latCount: 5_000}
	s.neighbourAfter = counters{submitted: 5_000 + 600_000, success: 5_000 + 600_000, latCount: 5_000 + 600_000}

	p := newPeer(map[int]tierScript{10: s})

	cfg := fastConfig([]int{10}, 10)
	cfg.VirtualSMSC = carrier
	res, err := run(t, p, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tier := tierByBinds(t, res, 10)
	if !nearly(tier.Throughput.SubmitPerSecond, 200) {
		t.Errorf("tier 10 rate = %v, want %v — the neighbour's traffic must not be in this figure",
			tier.Throughput.SubmitPerSecond, 200.0)
	}
	if !nearly(res.Ceiling, 200) {
		t.Errorf("Ceiling = %v, want %v", res.Ceiling, 200)
	}
}

// TestSweepFailsWhenTheVirtualSMSCIsUnknown is the anti-typo guard. Selecting a name the peer does not
// expose yields an empty reading whose deltas are all zero — a rate of 0/s indistinguishable from a
// peer absorbing nothing, which would be blamed on the injector for the rest of the campaign.
func TestSweepFailsWhenTheVirtualSMSCIsUnknown(t *testing.T) {
	s := absorbed(10, 12_000)
	s.hasNeighbour = true

	p := newPeer(map[int]tierScript{10: s})

	cfg := fastConfig([]int{10}, 10)
	cfg.VirtualSMSC = "carier" // the typo this guard exists for
	res, err := run(t, p, cfg)
	if err == nil {
		t.Fatal("Run: got nil error, want a failure naming the unknown virtual SMSC")
	}
	if !strings.Contains(err.Error(), "carier") {
		t.Errorf("Run error = %v, want it to quote the name asked for", err)
	}
	for _, name := range []string{carrier, neighbour} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("Run error = %v, want it to list the names the peer does expose (%q missing)", err, name)
		}
	}
	if tier := tierByBinds(t, res, 10); tier.Status != ceiling.TierFailed {
		t.Errorf("tier 10 status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
}

// TestSweepRunsTheTiersInAscendingOrder pins the ramp. Tiers are run from the lightest to the heaviest
// so the peer's sockets from one tier are gone before the next is counted; a sweep given -binds 80,10
// would otherwise measure ten binds through the teardown of eighty.
func TestSweepRunsTheTiersInAscendingOrder(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		40: absorbed(40, 48_000),
	})

	res, err := run(t, p, fastConfig([]int{40, 10}, 10))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls := p.params()
	order := make([]int, 0, len(calls))
	for _, c := range calls {
		order = append(order, c.Binds)
	}
	if len(order) != 2 || order[0] != 10 || order[1] != 40 {
		t.Errorf("injection order = %v, want [10 40]", order)
	}
	curve := make([]int, 0, len(res.Tiers))
	for _, tier := range res.Tiers {
		curve = append(curve, tier.Binds)
	}
	if len(curve) != 2 || curve[0] != 10 || curve[1] != 40 {
		t.Errorf("curve order = %v, want [10 40]", curve)
	}
}

// TestNewNormalisesTheSweep covers what a run with no flags actually gets: -reference 0 is the
// command's default, so the tier the reference figure is read at is chosen by this code on every real
// run, and nothing else picks it.
func TestNewNormalisesTheSweep(t *testing.T) {
	p := newPeer(nil)

	t.Run("defaults", func(t *testing.T) {
		s, err := ceiling.New(p, p, ceiling.Config{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		cfg := s.Config()
		if want := []int{10, 20, 40, 80}; !equalInts(cfg.Binds, want) {
			t.Errorf("Binds = %v, want %v", cfg.Binds, want)
		}
		// The largest tier a single connector pool could reproduce: bind_pool_size is capped at 32.
		if cfg.Reference != 20 {
			t.Errorf("Reference = %d, want %d", cfg.Reference, 20)
		}
		if cfg.Measure != 60*time.Second {
			t.Errorf("Measure = %v, want %v", cfg.Measure, 60*time.Second)
		}
	})

	t.Run("reference is the largest tier a pool could reproduce", func(t *testing.T) {
		s, err := ceiling.New(p, p, ceiling.Config{Binds: []int{8, 32, 64}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := s.Config().Reference; got != 32 {
			t.Errorf("Reference = %d, want %d", got, 32)
		}
	})

	t.Run("no tier a pool could reproduce", func(t *testing.T) {
		_, err := ceiling.New(p, p, ceiling.Config{Binds: []int{64, 128}})
		if err == nil {
			t.Fatal("New: got nil error, want a rejection — no tier is at most 32 binds")
		}
		if !strings.Contains(err.Error(), "32") {
			t.Errorf("New error = %v, want it to name the bound", err)
		}
	})

	t.Run("the sweep is sorted into a ramp", func(t *testing.T) {
		s, err := ceiling.New(p, p, ceiling.Config{Binds: []int{80, 10, 40}, Reference: 10})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if want := []int{10, 40, 80}; !equalInts(s.Config().Binds, want) {
			t.Errorf("Binds = %v, want %v", s.Config().Binds, want)
		}
	})
}

func TestSweepFailsWhenTheReferenceTierDidNotCount(t *testing.T) {
	shedding := absorbed(20, 24_000)
	shedding.after.throttled = 3

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: shedding,
	})

	res, err := run(t, p, fastConfig([]int{10, 20}, 20))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — the reference figure is a deliverable, not an option")
	}
	if res.ReferenceCeiling != 0 {
		t.Errorf("ReferenceCeiling = %v, want 0 for a reference tier that did not count", res.ReferenceCeiling)
	}
	// The curve itself is still usable.
	if !nearly(res.Ceiling, 200) {
		t.Errorf("Ceiling = %v, want %v", res.Ceiling, 200)
	}
}

func TestNewRejectsAConfigThatCouldNotMeasure(t *testing.T) {
	p := newPeer(nil)
	cases := []struct {
		name string
		cfg  ceiling.Config
	}{
		{"reference outside the sweep", fastConfig([]int{10, 20}, 15)},
		{"non-positive bind count", fastConfig([]int{10, 0}, 10)},
		{"duplicate tier", fastConfig([]int{10, 10}, 10)},
		{"measurement window below the metrics minimum", ceiling.Config{
			Binds: []int{10}, Reference: 10, Measure: smscmetrics.MinWindow - time.Nanosecond,
		}},
		{"negative warmup", ceiling.Config{Binds: []int{10}, Reference: 10, Warmup: -time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ceiling.New(p, p, tc.cfg); err == nil {
				t.Fatal("New: got nil error, want a rejection")
			}
		})
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	p := newPeer(nil)
	if _, err := ceiling.New(nil, p, fastConfig([]int{10}, 10)); err == nil {
		t.Error("New with no load: got nil error, want a rejection")
	}
	if _, err := ceiling.New(p, nil, fastConfig([]int{10}, 10)); err == nil {
		t.Error("New with no scraper: got nil error, want a rejection")
	}
}

// close compares two rates with the tolerance a float division deserves.
func nearly(got, want float64) bool {
	d := got - want
	return d < 1e-6 && d > -1e-6
}

func equalInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestSweepUnmarksASaturationTheCurveWentOnToDisprove guards the way the CEILING/LOWER BOUND
// distinction can be defeated from the other side. A single transient dip — a background process
// stealing CPU for one tier, on the shared host the README warns about — bends the curve and marks the
// sweep saturated. If the tiers above then go on scaling, that dip was noise, not a limit: keeping the
// mark would print CEILING over a sweep that was still doubling, which is the exact claim the
// saturation flag exists to prevent.
func TestSweepUnmarksASaturationTheCurveWentOnToDisprove(t *testing.T) {
	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: absorbed(20, 15_000), // the dip: +25% for twice the binds, scaling 0.25 — the curve "bends"
		40: absorbed(40, 48_000),
		80: absorbed(80, 96_000), // ... and then it doubles, twice
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40, 80}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Fixture guard: the dip must really have bent the curve, or this test proves nothing.
	dip := tierByBinds(t, res, 20)
	prev := tierByBinds(t, res, 10)
	if dip.Throughput.SubmitPerSecond >= 2*prev.Throughput.SubmitPerSecond {
		t.Fatalf("fixture: 20-bind tier at %.0f/s against %.0f/s did not bend the curve",
			dip.Throughput.SubmitPerSecond, prev.Throughput.SubmitPerSecond)
	}
	top := tierByBinds(t, res, 80)
	if top.Throughput.SubmitPerSecond <= dip.Throughput.SubmitPerSecond {
		t.Fatalf("fixture: the curve did not recover past the dip (%.0f/s at 80 against %.0f/s at 20)",
			top.Throughput.SubmitPerSecond, dip.Throughput.SubmitPerSecond)
	}

	if res.Saturated {
		t.Errorf("Saturated = true (%q), want false: tiers above the dip went on scaling, so the peer was never shown to have a limit",
			res.SaturationReason)
	}
	if res.CeilingBinds != 80 {
		t.Errorf("CeilingBinds = %d, want 80", res.CeilingBinds)
	}
}

// TestSweepKeepsAShedTierEvenWhenALaterTierOutrunsABend guards the asymmetry the two saturation
// signals are supposed to have. A bend is inferred from a rate and a later tier can disprove it; a
// tier the peer SHED on is evidence the peer itself reported, and no later tier withdraws it.
//
// The dangerous ordering is bend-then-shed: markSaturation keeps the first evidence, so the shed
// leaves saturationFromBend set, and the withdrawal then takes the shed away with the bend. The tool
// prints "no tier shed" over a sweep that has a disqualified tier in its own curve.
func TestSweepKeepsAShedTierEvenWhenALaterTierOutrunsABend(t *testing.T) {
	shed := absorbed(40, 12_000)
	shed.after.success = shed.before.success + 11_000
	shed.after.throttled = 1_000

	p := newPeer(map[int]tierScript{
		10: absorbed(10, 12_000),
		20: absorbed(20, 12_600), // the dip: scaling 0.05, the curve "bends"
		40: shed,                 // the peer itself sheds — evidence it reported
		80: absorbed(80, 54_000), // ... and a later tier outruns the bend
	})

	res, err := run(t, p, fastConfig([]int{10, 20, 40, 80}, 20))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Fixture guards: the bend and the shed must both really have happened, in that order.
	if tierByBinds(t, res, 40).Status != ceiling.TierDisqualified {
		t.Fatalf("fixture: tier 40 status = %v, want %v — the peer must have shed",
			tierByBinds(t, res, 40).Status, ceiling.TierDisqualified)
	}
	top := tierByBinds(t, res, 80)
	dip := tierByBinds(t, res, 20)
	if top.Throughput.SubmitPerSecond <= dip.Throughput.SubmitPerSecond {
		t.Fatalf("fixture: tier 80 (%.0f/s) must outrun the dip (%.0f/s) to attempt the withdrawal",
			top.Throughput.SubmitPerSecond, dip.Throughput.SubmitPerSecond)
	}

	if !res.Saturated {
		t.Errorf("Saturated = false, want true: the peer shed at 40 binds, which no later tier can undo")
	}
	if !strings.Contains(res.SaturationReason, "shed") {
		t.Errorf("SaturationReason = %q, want it to name the shedding the peer reported", res.SaturationReason)
	}
}

// TestSweepFailsAgainstAPeerThatNeverAnswers covers the shape every other guard misses. A peer that
// accepts submit_sm and never responds moves its receive counter, so the rate looks real and scales
// perfectly with the binds; it reports no outcome, so Qualified() is true; and it stalls every session
// identically after one window, so the spread is 1. Its served histogram never moves either, which
// disables the served-versus-accepted guard rather than tripping it.
//
// What gives it away is that essentially everything the injector sent is still outstanding: a healthy
// tier leaves its window (a few hundred PDUs against hundreds of thousands sent), this one leaves all
// of it.
func TestSweepFailsAgainstAPeerThatNeverAnswers(t *testing.T) {
	const binds, window = 20, 32
	deaf := absorbed(binds, binds*window) // it "absorbed" exactly one window per session, then stalled
	deaf.report.Submitted = binds * window
	deaf.report.Accepted = 0
	deaf.report.Unanswered = binds * window
	deaf.report.SubmittedMin = window
	deaf.report.SubmittedMax = window
	// A peer that never answers never serves: its latency histogram stays put.
	deaf.after.latSum = deaf.before.latSum
	deaf.after.latCount = deaf.before.latCount

	p := newPeer(map[int]tierScript{binds: deaf})
	res, err := run(t, p, fastConfig([]int{binds}, binds))
	if err == nil {
		t.Fatal("Run: got nil error, want a failure — a peer that answered nothing produced a publishable figure")
	}

	tier := tierByBinds(t, res, binds)
	// Fixture guards: every other guard must really be blind here, or this test proves nothing.
	if tier.Report.Failed != 0 || tier.Report.Dropped != 0 {
		t.Fatalf("fixture: Failed = %d, Dropped = %d, both must be 0", tier.Report.Failed, tier.Report.Dropped)
	}
	if tier.Report.SubmittedMin*4 < tier.Report.SubmittedMax {
		t.Fatalf("fixture: spread %d/%d already trips the spread guard",
			tier.Report.SubmittedMin, tier.Report.SubmittedMax)
	}
	if tier.Status != ceiling.TierFailed {
		t.Errorf("tier status = %v, want %v", tier.Status, ceiling.TierFailed)
	}
	if !strings.Contains(tier.Reason, "unanswered") {
		t.Errorf("tier Reason = %q, want it to name the unanswered submissions", tier.Reason)
	}
}
