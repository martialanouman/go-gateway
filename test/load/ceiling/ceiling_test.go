package ceiling_test

import (
	"context"
	"errors"
	"fmt"
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

// counters is one virtual SMSC's cumulative state at a point in time, as the fake peer would expose it.
type counters struct {
	submitted float64
	success   float64
	throttled float64
	binds     float64
	latSum    float64
	latCount  float64
}

func (c counters) snapshot(at time.Time) smscmetrics.Snapshot {
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
	return smscmetrics.Snapshot{At: at, SMSCs: map[string]smscmetrics.SMSC{"carrier": smsc}}
}

// tierScript is what the fake peer plays for one bind count: the counters before and after the
// measurement window, the injector's own report, and optional failures.
type tierScript struct {
	before    counters
	after     counters
	report    bindgen.Report
	injectErr error
	scrapeErr error
}

// absorbed builds a script for a tier that took n submit_sm over the window with no shedding, with the
// peer really holding the binds asked for.
func absorbed(binds int, n float64) tierScript {
	before := counters{submitted: 1000, success: 1000, binds: float64(binds), latSum: 5, latCount: 1000}
	after := counters{
		submitted: before.submitted + n,
		success:   before.success + n,
		binds:     float64(binds),
		latSum:    before.latSum + n*0.005,
		latCount:  before.latCount + n,
	}
	return tierScript{
		before: before,
		after:  after,
		report: bindgen.Report{Requested: binds, Bound: binds, Submitted: int(n) + 10, Accepted: int(n)},
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
	// startedBeforeFirstRead records whether injection had begun when the first reading was taken.
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
	p.mu.Unlock()

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

	s := p.scripts[lp.Binds]
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
	if n == 1 {
		return s.before.snapshot(time.Unix(0, 0)), nil
	}
	return s.after.snapshot(time.Unix(0, 0).Add(window)), nil
}

func (p *peer) params() []ceiling.LoadParams {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ceiling.LoadParams(nil), p.calls...)
}

// fastConfig is a sweep whose real waits are as short as the package allows: every rate comes from the
// fake's own timestamps, so the wall clock only has to not slow the test down.
func fastConfig(binds []int, reference int) ceiling.Config {
	return ceiling.Config{
		Binds:     binds,
		Reference: reference,
		Warmup:    time.Millisecond,
		Measure:   smscmetrics.MinWindow,
		Settle:    time.Millisecond,
		Cooldown:  time.Millisecond,
	}
}

// fastHold is the injection window fastConfig asks each tier for: warmup + measure + settle.
const fastHold = time.Millisecond + smscmetrics.MinWindow + time.Millisecond

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
