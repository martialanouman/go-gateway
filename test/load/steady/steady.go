// Package steady scores a local reference run against the steady-state criteria of step-201 (D2).
//
// It answers one question, and it is deliberately not "how fast did it go": does the gateway hold a
// STEADY STATE at or above the spec's per-worker lower bound (§2.5, ~1 vCPU sustains 1 000–2 000 msg/s)?
// A run that accepts 1 000 msg/s while emitting 400 is not a throughput, it is a queue filling up — and
// reporting its acceptance rate is exactly the mistake D1 exists to prevent.
//
// So the verdict is a conjunction, and every clause can fail on its own:
//
//   - the window is at least [Criteria.MinWindow] — a burst is not a tier;
//   - the accept rate clears [Criteria.MinThroughput];
//   - the OUTPUT rate equals the accept rate, to the segmentation margin;
//   - the Kafka consumer lag is flat rather than climbing;
//   - the ingest p99 is inside its budget;
//   - nothing errored, on either leg;
//   - the connector breaker is closed;
//   - the whole thing sits under the peer ceiling measured in D3.
//
// # Nothing missing may read as a pass
//
// Every input whose absence looks like health is refused rather than skipped: a zero window (no
// division, no infinite rate), fewer than [Criteria.MinLagSamples] lag readings (two samples cannot
// tell flat from climbing), an ingest p99 over zero observations (which would otherwise read as the
// fastest run ever recorded), and an unknown peer ceiling (D2 places the run UNDER a figure, so having
// no figure means the criterion was skipped, not met).
//
// The evaluator is pure: it takes a [Measurement] and returns a [Verdict]. What produces the
// measurement — an in-process harness, a k6 run, a remote scrape — is the caller's business, which is
// what lets the criteria be unit-tested without an ounce of infrastructure.
package steady

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Check names. They are constants because a report is read by name and a test asserts on one: a typo in
// a literal would silently assert on a check that does not exist.
const (
	// CheckWindow is the "was this a tier at all" bar (D2: ≥ 60 s).
	CheckWindow = "window"
	// CheckThroughput is the accept rate against Criteria.MinThroughput.
	CheckThroughput = "throughput"
	// CheckBalance is output-equals-acceptance, the clause that separates a throughput from a backlog.
	CheckBalance = "input/output balance"
	// CheckLag is the Kafka consumer lag trend.
	CheckLag = "kafka lag"
	// CheckIngestP99 is the §1.2 ingestion budget.
	CheckIngestP99 = "ingest p99"
	// CheckErrors is the zero-error clause, both legs.
	CheckErrors = "errors"
	// CheckBreaker is the connector circuit-breaker state.
	CheckBreaker = "breaker"
	// CheckPeerCeiling places the run below the ceiling measured in D3.
	CheckPeerCeiling = "below peer ceiling"
	// CheckBehind is the share of the window the injector itself was late for.
	CheckBehind = "injector on schedule"
)

// Criteria are the bars a reference run has to clear. They live with the caller rather than in a
// package default so a smoke run and a measurement can share the evaluator with different bars, and so
// a relaxed bar is visible at the call site instead of hidden behind a zero value.
type Criteria struct {
	// MinWindow is the shortest measurement window whose figures count (D2: 60 s).
	MinWindow time.Duration

	// MinThroughput is the accept rate the run must reach, in messages per second.
	MinThroughput float64

	// SegmentsPerMessage is how many submit_sm one accepted message is expected to produce. It is 1 for
	// a single-segment run; a run built on longer bodies declares its own factor here rather than
	// widening MaxSegmentationDrift, which would blunt the check for every run.
	SegmentsPerMessage float64

	// MaxSegmentationDrift is how far the observed output may sit from Accepted×SegmentsPerMessage, as
	// a fraction of the latter. It covers the messages in flight at each end of the window, not a
	// backlog.
	MaxSegmentationDrift float64

	// MaxLagSlopeFraction bounds the consumer lag's growth, as a fraction of the accept rate. A backlog
	// climbing at 1% of what is coming in is a queue filling, however healthy every counter looks.
	MaxLagSlopeFraction float64

	// MinLagSamples is how many lag readings the window must carry for its trend to be a measurement.
	MinLagSamples int

	// IngestP99Budget is the §1.2 acceptance budget (250 ms).
	IngestP99Budget time.Duration

	// MaxBehindFraction is the share of the window's attempts the injector may start late before the
	// run stops being about the gateway at all. Past it the achieved rate is a property of THIS
	// HARNESS: the injector asked for a rate it could not deliver, and every figure downstream is
	// bounded by its own shortfall rather than by anything under test. Like PeerCeiling, zero is a bar
	// and not a switch — a caller that does not set it is asking for an injector that was never late.
	MaxBehindFraction float64

	// PeerCeiling is the submit_sm rate the test peer was measured to absorb (D3). A run at or above it
	// measures the peer, not the gateway. Zero means "never measured", which fails.
	PeerCeiling float64
}

// LagSample is one reading of a consumer group's backlog.
type LagSample struct {
	// At is when the reading was taken.
	At time.Time
	// Records is the group's total lag across the topic, in records.
	Records int64
}

// Measurement is everything one reference run observed over its window.
//
// The counters are deliberately taken from three different places, because agreeing with itself is the
// one thing a harness must not be able to do: Accepted and Errors are the injector's own view of the
// HTTP leg, Submitted and SubmitRejected come from the gateway's submits_total (fed at the
// submit_sm_resp), and Lag comes from the broker.
type Measurement struct {
	// Window is the measured interval — warmup and settle excluded.
	Window time.Duration

	// Accepted is how many submissions the gateway answered 202 to during the window.
	Accepted uint64

	// Errors is how many submissions did not get a 202: a transport failure, a timeout, any other
	// status.
	Errors uint64

	// Submitted is how many submit_sm reached a terminal outcome during the window, refusals included.
	Submitted uint64

	// SubmitRejected is how many of those were refused by the peer.
	SubmitRejected uint64

	// IngestP99 is the 99th percentile of the client-observed acceptance latency. It comes from the
	// injector's own samples rather than from a histogram exposition: ingest_duration_seconds is
	// declared in the catalogue and observed nowhere, and its bucket edges straddle the 250 ms budget
	// anyway (0.128 / 0.256), so no exposition of it could decide this criterion today.
	IngestP99 time.Duration

	// IngestSamples is how many latencies that percentile was drawn from. Zero fails: a p99 of nothing
	// is not a fast run.
	IngestSamples uint64

	// Lag is the consumer-lag trace over the window, in arrival order.
	Lag []LagSample

	// Behind is how many of the window's attempts started after their scheduled instant — the injector
	// could not keep up. Read against Accepted+Errors it gives the share of the window that is a
	// property of the harness rather than of the gateway. It comes from the injector, like Accepted and
	// Errors, and it is windowed the same way (see steady.Sample.Late).
	Behind uint64

	// BreakerClosed reports the connector breaker's state at the end of the window.
	BreakerClosed bool
}

// Check is one clause of the verdict.
type Check struct {
	// Name is one of the Check* constants.
	Name string
	// Pass reports whether this clause held.
	Pass bool
	// Detail is the figures behind the verdict, in one line fit for a report — always populated, pass
	// or fail, so a reader sees what was measured and not only what was concluded.
	Detail string
}

// Verdict is the scored run: every clause, and the derived figures the clauses were drawn from.
type Verdict struct {
	// Checks is every clause, in a fixed order.
	Checks []Check

	// AcceptRate is Accepted over the window, in messages per second. Zero over a zero window.
	AcceptRate float64

	// OutputRate is Submitted over the window, in submit_sm per second.
	OutputRate float64

	// LagSlope is the least-squares trend of the backlog over the window, in records per second.
	// Positive means the queue is filling. NaN when there were too few readings to fit a line.
	LagSlope float64
}

// Pass reports whether every clause held. An empty verdict does not pass: it scored nothing.
func (v Verdict) Pass() bool {
	if len(v.Checks) == 0 {
		return false
	}
	for _, c := range v.Checks {
		if !c.Pass {
			return false
		}
	}
	return true
}

// String renders the whole verdict — every clause with its figures, then the overall answer. It prints
// the passing clauses too: a reader has to see which bars were actually exercised, not only which one
// broke.
func (v Verdict) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "accept %.0f msg/s · output %.0f submit_sm/s · lag slope %+.1f rec/s\n",
		v.AcceptRate, v.OutputRate, v.LagSlope)
	for _, c := range v.Checks {
		mark := "FAIL"
		if c.Pass {
			mark = "ok  "
		}
		fmt.Fprintf(&b, "  [%s] %-20s %s\n", mark, c.Name, c.Detail)
	}
	result := "FAILED"
	if v.Pass() {
		result = "PASSED"
	}
	fmt.Fprintf(&b, "  => steady-state criteria %s", result)
	return b.String()
}

// Evaluate scores a measurement against the criteria. It never returns an error: a run that cannot be
// scored is a run that failed a clause, and saying so in the verdict is what keeps a missing input from
// being mistaken for a met one.
func Evaluate(m Measurement, c Criteria) Verdict {
	v := Verdict{
		AcceptRate: rate(m.Accepted, m.Window),
		OutputRate: rate(m.Submitted, m.Window),
		LagSlope:   lagSlope(m.Lag),
	}
	v.Checks = []Check{
		windowCheck(m, c),
		throughputCheck(v.AcceptRate, c),
		balanceCheck(m, c, v),
		lagCheck(m, c, v),
		ingestCheck(m, c),
		behindCheck(m, c),
		errorsCheck(m),
		breakerCheck(m),
		peerCeilingCheck(v.OutputRate, c),
	}
	return v
}

func windowCheck(m Measurement, c Criteria) Check {
	return Check{
		Name:   CheckWindow,
		Pass:   m.Window > 0 && m.Window >= c.MinWindow,
		Detail: fmt.Sprintf("held %v, want at least %v", m.Window.Round(time.Millisecond), c.MinWindow),
	}
}

func throughputCheck(acceptRate float64, c Criteria) Check {
	return Check{
		Name:   CheckThroughput,
		Pass:   acceptRate >= c.MinThroughput,
		Detail: fmt.Sprintf("accepted %.0f msg/s, want at least %.0f", acceptRate, c.MinThroughput),
	}
}

// balanceCheck is the clause that distinguishes a throughput from a backlog. It compares COUNTS rather
// than rates so the window cancels out of both sides exactly.
func balanceCheck(m Measurement, c Criteria, v Verdict) Check {
	expected := float64(m.Accepted) * c.SegmentsPerMessage
	drift := math.Abs(float64(m.Submitted) - expected)
	relative := 0.0
	if expected > 0 {
		relative = drift / expected
	}
	return Check{
		Name: CheckBalance,
		Pass: expected > 0 && drift <= c.MaxSegmentationDrift*expected,
		Detail: fmt.Sprintf(
			"%d submit_sm out for %d accepted (%.0f/s out vs %.0f/s in), %.2f%% off the %.1f segments/message expected, tolerance %.0f%%",
			m.Submitted, m.Accepted, v.OutputRate, v.AcceptRate, 100*relative, c.SegmentsPerMessage,
			100*c.MaxSegmentationDrift),
	}
}

// lagCheck refuses a trend drawn from too few readings before it looks at the slope: a flat line
// through two points is not evidence of a flat backlog, and passing on it would make the clause
// unfalsifiable exactly when the harness is misconfigured.
func lagCheck(m Measurement, c Criteria, v Verdict) Check {
	if len(m.Lag) < c.MinLagSamples {
		return Check{
			Name: CheckLag,
			Detail: fmt.Sprintf("%d readings over the window, want at least %d: the trend is not measurable",
				len(m.Lag), c.MinLagSamples),
		}
	}
	if math.IsNaN(v.LagSlope) {
		return Check{
			Name:   CheckLag,
			Detail: "the readings share one instant, so no trend can be fitted",
		}
	}
	bar := c.MaxLagSlopeFraction * v.AcceptRate
	return Check{
		Name: CheckLag,
		Pass: v.LagSlope <= bar,
		Detail: fmt.Sprintf(
			"backlog trend %+.1f rec/s over %d readings (first %d, last %d), want at most %+.1f (%.0f%% of the accept rate)",
			v.LagSlope, len(m.Lag), m.Lag[0].Records, m.Lag[len(m.Lag)-1].Records, bar,
			100*c.MaxLagSlopeFraction),
	}
}

func ingestCheck(m Measurement, c Criteria) Check {
	if m.IngestSamples == 0 {
		return Check{
			Name:   CheckIngestP99,
			Detail: "no acceptance latency was observed, so there is no p99 to score",
		}
	}
	return Check{
		Name: CheckIngestP99,
		Pass: m.IngestP99 < c.IngestP99Budget,
		Detail: fmt.Sprintf("p99 %v over %d samples, budget %v",
			m.IngestP99.Round(time.Millisecond), m.IngestSamples, c.IngestP99Budget),
	}
}

func errorsCheck(m Measurement) Check {
	return Check{
		Name: CheckErrors,
		Pass: m.Errors == 0 && m.SubmitRejected == 0,
		Detail: fmt.Sprintf("%d submission errors, %d submit_sm refused, want 0 and 0",
			m.Errors, m.SubmitRejected),
	}
}

func breakerCheck(m Measurement) Check {
	state := "open or half-open"
	if m.BreakerClosed {
		state = "closed"
	}
	return Check{
		Name:   CheckBreaker,
		Pass:   m.BreakerClosed,
		Detail: fmt.Sprintf("connector breaker %s, want closed", state),
	}
}

// peerCeilingCheck places the run under the figure D3 measured. An unmeasured ceiling fails: D2 asks
// for a run BELOW a number, and there is no below without one.
// behindCheck scores the injector against its own schedule.
//
// It is the clause that keeps a run from being read as the gateway's when the number it produced was
// the harness's. inject.go has said so since it was written — "a large share means the achieved rate is
// a property of this harness and not of the gateway" — but nothing applied it: the figure was printed
// beside the verdict and never in it, so a run 17.3% behind was published with a footnote instead of
// failing (step-201e).
//
// The share is taken over the window's own attempts, never over the run: warmup is where an injector
// catches up to its schedule, so a run total would score the ramp-up and fail good measurements.
func behindCheck(m Measurement, c Criteria) Check {
	attempted := m.Accepted + m.Errors
	if attempted == 0 {
		return Check{
			Name:   CheckBehind,
			Detail: "no submission landed in the window, so the injector's own schedule cannot be scored",
		}
	}
	share := float64(m.Behind) / float64(attempted)
	return Check{
		Name: CheckBehind,
		Pass: share <= c.MaxBehindFraction,
		Detail: fmt.Sprintf("%d of %d attempts started late (%.1f%%), budget %.1f%%",
			m.Behind, attempted, 100*share, 100*c.MaxBehindFraction),
	}
}

func peerCeilingCheck(outputRate float64, c Criteria) Check {
	if c.PeerCeiling <= 0 {
		return Check{
			Name:   CheckPeerCeiling,
			Detail: "no peer ceiling was measured, so the run cannot be placed under one (D3)",
		}
	}
	return Check{
		Name: CheckPeerCeiling,
		Pass: outputRate < c.PeerCeiling,
		Detail: fmt.Sprintf("%.0f submit_sm/s against a measured ceiling of %.0f/s (%.1f%% of it)",
			outputRate, c.PeerCeiling, 100*outputRate/c.PeerCeiling),
	}
}

// rate divides a count by a window, yielding zero rather than an infinity on a zero window. A run that
// never started must not report the fastest throughput ever recorded.
func rate(n uint64, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return float64(n) / window.Seconds()
}

// lagSlope fits a least-squares line through the backlog readings and returns its slope in records per
// second. It returns NaN when fewer than two readings exist, or when they all share one instant — the
// two cases where no line is defined.
//
// A slope is used rather than "last minus first" because a single spike at either end of the window
// would then decide the verdict on its own, in whichever direction it happened to land.
func lagSlope(samples []LagSample) float64 {
	if len(samples) < 2 {
		return math.NaN()
	}
	origin := samples[0].At
	var sumX, sumY, sumXY, sumXX float64
	for _, s := range samples {
		x := s.At.Sub(origin).Seconds()
		y := float64(s.Records)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	n := float64(len(samples))
	denom := n*sumXX - sumX*sumX
	if denom == 0 {
		return math.NaN()
	}
	return (n*sumXY - sumX*sumY) / denom
}

// Percentile is the q-th percentile of the samples, by nearest rank: the smallest sample at or above
// which q of them sit. It is exact — no bucketing, no interpolation — because the injector keeps every
// sample it took, unlike a Prometheus exposition.
//
// It copies before sorting: the samples belong to the caller, which may still want them in arrival
// order. An empty slice yields zero, which callers must not read as a fast run — see
// [Measurement.IngestSamples].
func Percentile(samples []time.Duration, q float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)

	rank := int(math.Ceil(q * float64(len(sorted))))
	switch {
	case rank < 1:
		rank = 1
	case rank > len(sorted):
		rank = len(sorted)
	}
	return sorted[rank-1]
}
