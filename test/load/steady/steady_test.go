package steady_test

import (
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/steady"
)

// d2 is the criteria set of step-201 D2, reused by the tests below so a change to the decision shows up
// as a single edit rather than a dozen literals drifting apart.
func d2() steady.Criteria {
	return steady.Criteria{
		MinWindow:            60 * time.Second,
		MinThroughput:        1000,
		SegmentsPerMessage:   1,
		MaxSegmentationDrift: 0.02,
		MaxLagSlopeFraction:  0.01,
		MinLagSamples:        6,
		IngestP99Budget:      250 * time.Millisecond,
		PeerCeiling:          43498,
	}
}

// healthy is a measurement that clears every bar of D2, so each test below can spoil exactly one thing
// and know which check it is reading.
func healthy() steady.Measurement {
	return steady.Measurement{
		Window:        60 * time.Second,
		Accepted:      66000,
		Errors:        0,
		Submitted:     66000,
		IngestP99:     40 * time.Millisecond,
		IngestSamples: 66000,
		Lag:           flatLag(12, 120),
		BreakerClosed: true,
	}
}

// flatLag builds n samples one second apart, all at the same depth.
func flatLag(n int, depth int64) []steady.LagSample {
	base := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	out := make([]steady.LagSample, n)
	for i := range out {
		out[i] = steady.LagSample{At: base.Add(time.Duration(i) * time.Second), Records: depth}
	}
	return out
}

// growingLag builds n samples one second apart whose depth climbs by perSecond records each second.
func growingLag(n int, start, perSecond int64) []steady.LagSample {
	out := flatLag(n, start)
	for i := range out {
		out[i].Records = start + perSecond*int64(i)
	}
	return out
}

// check finds a named check in a verdict, failing the test when the verdict does not carry it.
func check(t *testing.T, v steady.Verdict, name string) steady.Check {
	t.Helper()
	for _, c := range v.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("verdict carries no check named %q, only %v", name, names(v))
	return steady.Check{}
}

func names(v steady.Verdict) []string {
	out := make([]string, 0, len(v.Checks))
	for _, c := range v.Checks {
		out = append(out, c.Name)
	}
	return out
}

// TestHealthyRunPasses: the whole point of the evaluator is that it CAN pass. A criteria set nothing
// clears is as useless as one nothing fails.
func TestHealthyRunPasses(t *testing.T) {
	v := steady.Evaluate(healthy(), d2())
	if !v.Pass() {
		t.Fatalf("Pass() = false, want true\n%s", v)
	}
	if len(v.Checks) == 0 {
		t.Fatal("verdict carries no check at all, so it proved nothing")
	}
}

// TestThroughputBelowThresholdFails is the check the whole step exists for: a run under the spec's
// per-worker lower bound (§2.5) must fail rather than be reported as a number.
func TestThroughputBelowThresholdFails(t *testing.T) {
	m := healthy()
	m.Accepted, m.Submitted, m.IngestSamples = 54000, 54000, 54000 // 900/s

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false at 900 msg/s\n%s", v)
	}
	if c := check(t, v, steady.CheckThroughput); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
	if got := v.AcceptRate; got < 899 || got > 901 {
		t.Errorf("AcceptRate = %v, want ~900", got)
	}
}

// TestOutputBelowAcceptanceFails: the distinction between a throughput and a queue filling up. The
// acceptance rate alone clears the threshold here; the run must still fail.
func TestOutputBelowAcceptanceFails(t *testing.T) {
	m := healthy()
	m.Submitted = 33000 // half of what was accepted came out

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false when output is half the acceptance\n%s", v)
	}
	if c := check(t, v, steady.CheckBalance); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
	if c := check(t, v, steady.CheckThroughput); !c.Pass {
		t.Errorf("%s pass = false, want true: acceptance alone did clear the bar (%s)", c.Name, c.Detail)
	}
}

// TestOutputAboveAcceptanceFailsToo: drift is two-sided. More submit_sm than messages means the run was
// not single-segment, and the throughput figure then counts segments as messages.
func TestOutputAboveAcceptanceFailsToo(t *testing.T) {
	m := healthy()
	m.Submitted = 85800 // 1.3 segments per message

	v := steady.Evaluate(m, d2())
	if c := check(t, v, steady.CheckBalance); c.Pass {
		t.Errorf("%s pass = true, want false at 1.3 segments/message with SegmentsPerMessage=1 (%s)",
			c.Name, c.Detail)
	}
}

// TestSegmentationIsHonoured: a run whose messages ARE multipart is not unbalanced, as long as the
// criteria say so. Without this the drift check would forbid the very case D2 carves out.
func TestSegmentationIsHonoured(t *testing.T) {
	c := d2()
	c.SegmentsPerMessage = 1.3

	m := healthy()
	m.Submitted = 85800

	v := steady.Evaluate(m, c)
	if !v.Pass() {
		t.Fatalf("Pass() = false, want true for a declared 1.3 segments/message run\n%s", v)
	}
}

// TestGrowingLagFails: a backlog climbing at 1% of the accept rate is a queue filling, whatever the
// other counters say.
func TestGrowingLagFails(t *testing.T) {
	m := healthy()
	m.Lag = growingLag(12, 100, 50) // 50 rec/s against an accept rate of 1100/s

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false on a lag climbing 50 rec/s\n%s", v)
	}
	if c := check(t, v, steady.CheckLag); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
	if v.LagSlope < 49 || v.LagSlope > 51 {
		t.Errorf("LagSlope = %v, want ~50", v.LagSlope)
	}
}

// TestDrainingLagPasses: a backlog going DOWN is not a run failing. Only growth is the signal.
func TestDrainingLagPasses(t *testing.T) {
	m := healthy()
	m.Lag = growingLag(12, 5000, -100)

	v := steady.Evaluate(m, d2())
	if c := check(t, v, steady.CheckLag); !c.Pass {
		t.Errorf("%s pass = false, want true on a draining backlog (%s)", c.Name, c.Detail)
	}
}

// TestTooFewLagSamplesFails: absence of evidence must not read as a flat lag. A run that polled the
// backlog twice cannot tell flat from climbing, and the verdict has to say so.
func TestTooFewLagSamplesFails(t *testing.T) {
	m := healthy()
	m.Lag = flatLag(2, 120)

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false on 2 lag samples\n%s", v)
	}
	if c := check(t, v, steady.CheckLag); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
}

// TestIngestP99OverBudgetFails covers the §1.2 ingestion budget.
func TestIngestP99OverBudgetFails(t *testing.T) {
	m := healthy()
	m.IngestP99 = 300 * time.Millisecond

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false at a 300ms ingest p99\n%s", v)
	}
	if c := check(t, v, steady.CheckIngestP99); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
}

// TestNoIngestSamplesFails: a p99 of zero over zero samples is the shape of a measurement that never
// happened, and it must never read as the fastest run ever recorded.
func TestNoIngestSamplesFails(t *testing.T) {
	m := healthy()
	m.IngestP99, m.IngestSamples = 0, 0

	v := steady.Evaluate(m, d2())
	if c := check(t, v, steady.CheckIngestP99); c.Pass {
		t.Errorf("%s pass = true, want false with no observation at all (%s)", c.Name, c.Detail)
	}
}

// TestAnyErrorFails: D2 says zero, and one is not zero.
func TestAnyErrorFails(t *testing.T) {
	m := healthy()
	m.Errors = 1

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false with one error\n%s", v)
	}
	if c := check(t, v, steady.CheckErrors); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
}

// TestSubmitRejectionFails: an error on the SMSC leg counts too. Only counting the HTTP leg would let a
// run pass while every message was refused downstream.
func TestSubmitRejectionFails(t *testing.T) {
	m := healthy()
	m.SubmitRejected = 3

	v := steady.Evaluate(m, d2())
	if c := check(t, v, steady.CheckErrors); c.Pass {
		t.Errorf("%s pass = true, want false with 3 submit_sm refused (%s)", c.Name, c.Detail)
	}
}

// TestOpenBreakerFails covers the last clause of D2.
func TestOpenBreakerFails(t *testing.T) {
	m := healthy()
	m.BreakerClosed = false

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false with the breaker not closed\n%s", v)
	}
	if c := check(t, v, steady.CheckBreaker); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
}

// TestRunAtThePeerCeilingFails: a run at the peer's own limit measures the peer. D3 exists so this
// cannot be published as a gateway figure.
func TestRunAtThePeerCeilingFails(t *testing.T) {
	c := d2()
	c.PeerCeiling = 1000 // a peer that can barely take what the run pushes

	v := steady.Evaluate(healthy(), c)
	if v.Pass() {
		t.Fatalf("Pass() = true, want false for a run at the peer's ceiling\n%s", v)
	}
	if ch := check(t, v, steady.CheckPeerCeiling); ch.Pass {
		t.Errorf("%s pass = true, want false (%s)", ch.Name, ch.Detail)
	}
}

// TestUnknownPeerCeilingFails: D2 places the run UNDER a ceiling, so a run with no ceiling to compare
// against has not met the criterion — it has skipped it.
func TestUnknownPeerCeilingFails(t *testing.T) {
	c := d2()
	c.PeerCeiling = 0

	v := steady.Evaluate(healthy(), c)
	ch := check(t, v, steady.CheckPeerCeiling)
	if ch.Pass {
		t.Errorf("%s pass = true, want false with no measured ceiling (%s)", ch.Name, ch.Detail)
	}
	// The diagnosis, not only the verdict: comparing against a ceiling of zero happens to fail too, but
	// it reports "0.0% of a 0/s ceiling" — an arithmetic artefact where the reader needs to be told the
	// figure was never measured.
	if !strings.Contains(ch.Detail, "no peer ceiling was measured") {
		t.Errorf("%s detail = %q, want it to name the missing D3 measurement", ch.Name, ch.Detail)
	}
	if strings.Contains(ch.Detail, "Inf") || strings.Contains(ch.Detail, "NaN") {
		t.Errorf("%s detail = %q, want no artefact of dividing by a zero ceiling", ch.Name, ch.Detail)
	}
}

// TestShortWindowFails: D2 requires a tier of at least 60s, and a 10s burst is not one.
func TestShortWindowFails(t *testing.T) {
	m := healthy()
	m.Window = 10 * time.Second
	m.Accepted, m.Submitted, m.IngestSamples = 11000, 11000, 11000 // still 1100/s

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false over a 10s window\n%s", v)
	}
	if c := check(t, v, steady.CheckWindow); c.Pass {
		t.Errorf("%s pass = true, want false (%s)", c.Name, c.Detail)
	}
}

// TestZeroWindowDoesNotDivide: a zero window is the shape of a run that never started. It must not
// produce an infinite rate that then clears every throughput bar.
func TestZeroWindowDoesNotDivide(t *testing.T) {
	m := healthy()
	m.Window = 0

	v := steady.Evaluate(m, d2())
	if v.Pass() {
		t.Fatalf("Pass() = true, want false over a zero window\n%s", v)
	}
	if v.AcceptRate != 0 {
		t.Errorf("AcceptRate = %v, want 0 rather than a division by a zero window", v.AcceptRate)
	}
}

// TestStringReportsEveryCheck: the report is what a reader gets, so every check has to be in it, pass
// or fail, with its own figures rather than a bare verdict.
func TestStringReportsEveryCheck(t *testing.T) {
	v := steady.Evaluate(healthy(), d2())
	report := v.String()
	for _, c := range v.Checks {
		if !strings.Contains(report, c.Name) {
			t.Errorf("report omits check %q:\n%s", c.Name, report)
		}
	}
}

func TestPercentile(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }

	tests := []struct {
		name string
		in   []time.Duration
		q    float64
		want time.Duration
	}{
		{"empty is zero", nil, 0.99, 0},
		{"single sample", []time.Duration{ms(7)}, 0.99, ms(7)},
		// Nearest rank, Prometheus's own convention: the smallest sample at or below which 99% of them
		// sit. Of 1…100 ms that is 99 ms, not the maximum — a p99 equal to the worst sample would be a
		// maximum wearing a percentile's name.
		{"p99 of a hundred", seq(100), 0.99, ms(99)},
		{"p50 of a hundred", seq(100), 0.5, ms(50)},
		{"unsorted input", []time.Duration{ms(9), ms(1), ms(5)}, 0.5, ms(5)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := steady.Percentile(tc.in, tc.q); got != tc.want {
				t.Errorf("Percentile(%v) = %v, want %v", tc.q, got, tc.want)
			}
		})
	}
}

// TestPercentileDoesNotReorderTheCaller: the samples belong to the caller, which may still want them in
// arrival order for a second reading.
func TestPercentileDoesNotReorderTheCaller(t *testing.T) {
	in := []time.Duration{3, 1, 2}
	_ = steady.Percentile(in, 0.5)
	if in[0] != 3 || in[1] != 1 || in[2] != 2 {
		t.Errorf("input reordered to %v, want [3 1 2]", in)
	}
}

// seq returns n durations of 1ms … n ms.
func seq(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i+1) * time.Millisecond
	}
	return out
}
