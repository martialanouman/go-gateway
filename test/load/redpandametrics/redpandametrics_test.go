package redpandametrics_test

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/redpandametrics"
)

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return string(b)
}

func parse(t *testing.T, name string, at time.Time) redpandametrics.Snapshot {
	t.Helper()
	snap, err := redpandametrics.Parse(strings.NewReader(fixture(t, name)), at)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return snap
}

// TestNewClientTargetsPublicMetrics is the most valuable test in this package.
//
// Redpanda serves two expositions on port 9644: /metrics carries thousands of internal vectorized_*
// series, /public_metrics the curated redpanda_* ones this reader knows. The two scrapers this package
// was modelled on both default a bare origin to "/metrics" — inherited unchanged, this reader would
// parse a successful 200 and find none of its families, which reads exactly like a broker at rest.
func TestNewClientTargetsPublicMetrics(t *testing.T) {
	c, err := redpandametrics.NewClient("http://127.0.0.1:9644")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if got := c.URL(); got != "http://127.0.0.1:9644/public_metrics" {
		t.Errorf("a bare admin origin must be scraped at /public_metrics, got %s", got)
	}
}

// TestParseReadsLatencyPerAPI: the whole point of D2 is per-API service latency, so the two families
// that carry it have to come back keyed by API and not merged.
func TestParseReadsLatencyPerAPI(t *testing.T) {
	snap := parse(t, "public_metrics.txt", time.Unix(1000, 0))

	produce, ok := snap.Handlers["produce"]
	if !ok {
		t.Fatalf("handler latency must be keyed by API, got keys %v", keys(snap.Handlers))
	}
	if produce.Count != 186369 {
		t.Errorf("produce handler count = %d, want 186369", produce.Count)
	}
	if produce.Sum < 12.6 || produce.Sum > 12.8 {
		t.Errorf("produce handler sum = %v, want ~12.7", produce.Sum)
	}
	if fetch := snap.Handlers["fetch"]; fetch.Count != 86 {
		t.Errorf("fetch handler count = %d, want 86", fetch.Count)
	}

	// The second family is a different question: handler latency is what the client waited for,
	// request latency is what the broker spent internally. Merging them would answer neither.
	if req, ok := snap.Requests["produce"]; !ok || req.Count != 211304 {
		t.Errorf("internal produce latency must be read separately, got %+v", req)
	}

	// The +Inf bucket is rebuilt from the count rather than trusted, as in gatewaymetrics.
	last := produce.Buckets[len(produce.Buckets)-1]
	if !math.IsInf(last.UpperBound, 1) || last.Cumulative != produce.Count {
		t.Errorf("the last bucket must be +Inf at the family's own count, got %+v", last)
	}

	// A family this reader does not know must be skipped in silence.
	if len(snap.Handlers) != 2 {
		t.Errorf("unknown families must not create handlers, got %v", keys(snap.Handlers))
	}
}

// TestParseSumsCPUAcrossShards: Redpanda exposes one series per shard, and a reader that takes the
// last one instead of summing understates the broker's CPU by a factor of the core count — silently,
// and in the direction that makes the broker look innocent.
func TestParseSumsCPUAcrossShards(t *testing.T) {
	snap := parse(t, "public_metrics.txt", time.Unix(1000, 0))

	if len(snap.CPUBusy) != 2 {
		t.Fatalf("both shards must be kept, got %v", snap.CPUBusy)
	}
	if got := snap.CPUBusySeconds(); math.Abs(got-14.0) > 0.01 {
		t.Errorf("CPU across shards = %v, want 9.998203 + 4.001797 = 14", got)
	}
}

// TestParseRefusesANonFiniteReading: NaN and ±Inf are legal in the text format and expfmt decodes them
// as-is. A reading that cannot be trusted must fail, never average into a plausible number.
func TestParseRefusesANonFiniteReading(t *testing.T) {
	_, err := redpandametrics.Parse(strings.NewReader(fixture(t, "nan.txt")), time.Unix(1000, 0))
	if err == nil {
		t.Fatal("a NaN reading must be refused")
	}
	if !strings.Contains(err.Error(), "redpandametrics") {
		t.Errorf("the error must name the reader, got: %v", err)
	}
}

// TestRateDerivesServiceLatencyOverTheWindow pins the arithmetic the attribution rests on: what the
// broker served during the window, not since it booted.
func TestRateDerivesServiceLatencyOverTheWindow(t *testing.T) {
	before := parse(t, "public_metrics.txt", time.Unix(1000, 0))
	after := parse(t, "public_metrics.txt", time.Unix(1010, 0))

	// Same fixture twice: nothing moved, and "nothing moved" must not render as a zero latency.
	rep, err := redpandametrics.Rate(before, after)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	if got := rep.Render(); !strings.Contains(got, "no ") {
		t.Errorf("a window in which the broker served nothing must say so, got: %s", got)
	}
}

// TestRateRefusesAnUnusableWindow: a counter that went backwards means the broker restarted, and a
// window too short makes every derived figure noise. Both must fail rather than be reported.
func TestRateRefusesAnUnusableWindow(t *testing.T) {
	early := parse(t, "public_metrics.txt", time.Unix(1000, 0))
	late := parse(t, "public_metrics.txt", time.Unix(1010, 0))

	if _, err := redpandametrics.Rate(late, early); err == nil {
		t.Error("readings in the wrong order must be refused")
	}

	same := parse(t, "public_metrics.txt", time.Unix(1000, 0))
	if _, err := redpandametrics.Rate(early, same); err == nil {
		t.Error("a zero-length window must be refused")
	}

	// A restart resets the counters: after < before on a cumulative family.
	dropped := late
	dropped.Handlers = map[string]redpandametrics.Latency{"produce": {Count: 1, Sum: 0.001}}
	if _, err := redpandametrics.Rate(early, dropped); err == nil {
		t.Error("a counter that went backwards must be refused: the broker restarted mid-window")
	}
}

// TestRenderNamesWhatItIsAShareOf follows the rule cpuShare established in internal/e2e: a figure that
// does not say what it excludes gets read as a total.
func TestRenderNamesWhatItIsAShareOf(t *testing.T) {
	before := parse(t, "public_metrics.txt", time.Unix(1000, 0))
	after := before
	after.At = time.Unix(1010, 0)
	after.Handlers = map[string]redpandametrics.Latency{
		"produce": {Count: 186369 + 100000, Sum: 12.6993 + 6.0},
	}
	after.CPUBusy = map[string]float64{"0": 14.998203, "1": 6.001797}

	rep, err := redpandametrics.Rate(before, after)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	got := rep.Render()

	if !strings.Contains(got, "produce") {
		t.Errorf("the render must name the API it measured, got: %s", got)
	}
	// 6s of CPU over a 10s window across 2 shards.
	if !strings.Contains(got, "0.70 cores") {
		t.Errorf("7s of broker CPU over a 10s window is 0.70 cores, got: %s", got)
	}
	if !strings.Contains(got, "2 shards") {
		t.Errorf("the core figure is meaningless without the shard count, got: %s", got)
	}
	if !strings.Contains(got, "60 µs") && !strings.Contains(got, "60µs") {
		t.Errorf("6s over 100000 produces is 60µs of mean service time, got: %s", got)
	}
	// The clause this test is named for. Without it the core figure reads as the host's, and the whole
	// reason this reader exists is that the host total was never what anyone had measured.
	if !strings.Contains(got, "NOT counted") {
		t.Errorf("the render must say what it excludes, or it reads as a host total: %s", got)
	}
}

func keys(m map[string]redpandametrics.Latency) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
