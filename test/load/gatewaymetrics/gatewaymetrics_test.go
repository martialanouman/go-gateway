package gatewaymetrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/test/load/gatewaymetrics"
)

const connectorA = "11111111-1111-1111-1111-111111111111"

// exposition renders the gateway's OWN catalogue after prime has fed it, through the same guarded
// registry and the same promhttp handler a service serves /metrics with.
//
// The reader is deliberately not tested against a hand-written fixture: a hand-written one asserts what
// the author believes the gateway emits, and this whole unit exists because a belief about a metric went
// unchecked for a whole milestone.
func exposition(t *testing.T, prime func(*metrics.Catalog)) string {
	t.Helper()
	reg := metrics.Guard(prometheus.NewRegistry())
	cat := metrics.NewCatalog()
	reg.MustRegister(cat.Collectors()...)
	prime(cat)

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want %d", rec.Code, http.StatusOK)
	}
	return rec.Body.String()
}

// observe primes the e2e histogram with one series' worth of latencies, in seconds.
func observe(connectorID, status string, seconds ...float64) func(*metrics.Catalog) {
	return func(c *metrics.Catalog) {
		h := c.MessageE2EDuration.WithLabelValues(connectorID, status)
		for _, s := range seconds {
			h.Observe(s)
		}
	}
}

// parse renders and reads back in one step.
func parse(t *testing.T, prime func(*metrics.Catalog)) gatewaymetrics.Snapshot {
	t.Helper()
	at := time.Now()
	snap, err := gatewaymetrics.Parse(strings.NewReader(exposition(t, prime)), at)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !snap.At.Equal(at) {
		t.Errorf("At = %v, want %v", snap.At, at)
	}
	return snap
}

// repeat builds n identical observations.
func repeat(n int, v float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func TestParseReadsTheGatewaysOwnExposition(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", 0.05, 0.05, 3))

	key := gatewaymetrics.Key{ConnectorID: connectorA, Status: "ok"}
	h, ok := snap.Series[key]
	if !ok {
		t.Fatalf("series %+v missing; got %+v", key, snap.Keys())
	}
	if h.Count != 3 {
		t.Errorf("Count = %v, want %v", h.Count, 3)
	}
	if h.Sum != 3.1 {
		t.Errorf("Sum = %v, want %v", h.Sum, 3.1)
	}
	// The overflow bucket must be present and cumulative to the full count, whether or not the
	// exposition spells it out: every quantile above the last finite bound depends on it.
	last := h.Buckets[len(h.Buckets)-1]
	if !last.Unbounded() || last.Cumulative != 3 {
		t.Errorf("last bucket = %+v, want the +Inf bucket cumulative to 3", last)
	}
	for i := 1; i < len(h.Buckets); i++ {
		if h.Buckets[i].UpperBound <= h.Buckets[i-1].UpperBound {
			t.Fatalf("buckets are not ascending at %d: %+v", i, h.Buckets)
		}
		if h.Buckets[i].Cumulative < h.Buckets[i-1].Cumulative {
			t.Fatalf("buckets are not cumulative at %d: %+v", i, h.Buckets)
		}
	}
}

func TestSnapshotSeparatesConnectorsAndStatuses(t *testing.T) {
	const connectorB = "22222222-2222-2222-2222-222222222222"
	snap := parse(t, func(c *metrics.Catalog) {
		observe(connectorA, "ok", 0.05)(c)
		observe(connectorA, "rejected", 0.05, 0.05)(c)
		observe(connectorB, "ok", 0.05, 0.05, 0.05)(c)
	})

	if got := len(snap.Series); got != 3 {
		t.Fatalf("series = %v, want %v (%+v)", got, 3, snap.Keys())
	}
	if got := snap.Total().Count; got != 6 {
		t.Errorf("Total().Count = %v, want %v", got, 6)
	}
	// A budget is normally read over every attempt, but a run against one connector must be able to
	// exclude a neighbour's traffic — the same reason smscmetrics grew Select.
	if got := snap.Where(func(k gatewaymetrics.Key) bool { return k.ConnectorID == connectorA }).Count; got != 3 {
		t.Errorf("connector A count = %v, want %v", got, 3)
	}
	if got := snap.Where(func(k gatewaymetrics.Key) bool { return k.Status == "ok" }).Count; got != 4 {
		t.Errorf("status ok count = %v, want %v", got, 4)
	}
}

// TestCheckBudgetPasses: 100 sub-second sends. The p99 lands in a bucket whose UPPER bound is already
// below the budget, so the verdict is proven, not inferred.
func TestCheckBudgetPasses(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", repeat(100, 0.05)...))

	v, q, err := snap.Total().CheckBudget(0.99, 2*time.Second)
	if err != nil {
		t.Fatalf("CheckBudget: %v", err)
	}
	if v != gatewaymetrics.Pass {
		t.Errorf("verdict = %v, want %v (quantile %v)", v, gatewaymetrics.Pass, q)
	}
	if q.Upper > 2*time.Second {
		t.Errorf("q.Upper = %v, want at most the 2s budget", q.Upper)
	}
}

// TestCheckBudgetFails: every send took ten seconds. The p99's bucket LOWER bound is already above the
// budget, so the failure is proven too.
func TestCheckBudgetFails(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", repeat(100, 10)...))

	v, q, err := snap.Total().CheckBudget(0.99, 2*time.Second)
	if err != nil {
		t.Fatalf("CheckBudget: %v", err)
	}
	if v != gatewaymetrics.Fail {
		t.Errorf("verdict = %v, want %v (quantile %v)", v, gatewaymetrics.Fail, q)
	}
	if q.Lower < 2*time.Second {
		t.Errorf("q.Lower = %v, want at least the 2s budget", q.Lower)
	}
}

// TestCheckBudgetIsIndeterminateWhenTheBucketStraddlesIt is the honesty requirement. A histogram gives
// bucket bounds, not a value: when the budget falls strictly inside the bucket the quantile lands in,
// no reading of the exposition can decide, and the reader must say so rather than pick a side. An
// interpolated "p99 = 1.7s" here would be a number nobody measured.
func TestCheckBudgetIsIndeterminateWhenTheBucketStraddlesIt(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", repeat(100, 0.05)...))

	// 0.05 s sits in a bucket that spans the 60 ms asked for here.
	v, q, err := snap.Total().CheckBudget(0.99, 60*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckBudget: %v", err)
	}
	if v != gatewaymetrics.Indeterminate {
		t.Errorf("verdict = %v, want %v (quantile %v)", v, gatewaymetrics.Indeterminate, q)
	}
	if q.Lower >= 60*time.Millisecond || q.Upper <= 60*time.Millisecond {
		t.Errorf("quantile bounds = (%v, %v], want them to straddle 60ms", q.Lower, q.Upper)
	}
}

// TestTheSpecBudgetsAreDecidableAgainstTheRealCatalogue is what makes the reader useful rather than
// merely correct. A text exposition resolves a quantile only to a bucket, so "p99 < 2 s" has an answer
// ONLY if 2 s is itself a bucket edge — otherwise the budget falls strictly inside a bucket and every
// run, fast or slow, reports Indeterminate.
//
// The two values checked here are the spec's own (§1.2), not thresholds invented for a test, and they
// are checked against the catalogue the gateway really serves. It is a property of the histogram's
// bucket layout, so it belongs to whoever changes those buckets.
func TestTheSpecBudgetsAreDecidableAgainstTheRealCatalogue(t *testing.T) {
	for _, tc := range []struct {
		name    string
		q       float64
		budget  time.Duration
		latency float64
		want    gatewaymetrics.Verdict
	}{
		{"p99 just under 2s", 0.99, 2 * time.Second, 1.9, gatewaymetrics.Pass},
		{"p99 just over 2s", 0.99, 2 * time.Second, 2.1, gatewaymetrics.Fail},
		{"p50 just under 400ms", 0.5, 400 * time.Millisecond, 0.35, gatewaymetrics.Pass},
		{"p50 just over 400ms", 0.5, 400 * time.Millisecond, 0.45, gatewaymetrics.Fail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := parse(t, observe(connectorA, "ok", repeat(100, tc.latency)...))
			v, q, err := snap.Total().CheckBudget(tc.q, tc.budget)
			if err != nil {
				t.Fatalf("CheckBudget: %v", err)
			}
			if v != tc.want {
				t.Errorf("verdict = %v, want %v — %v is not resolved by the catalogue's buckets (%v)",
					v, tc.want, tc.budget, q)
			}
		})
	}
}

// TestCheckBudgetRefusesAnEmptyHistogram is the reason this unit exists. A metric nobody feeds exposes
// zero observations, and "zero observations" must never be reported as a budget met — that is precisely
// the dashboard reading "no problem" off a dead instrument.
func TestCheckBudgetRefusesAnEmptyHistogram(t *testing.T) {
	var empty gatewaymetrics.Histogram

	v, _, err := empty.CheckBudget(0.99, 2*time.Second)
	if err == nil {
		t.Fatalf("CheckBudget on an empty histogram = %v, nil; want an error", v)
	}
	if v == gatewaymetrics.Pass {
		t.Errorf("verdict = %v, want anything but a pass", v)
	}
	if !strings.Contains(err.Error(), "no observation") {
		t.Errorf("error = %q, want it to name the missing observations", err)
	}
}

// TestCheckBudgetRefusesAQuantileOutOfRange: q must be a proper quantile, or the "first bucket at or
// above q*count" search silently returns the first bucket and reports a flattering pass.
func TestCheckBudgetRefusesAQuantileOutOfRange(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", repeat(10, 0.05)...))

	for _, q := range []float64{0, 1, -0.5, 1.5} {
		if _, _, err := snap.Total().CheckBudget(q, 2*time.Second); err == nil {
			t.Errorf("CheckBudget(%v) = nil error, want a rejection", q)
		}
	}
}

// TestQuantileLandingInTheOverflowBucketFails: an observation past the top finite bound has no upper
// bound at all. It cannot pass any budget, and the reader must not treat "unbounded" as "unknown".
func TestQuantileLandingInTheOverflowBucketFails(t *testing.T) {
	snap := parse(t, observe(connectorA, "ok", repeat(100, 3600)...))

	v, q, err := snap.Total().CheckBudget(0.99, 2*time.Second)
	if err != nil {
		t.Fatalf("CheckBudget: %v", err)
	}
	if !q.Unbounded {
		t.Errorf("q.Unbounded = %v, want %v", q.Unbounded, true)
	}
	if v != gatewaymetrics.Fail {
		t.Errorf("verdict = %v, want %v", v, gatewaymetrics.Fail)
	}
}

// TestSubWindowsTheHistogram: a load run cares about the traffic it injected, not about whatever the
// pod did before it started. Cumulative buckets subtract bucket-wise.
func TestSubWindowsTheHistogram(t *testing.T) {
	before := parse(t, observe(connectorA, "ok", repeat(50, 10)...)).Total()
	after := parse(t, func(c *metrics.Catalog) {
		observe(connectorA, "ok", repeat(50, 10)...)(c)
		observe(connectorA, "ok", repeat(100, 0.05)...)(c)
	}).Total()

	win, err := gatewaymetrics.Sub(before, after)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if win.Count != 100 {
		t.Fatalf("windowed count = %v, want %v", win.Count, 100)
	}
	// Only the fast sends fall in the window, so the budget passes even though the lifetime histogram
	// is dominated by 10-second ones.
	v, _, err := win.CheckBudget(0.99, 2*time.Second)
	if err != nil {
		t.Fatalf("CheckBudget: %v", err)
	}
	if v != gatewaymetrics.Pass {
		t.Errorf("windowed verdict = %v, want %v", v, gatewaymetrics.Pass)
	}
	if lifetime, _, _ := after.CheckBudget(0.99, 2*time.Second); lifetime != gatewaymetrics.Fail {
		t.Errorf("lifetime verdict = %v, want %v — otherwise this test proves nothing about windowing", lifetime, gatewaymetrics.Fail)
	}
}

// TestSubDetectsACounterReset: a restarted pod serves smaller counters than before, and a negative
// delta is a discontinuity, not a fast run.
func TestSubDetectsACounterReset(t *testing.T) {
	small := parse(t, observe(connectorA, "ok", repeat(10, 0.05)...)).Total()
	big := parse(t, observe(connectorA, "ok", repeat(100, 0.05)...)).Total()

	if _, err := gatewaymetrics.Sub(big, small); err == nil {
		t.Fatal("Sub(big, small) = nil error, want a counter-reset rejection")
	}
}

func TestScrapeReadsAnEndpoint(t *testing.T) {
	body := exposition(t, observe(connectorA, "ok", repeat(100, 0.05)...))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	// A bare origin gets /metrics appended, so a recopied base URL is not a 404 blamed on the gateway.
	c, err := gatewaymetrics.NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	snap, err := c.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got := snap.Total().Count; got != 100 {
		t.Errorf("scraped count = %v, want %v", got, 100)
	}
	if snap.At.IsZero() {
		t.Error("At is zero, want the instant of the reading")
	}
}

func TestScrapeRejectsANonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, err := gatewaymetrics.NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := c.Scrape(context.Background()); err == nil {
		t.Fatal("Scrape against a 500 = nil error, want a rejection")
	}
}

// TestClientNeverEchoesCredentials: the harness logs the endpoint it scrapes, and a scrape password in
// a CI log is a leak whoever reads the log did not ask for.
func TestClientNeverEchoesCredentials(t *testing.T) {
	c, err := gatewaymetrics.NewClient("https://scrape:hunter2@gw.internal:9100")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if strings.Contains(c.URL(), "hunter2") {
		t.Errorf("URL() = %q, want the password masked", c.URL())
	}
	_, err = c.Scrape(context.Background())
	if err == nil {
		t.Fatal("Scrape against a nonexistent host = nil error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error = %q, want the password masked", err)
	}
}

// TestParseSaysSoWhenTheMetricIsAbsent: an exposition that carries no e2e histogram at all is the
// symptom this unit was written to kill. It must not decode to a healthy empty snapshot that then
// reads as a pass — Parse succeeds (other metrics are legitimately absent too) and the emptiness is
// caught by CheckBudget, which is asserted here end to end.
func TestParseSaysSoWhenTheMetricIsAbsent(t *testing.T) {
	snap, err := gatewaymetrics.Parse(strings.NewReader("# TYPE up gauge\nup 1\n"), time.Now())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(snap.Series) != 0 {
		t.Errorf("Series = %+v, want none", snap.Series)
	}
	if _, _, err := snap.Total().CheckBudget(0.99, 2*time.Second); err == nil {
		t.Fatal("a missing metric read as a decidable budget; want an error")
	}
}
