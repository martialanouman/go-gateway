package metrics_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// scrape registers the catalogue on a guarded registry and returns the /metrics body. Registering through
// the guard is the point: the catalogue is subject to its own cardinality rule, not exempt from it.
func scrape(t *testing.T, prime func(*metrics.Catalog)) string {
	t.Helper()
	reg := metrics.Guard(prometheus.NewRegistry())
	cat := metrics.NewCatalog()
	reg.MustRegister(cat.Collectors()...)
	if prime != nil {
		prime(cat)
	}

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

// TestCatalogRegistersAndExposesEveryMetric covers the step's first acceptance criterion. A label vector
// exposes nothing until it has a child, so each one is primed with a plausible bounded value first.
func TestCatalogRegistersAndExposesEveryMetric(t *testing.T) {
	body := scrape(t, func(c *metrics.Catalog) {
		c.IngestDuration.WithLabelValues("rest").Observe(0.01)
		c.MessageE2EDuration.WithLabelValues("00000000-0000-0000-0000-000000000001", "delivered").Observe(1.5)
		c.QueueDepth.WithLabelValues("mt.inbound").Set(12)
		c.SetConnectorBreakerState("00000000-0000-0000-0000-000000000001", metrics.BreakerStateOpen)
		c.BalanceCacheAge.Observe(30)
		c.RoutingScriptFailures.WithLabelValues("js", "timeout").Inc()
	})

	for _, name := range []string{
		"ingest_duration_seconds",
		"message_e2e_duration_seconds",
		"queue_depth",
		"connector_breaker_state",
		"billing_balance_cache_age_seconds",
		"routing_script_failures_total",
	} {
		if !strings.Contains(body, name+"{") && !strings.Contains(body, name+"_bucket{") {
			t.Errorf("%s is not exposed on /metrics", name)
		}
	}
}

// TestSetConnectorBreakerStateIsOneHot: the breaker is an enum, and Prometheus models an enum as one gauge
// per value with exactly one set to 1. Leaving the previous state at 1 would make a dashboard show a
// connector as open and closed at once — worse than no metric.
func TestSetConnectorBreakerStateIsOneHot(t *testing.T) {
	const connector = "00000000-0000-0000-0000-000000000001"
	body := scrape(t, func(c *metrics.Catalog) {
		c.SetConnectorBreakerState(connector, metrics.BreakerStateOpen)
		c.SetConnectorBreakerState(connector, metrics.BreakerStateClosed) // the connector recovers
	})

	for _, want := range []string{
		`connector_breaker_state{connector_id="` + connector + `",state="closed"} 1`,
		`connector_breaker_state{connector_id="` + connector + `",state="open"} 0`,
		`connector_breaker_state{connector_id="` + connector + `",state="half_open"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

// TestSetConnectorBreakerStateClaimsNothingOnAnUnknownState: an unrecognised state must not invent a series.
// Every gauge reads 0, which is visibly wrong on a dashboard (their sum drops to 0) instead of quietly
// mislabelling the connector.
func TestSetConnectorBreakerStateClaimsNothingOnAnUnknownState(t *testing.T) {
	const connector = "00000000-0000-0000-0000-000000000001"
	body := scrape(t, func(c *metrics.Catalog) {
		c.SetConnectorBreakerState(connector, metrics.BreakerStateOpen)
		c.SetConnectorBreakerState(connector, "banana")
	})

	if strings.Contains(body, `state="banana"`) {
		t.Error("an unknown state created a series")
	}
	if !strings.Contains(body, `connector_breaker_state{connector_id="`+connector+`",state="open"} 0`) {
		t.Errorf("the previous state was not cleared:\n%s", body)
	}
}

// TestCatalogHistogramsCarryNoCustomerLabel. customer_id is bounded and allowed, but a histogram multiplies
// its labels by its buckets: one customer_id on a 14-bucket histogram is 14 series per customer. The rule is
// "customer_id on counters and gauges, never on a histogram", and this test is what keeps it true.
func TestCatalogHistogramsCarryNoCustomerLabel(t *testing.T) {
	body := scrape(t, func(c *metrics.Catalog) {
		c.IngestDuration.WithLabelValues("rest").Observe(0.01)
		c.MessageE2EDuration.WithLabelValues("00000000-0000-0000-0000-000000000001", "delivered").Observe(1)
		c.BalanceCacheAge.Observe(1)
	})
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "_bucket{") && strings.Contains(line, "customer_id=") {
			t.Errorf("a histogram carries customer_id: %s", line)
		}
	}
}
