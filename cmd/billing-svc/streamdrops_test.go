package main

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// countingTransport stands in for the Kafka stream producer, which cannot be built without a broker.
type countingTransport struct{ n int64 }

func (t *countingTransport) Dropped() int64 { return t.n }

type discardSink struct{ published int }

func (s *discardSink) TryPublish(_, _ []byte) { s.published++ }

// TestAlertDropsReachTheExposition ties this service's wiring to what a scraper sees. Registering a collector
// and feeding its source are independent wirings, and step-184 got them wrong in exactly this file.
func TestAlertDropsReachTheExposition(t *testing.T) {
	t.Parallel()

	sink := &discardSink{}
	alerts := metricstream.NewEventPublisher(serviceName, sink)
	transport := &countingTransport{n: 3}

	reg := metrics.Guard(prometheus.NewRegistry())
	reg.MustRegister(streamDropCollectors(transport, alerts)...)

	drops := scrapeDrops(t, reg)
	if got := drops["buffer"]; got != 3 {
		t.Errorf("buffer = %v, want 3 — the exposition does not follow the transport", got)
	}
	if _, ok := drops["encode"]; !ok {
		t.Error("no encode series: an unserializable alert would vanish unrecorded")
	}
	// The cap guards session events only, and this service publishes none. A rate_cap series pinned at zero
	// would read as a guarantee that alerts are throttled — they are not.
	if _, ok := drops["rate_cap"]; ok {
		t.Error("rate_cap exposed on a service that never rate-caps: a permanent zero reads as a promise")
	}
}

// TestAlertsSurviveABurst: no cap applies to alerts, so a burst must reach the sink whole and leave the drop
// counters untouched. It is the compensating half of the assertion above.
func TestAlertsSurviveABurst(t *testing.T) {
	t.Parallel()

	const alerts = 200

	sink := &discardSink{}
	p := metricstream.NewEventPublisher(serviceName, sink)
	for range alerts {
		p.Alerted("cust-1", "customer", "cust-1", "mo_floor_reached", 0)
	}
	if sink.published != alerts {
		t.Errorf("published %d alerts, want all %d", sink.published, alerts)
	}
	if got := p.DroppedRateCapped(); got != 0 {
		t.Errorf("rate-capped = %d, want alerts exempt from the session cap", got)
	}
}

var dropLine = regexp.MustCompile(`metrics_stream_dropped_total\{reason="([a-z_]+)"\} ([0-9.e+]+)`)

// scrapeDrops reads what a Prometheus scraper would actually receive, keyed by reason.
func scrapeDrops(t *testing.T, reg *metrics.Registry) map[string]float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	drops := map[string]float64{}
	for _, m := range dropLine.FindAllStringSubmatch(rec.Body.String(), -1) {
		v, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			t.Fatalf("parse %q: %v", m[2], err)
		}
		drops[m[1]] = v
	}
	return drops
}
