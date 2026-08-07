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

// acceptingSink takes everything, so a record missing from it was refused by the publisher itself.
type acceptingSink struct{ published int }

func (s *acceptingSink) TryPublish(_, _ []byte) { s.published++ }

// TestSessionDropsReachTheExposition is the assertion step-184 lacked, at the layer where the defect lived.
//
// That step metered the transport and the publisher on two independent counters, and only wired the first to
// /metrics. The rate cap then truncated the session feed in silence: a pod drain producing thousands of binds
// looked, to a scraper, exactly like a quiet one. Reading the exposition rather than the publisher is what
// joins the two — observing a drop and exposing it are separate wirings, and this service already learned
// that once (see cmd/connector-pool-svc/opsmetrics_test.go).
func TestSessionDropsReachTheExposition(t *testing.T) {
	t.Parallel()

	const binds = 200 // past the publisher's 50/s cap even if the burst straddles a window boundary

	sink := &acceptingSink{}
	events := metricstream.NewEventPublisher(serviceName, sink)
	transport := &countingTransport{}

	reg := metrics.Guard(prometheus.NewRegistry())
	reg.MustRegister(streamDropCollectors(transport, events)...)

	one := 1
	for range binds {
		events.SessionChanged("acct-1", "ACME01", "bound", &one)
	}

	drops := scrapeDrops(t, reg)
	if drops["rate_cap"] == 0 {
		t.Error("rate_cap = 0 on /metrics after a burst past the cap: a truncated feed reads as a complete one")
	}
	if got := float64(sink.published) + drops["rate_cap"]; got != binds {
		t.Errorf("published %d + exposed rate_cap %v = %v, want %d — an event is unaccounted for",
			sink.published, drops["rate_cap"], got, binds)
	}
	if drops["buffer"] != 0 {
		t.Errorf("buffer = %v, want 0 — a publisher drop must not be attributed to the transport", drops["buffer"])
	}

	transport.n = 4
	if got := scrapeDrops(t, reg)["buffer"]; got != 4 {
		t.Errorf("buffer = %v after four transport drops, want 4", got)
	}
}

var dropLine = regexp.MustCompile(`metrics_stream_dropped_total\{reason="([a-z_]+)"\} ([0-9.e+]+)`)

// scrapeDrops reads what a Prometheus scraper would actually receive, keyed by reason. Going through promhttp
// rather than Gather also proves the exposition is served at all: the guarded registry drops an offending
// family and answers 500.
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
