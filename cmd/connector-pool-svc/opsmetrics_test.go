package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// TestOpsExposesTheMetricsThisServiceFeeds closes the second half of a dead guard.
//
// Observing a metric and REGISTERING it are two independent wirings, and this service deliberately
// registers a hand-picked subset of the catalogue rather than Collectors() wholesale. A metric fed by
// connector-pool but absent from that subset is observed into a collector nobody scrapes: perfectly
// live in memory, invisible on /metrics, and indistinguishable from a healthy silence on a dashboard.
//
// The check is an exposition, not a list comparison: it asks what a scraper would actually receive.
func TestOpsExposesTheMetricsThisServiceFeeds(t *testing.T) {
	t.Parallel()

	reg := metrics.Guard(prometheus.NewRegistry())
	catalog := metrics.NewCatalog()
	reg.MustRegister(poolCatalogueCollectors(catalog)...)

	// A label vector exposes nothing until it has a child, so each one is fed a bounded value first —
	// the same values the pool's own call sites use.
	const connector = "00000000-0000-0000-0000-000000000001"
	catalog.SetConnectorBreakerState(connector, metrics.BreakerStateClosed)
	catalog.QueueDepth.WithLabelValues("mt.routed").Set(0)
	catalog.SubmitsTotal.WithLabelValues(connector, "ok").Inc()
	catalog.SubmitRejectedTotal.WithLabelValues(connector, "submit_failed").Inc()
	catalog.MessageE2EDuration.WithLabelValues(connector, "ok").Observe(0.05)

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()

	for _, name := range []string{
		"connector_breaker_state",
		"queue_depth_records",
		"submits_total",
		"submit_rejected_total",
		// The NFR budget (spec §1.2) is read off this one by test/load/gatewaymetrics. Unregistered, it
		// reports nothing and a load run would score a budget it never measured.
		"message_e2e_duration_seconds",
	} {
		if !strings.Contains(body, name+"{") && !strings.Contains(body, name+"_bucket{") {
			t.Errorf("%s is fed by connector-pool but not exposed on /metrics", name)
		}
	}
}
