package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Breaker states, the closed vocabulary of the connector_breaker_state gauge. They mirror the local breaker
// machine (§6.13, step-121); adding one here without adding it there would silently under-report.
const (
	BreakerStateClosed   = "closed"
	BreakerStateOpen     = "open"
	BreakerStateHalfOpen = "half_open"
)

// breakerStates is the enumeration the one-hot gauge iterates. Order is only cosmetic.
var breakerStates = []string{BreakerStateClosed, BreakerStateOpen, BreakerStateHalfOpen}

// Catalog is the gateway's metric catalogue: every cross-service metric declared once, with its labels, its
// buckets and the reason it exists.
//
// One place rather than eight main.go files, because a metric's cost is not local — a name chosen twice with
// different labels is unqueryable, and a bucket set chosen carelessly is paid on every scrape forever.
//
// Emission belongs to the services (step-182). Construct with [NewCatalog], register [Catalog.Collectors] on
// the ops registry, and hand the fields to the components that observe.
//
// The registry guard bounds label NAMES; bounding their VALUES is the emitter's job, and step-182 owes it for
// every label below. The rule: a label value comes from a constant or from a mapping that buckets the
// unknown — never from an error string, a config field or anything a submission influences. One
// WithLabelValues(runtime, err.Error()) is a cardinality incident with a perfectly legal label name.
// routing.script.Reason is the shape to copy: four constants and "runtime_error" for everything else.
type Catalog struct {
	// IngestDuration measures acceptance: the submission arriving until the durable Kafka ACK — the moment
	// the gateway owns the message and may answer the client. It is the latency an ESME actually feels, and
	// it excludes everything downstream on purpose.
	IngestDuration *prometheus.HistogramVec

	// MessageE2EDuration measures acceptance until the final outcome from the SMSC. It spans the queue, so
	// it is the backlog indicator; its buckets reach minutes for that reason.
	MessageE2EDuration *prometheus.HistogramVec

	// QueueDepth is the lag of a Kafka topic, sampled by whoever consumes it. Rising depth with flat
	// ingestion is the signature of a slow connector.
	//
	// step-182 must give each topic ONE owner: several services consuming the same topic and all reporting
	// their own lag would double-count without any of them being wrong.
	QueueDepth *prometheus.GaugeVec

	// ConnectorBreakerState is a one-hot enum gauge: one series per (connector, state), exactly one at 1.
	// Set it through [Catalog.SetConnectorBreakerState], never directly, or stale states stay at 1.
	ConnectorBreakerState *prometheus.GaugeVec

	// BalanceCacheAge is the age of the balance cache entry a reserve actually read. It answers "how stale
	// can a credit decision be?", which the TTL alone does not: the TTL bounds it in theory, an expiry storm
	// or a stuck refresher shows up here.
	BalanceCacheAge prometheus.Histogram

	// RoutingScriptFailures counts routing scripts that fell back to declarative resolution, by runtime and
	// reason — "timeout" being the wall-clock trip, the guard that actually protects the pod (§6.12). The
	// catalogue adopts the metric router-svc already exposed rather than adding a second, near-duplicate
	// timeout counter: two names for one event is how a dashboard ends up disagreeing with itself.
	RoutingScriptFailures *prometheus.CounterVec
}

// Native histogram settings, applied to every histogram in the catalogue. A factor of 1.1 gives ~10%
// resolution at any magnitude, which classic buckets cannot without exploding in count; the bucket ceiling
// and the reset duration bound the memory a pathological distribution can cost. Classic buckets are declared
// alongside so a scraper that does not negotiate native histograms still gets usable quantiles.
const (
	nativeBucketFactor     = 1.1
	nativeMaxBucketNumber  = 160
	nativeMinResetDuration = time.Hour
)

// NewCatalog builds the catalogue. It registers nothing: the caller decides which registry owns it.
func NewCatalog() *Catalog {
	return &Catalog{
		IngestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_duration_seconds",
			Help: "Time from submission to the durable Kafka ACK, by ingress protocol.",
			// 1 ms … ~4 s: acceptance is a handful of milliseconds when healthy, and anything past a few
			// seconds is already a client timeout.
			Buckets:                         prometheus.ExponentialBuckets(0.001, 2, 13),
			NativeHistogramBucketFactor:     nativeBucketFactor,
			NativeHistogramMaxBucketNumber:  nativeMaxBucketNumber,
			NativeHistogramMinResetDuration: nativeMinResetDuration,
		}, []string{"source"}),

		MessageE2EDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "message_e2e_duration_seconds",
			Help: "Time from submission to the final SMSC outcome, by connector and status.",
			// 10 ms … ~5 min: a healthy send is sub-second, but a queue draining after an incident is
			// measured in minutes and must stay visible instead of piling into +Inf.
			Buckets:                         prometheus.ExponentialBuckets(0.01, 2, 16),
			NativeHistogramBucketFactor:     nativeBucketFactor,
			NativeHistogramMaxBucketNumber:  nativeMaxBucketNumber,
			NativeHistogramMinResetDuration: nativeMinResetDuration,
		}, []string{"connector_id", "status"}),

		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth_records",
			Help: "Consumer lag of a Kafka topic, in records.",
		}, []string{"queue"}),

		ConnectorBreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "connector_breaker_state",
			Help: "Connector circuit-breaker state: 1 for the current state, 0 for the others.",
		}, []string{"connector_id", "state"}),

		BalanceCacheAge: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "billing_balance_cache_age_seconds",
			Help: "Age of the balance cache entry read by a credit reserve.",
			// 100 ms … ~27 min: a healthy read is sub-second, so starting at 1 s would pile every normal
			// case into the first bucket, and the range still covers the 10-minute TTL — an entry
			// outliving it stays visible instead of vanishing into +Inf.
			Buckets:                         prometheus.ExponentialBuckets(0.1, 2, 15),
			NativeHistogramBucketFactor:     nativeBucketFactor,
			NativeHistogramMaxBucketNumber:  nativeMaxBucketNumber,
			NativeHistogramMinResetDuration: nativeMinResetDuration,
		}),

		RoutingScriptFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "routing_script_failures_total",
			Help: "Routing scripts that fell back to declarative resolution, by runtime and reason.",
		}, []string{"runtime", "reason"}),
	}
}

// Collectors returns every collector to register, in one call.
func (c *Catalog) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		c.IngestDuration,
		c.MessageE2EDuration,
		c.QueueDepth,
		c.ConnectorBreakerState,
		c.BalanceCacheAge,
		c.RoutingScriptFailures,
	}
}

// SetConnectorBreakerState records a connector's breaker state as a one-hot set of gauges: the given state
// reads 1, every other state reads 0.
//
// An unrecognised state sets every gauge to 0 rather than creating a series for it. That keeps the label
// bounded whatever a caller passes, and the anomaly is visible — the states of a connector no longer sum to
// 1 — instead of silently mislabelling it.
func (c *Catalog) SetConnectorBreakerState(connectorID, state string) {
	// Clear first, set last. A scrape landing mid-update must never see two states at 1 — a connector shown
	// as open AND closed is worse than no metric at all. The transient "all zero" it can see instead is
	// unambiguous, and it lasts one gauge write.
	var current prometheus.Gauge
	for _, known := range breakerStates {
		gauge := c.ConnectorBreakerState.WithLabelValues(connectorID, known)
		if known == state {
			current = gauge
			continue
		}
		gauge.Set(0)
	}
	if current != nil {
		current.Set(1)
	}
}
