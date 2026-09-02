package metrics

import (
	"sort"
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

	// MessageE2EDuration measures acceptance until the SMSC's terminal answer to a submit_sm — the span
	// the NFR budgets (spec §1.2: p50 < 400 ms, p99 < 2 s). It spans the queue, so it is also the backlog
	// indicator; its buckets reach minutes for that reason.
	//
	// Observed by connector-pool, once per SEGMENT and only on a terminal outcome, with the same labels
	// and the same values as SubmitsTotal — so _count and that counter are directly comparable, and a
	// multipart message contributes one sample per submit_sm rather than one per message.
	//
	// What it deliberately leaves out is the whole reason a p99 read off it means anything. The NFR
	// excludes deliberate backpressure and dead-lettering (§6.7), so a throttled submit (redelivered) and
	// a message parked on the max-age SLA (never sent) are not observed. A replayed message is timed from
	// its replay, not from its immutable accept time hours earlier, which would otherwise land every
	// drained message past the top bucket. The clock stops on the submit_sm_resp: the billing settle and
	// the CDR write that follow are our bookkeeping, not delivery latency.
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

	// MessagesTotal counts messages leaving the MT pipeline, by outcome. It is the rate a dashboard shows
	// and the denominator of every reject ratio.
	MessagesTotal *prometheus.CounterVec

	// RejectedTotal breaks the rejections down by flat error code (§11.3) — the same vocabulary the CDR,
	// the HTTP status and the SMPP command_status use, so one reject can be followed across all four.
	RejectedTotal *prometheus.CounterVec

	// PipelineDuration is the time the MT pipeline itself takes, excluding queueing. It separates "we are
	// slow" from "we are behind", which MessageE2EDuration alone cannot.
	PipelineDuration prometheus.Histogram

	// SubmitsTotal counts SMSC submissions by connector and outcome; SubmitRejectedTotal breaks the
	// rejections down by code, mirroring the MessagesTotal/RejectedTotal pair so both legs of a message
	// read the same way.
	SubmitsTotal        *prometheus.CounterVec
	SubmitRejectedTotal *prometheus.CounterVec

	// RoutingScriptFailures counts routing scripts that fell back to declarative resolution, by runtime and
	// reason — "timeout" being the wall-clock trip, the guard that actually protects the pod (§6.12). The
	// catalogue adopts the metric router-svc already exposed rather than adding a second, near-duplicate
	// timeout counter: two names for one event is how a dashboard ends up disagreeing with itself.
	RoutingScriptFailures *prometheus.CounterVec

	// ExactRouteLookups counts L0 exact-number lookups by outcome, exactly one per resolution, failures
	// included. Since step-250e the key is a read-through cache, so this is what tells a stale-Bloom
	// false positive (pg_miss, a Postgres round trip resolving nothing) from a real cold hit — the two
	// follow-ups that step defers, a negative cache and the TTL, are not decidable without it.
	//
	// ExactRouteCacheCorrupt counts undecodable cached values: a cache-leg anomaly, kept off the outcome
	// label so it cannot inflate those ratios.
	ExactRouteLookups      *prometheus.CounterVec
	ExactRouteCacheCorrupt prometheus.Counter
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

// e2eLatencyBudgets are the end-to-end latency thresholds the spec states (§1.2): p50 < 400 ms and
// p99 < 2 s, submission to SMSC delivery attempt. They are spec figures, not tuning.
var e2eLatencyBudgets = []float64{0.4, 2}

// e2eBuckets is the exponential spine 10 ms … ~5 min, with the two spec thresholds spliced in.
//
// The range is the backlog requirement: a healthy send is sub-second, but a queue draining after an
// incident is measured in minutes and must stay visible instead of piling into +Inf.
//
// The two extra edges are what make the budgets answerable IN THE ZONE WHERE THE VERDICT MATTERS. A
// classic histogram resolves a quantile only to the bucket it falls in, and the spine's edges around
// 2 s are 1.28 and 2.56. A comfortably fast p99 — say 80 ms, in (0.064, 0.128] — was always decidable;
// what was not is a p99 anywhere in (1.28, 2.56], i.e. exactly the range where a run is close enough
// to the budget for the answer to be worth having. Same for 400 ms and its (0.32, 0.64] straddle.
// Two extra series per (connector, status) is a trivial price for that.
//
// This buys the TEXT exposition, which is what test/load/gatewaymetrics reads. A Prometheus that
// negotiates protobuf keeps the native histogram configured below (1.1 bucket factor, ~10 % at any
// magnitude) and discards these classic buckets entirely.
func e2eBuckets() []float64 {
	b := append(prometheus.ExponentialBuckets(0.01, 2, 16), e2eLatencyBudgets...)
	sort.Float64s(b)
	return b
}

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
			Help: "Time from submission (or from replay) to the SMSC's terminal submit_sm_resp, by connector" +
				" and status (ok|rejected). A message never sent is not observed; one that WAS throttled" +
				" and later got through carries the wait it spent being throttled.",
			Buckets:                         e2eBuckets(),
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

		MessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messages_total",
			Help: "Messages leaving the MT pipeline, by outcome.",
		}, []string{"status"}),

		RejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rejected_total",
			Help: "Messages rejected by the MT pipeline, by flat error code.",
		}, []string{"code"}),

		PipelineDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pipeline_duration_seconds",
			Help: "Time the MT pipeline spends on one message, excluding queueing.",
			// 100 µs … ~3 s: the pipeline is in-memory apart from a few Redis round-trips, so the
			// interesting resolution is well below a millisecond.
			Buckets:                         prometheus.ExponentialBuckets(0.0001, 2, 15),
			NativeHistogramBucketFactor:     nativeBucketFactor,
			NativeHistogramMaxBucketNumber:  nativeMaxBucketNumber,
			NativeHistogramMinResetDuration: nativeMinResetDuration,
		}),

		SubmitsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "submits_total",
			Help: "SMSC submissions, by connector and outcome.",
		}, []string{"connector_id", "status"}),

		SubmitRejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "submit_rejected_total",
			Help: "SMSC submissions refused, by connector and flat error code.",
		}, []string{"connector_id", "code"}),

		RoutingScriptFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "routing_script_failures_total",
			Help: "Routing scripts that fell back to declarative resolution, by runtime and reason.",
		}, []string{"runtime", "reason"}),

		ExactRouteLookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "exact_route_lookups_total",
			Help: "L0 exact-number lookups, by outcome " +
				"(bloom_miss, redis_hit, redis_error, pg_hit, pg_miss, pg_error).",
		}, []string{"outcome"}),

		ExactRouteCacheCorrupt: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exact_route_cache_corrupt_total",
			Help: "Cached exact-route values that could not be decoded and were healed from the durable table.",
		}),
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
		c.ExactRouteLookups,
		c.ExactRouteCacheCorrupt,
		c.MessagesTotal,
		c.RejectedTotal,
		c.PipelineDuration,
		c.SubmitsTotal,
		c.SubmitRejectedTotal,
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
