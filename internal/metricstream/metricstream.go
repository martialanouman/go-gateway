// Package metricstream feeds the metrics.stream topic (§1.6) with the live figures the realtime WS/SSE
// gateway broadcasts (step-183).
//
// Two properties shape everything here:
//
//   - It is BEST-EFFORT. A stream event is a dashboard pixel; the CDR is the authority for what happened to a
//     message. Nothing in this package may block, fail or slow the hot path — a saturated or dead Kafka costs
//     a counter, never a message.
//   - It is BOUNDED. Snapshots are periodic aggregates, not sampled per-message events, so the volume is one
//     record per tick per service whatever the traffic — and the freshness a dashboard needs (< 5 s, step-183)
//     is a property of the tick, not of the load.
package metricstream

import (
	"time"
)

// SchemaVersion is the wire version of [Snapshot]. Bump it when the shape changes in a way a consumer must
// notice; step-183 and any later reader branch on it. The topic has no schema registry, so this field is the
// only thing standing between a format change and a silently misreading consumer.
const SchemaVersion = 1

// Labels are the bounded dimensions of a sample. Names must belong to the shared vocabulary
// (internal/observability/metrics) — the same one the Prometheus registry enforces, so a dimension cannot be
// legitimate in one exposition and forbidden in the other.
//
// VALUES must come from constants or from a mapping that buckets the unknown; see [Emitter] for what happens
// when they do not.
type Labels map[string]string

// Sample is one measurement in a snapshot.
type Sample struct {
	// Kind is the metric name, matching the catalogue in internal/observability/metrics where one exists.
	Kind   string  `json:"kind"`
	Labels Labels  `json:"labels,omitempty"`
	Value  float64 `json:"value"`
}

// Feed names which realtime stream a record belongs to. metrics.stream carries all three (§1.6 declares it
// as the realtime topic; adding topics is a spec change).
//
// DEPLOY ORDER: admin-api-svc first. A consumer predating this field branches on V alone, so it would push a
// SessionEvent onto the metrics feed — the compatibility is one-way.
type Feed string

// The feeds, matching the hub's stream names.
const (
	FeedMetrics       Feed = "metrics"
	FeedSessions      Feed = "sessions"
	FeedBillingAlerts Feed = "billing-alerts"
)

// SessionEvent reports an SMPP bind state change. Identifiers only — never a password or a body.
type SessionEvent struct {
	V         int       `json:"v"`
	Feed      Feed      `json:"feed"`
	Service   string    `json:"service"`
	Instance  string    `json:"instance"`
	EmittedAt time.Time `json:"emitted_at"`
	AccountID string    `json:"account_id"`
	SystemID  string    `json:"system_id"`
	// State is bound | unbound.
	State string `json:"state"`
	// Sessions is the account's live bind count after the change. Absent on unbound: the registry's
	// UnbindResponse does not carry the remaining count, and publishing 0 would read as "no binds left".
	Sessions *int `json:"sessions,omitempty"`
}

// BillingAlert reports a threshold an operator must see.
type BillingAlert struct {
	V          int       `json:"v"`
	Feed       Feed      `json:"feed"`
	Service    string    `json:"service"`
	Instance   string    `json:"instance"`
	EmittedAt  time.Time `json:"emitted_at"`
	CustomerID string    `json:"customer_id"`
	// OwnerType/OwnerID name the balance the alert is about. The MO meter is owner-scoped, so under
	// balance_scope=smpp_account two accounts of one customer alert separately and must stay distinguishable.
	OwnerType string `json:"owner_type"`
	OwnerID   string `json:"owner_id"`
	// Alert is mo_floor_reached. Low-balance and breaker-open alerts are not emitted yet: neither has a
	// configured threshold to fire on, and the threshold belongs to the dashboard's alert_rules, not here.
	// This feed is for display in any case — detection needs a durable source with a persisted cursor
	// (step-400).
	Alert   string `json:"alert"`
	Balance int64  `json:"balance"`
}

// Snapshot is one record on metrics.stream: everything a service measured during one interval.
//
// Counters carry the DELTA over the interval, gauges their current level. A consumer therefore needs no state
// to render a rate, and a reconnecting browser is never shown a total accumulated since boot.
type Snapshot struct {
	V int `json:"v"`
	// Feed is empty on records produced before step-184; a reader treats empty as FeedMetrics.
	Feed    Feed   `json:"feed,omitempty"`
	Service string `json:"service"`
	// Instance identifies the REPLICA that produced this snapshot (the pod name by default). Without it a
	// consumer cannot aggregate: every replica of a service publishes the same kinds under the same service
	// name, indistinguishable and interleaved on the topic.
	//
	// The aggregation contract a consumer must follow (step-183):
	//
	//   - COUNTERS (interval deltas) are SUMMED across instances. Two replicas each routing 100 messages
	//     routed 200.
	//   - INSTANCE-SCOPED GAUGES are read per instance, then summed or maxed as the question requires.
	//   - GROUP-SCOPED GAUGES — queue_depth_records above all — are the SAME value on every replica, because
	//     the consumer-group lag is a property of the group, not of the pod. Summing them multiplies the
	//     backlog by the replica count. Take the latest per instance and then the MAX, never the sum.
	Instance  string    `json:"instance"`
	EmittedAt time.Time `json:"emitted_at"`
	Samples   []Sample  `json:"samples"`
	// DroppedSinceStart is CUMULATIVE, unlike the counters in Samples which are interval deltas. It says so
	// in its name so a consumer does not plot it as a rate.
	DroppedSinceStart int64 `json:"dropped_since_start,omitempty"`
}

// Sink is where snapshots go, defined consumer-side so this package depends on no infrastructure.
//
// TryPublish must NOT block and reports nothing. It returns no error on purpose: the Kafka client refuses a
// full buffer through an ASYNCHRONOUS promise, so a synchronous "accepted" would be a fiction — and there is
// nothing a caller could do with the answer anyway, since the next tick supersedes a lost snapshot. A sink
// that drops counts its own drops, where it actually knows about them.
type Sink interface {
	TryPublish(key, value []byte)
}
