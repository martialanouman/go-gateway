package metrics

import "github.com/prometheus/client_golang/prometheus"

// DropCounter is anything that counts the realtime records it refused since start. Declared consumer-side so
// this package depends on neither the Kafka producer nor the metricstream publisher.
type DropCounter interface {
	Dropped() int64
}

// DropCounterFunc adapts a plain function to [DropCounter], for a source that exposes its drops under
// another name — an event publisher counting per reason, say.
type DropCounterFunc func() int64

// Dropped implements [DropCounter].
func (f DropCounterFunc) Dropped() int64 { return f() }

// streamDropHelp is shared by every reason: Prometheus requires one Help per metric name, and the three
// reasons are one metric seen from three angles.
const streamDropHelp = "Realtime records that never reached metrics.stream, by reason."

// StreamDropCollector exposes one source of metrics.stream drops under metrics_stream_dropped_total, tagged
// with a constant reason label.
//
// Reasons in use:
//
//	buffer    the producer's buffer was full, or the broker unreachable
//	rate_cap  over the session-event rate cap
//	encode    the record could not be serialised
//	refused   the emitter itself refused a sample: an unbounded label, the series cap, or a snapshot that
//	          would not serialise — every cause a code defect
//
// They stay separate on purpose. A rate-cap drop is an expected operational signal under load and an encode
// drop is a bug; merging them would bury the second in the noise of the first.
//
// Register only the reasons a service can actually produce. A series pinned at zero reads as a guarantee —
// an operator seeing rate_cap on a service that never rate-caps concludes its feed is throttled.
//
// Several collectors share the metric name because Prometheus identifies a Desc by its name AND its constant
// labels, so registering one per reason is legitimate — provided the Help matches, which is why it is a
// constant here.
func StreamDropCollector(reason string, d DropCounter) prometheus.Collector {
	return prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name:        "metrics_stream_dropped_total",
		Help:        streamDropHelp,
		ConstLabels: prometheus.Labels{"reason": reason},
	}, func() float64 { return float64(d.Dropped()) })
}
