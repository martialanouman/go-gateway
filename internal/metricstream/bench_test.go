package metricstream_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/metricstream"
)

type discardSink struct{}

func (discardSink) TryPublish([]byte, []byte) {}

// BenchmarkHotPathMessage measures what one routed message costs the emitter: the three recordings
// router-svc makes per message.
//
// The steady state must allocate NOTHING. At 8 000 msg/s an allocation per recording is hundreds of thousands
// of allocations per second of pure garbage on the hottest path in the system — never a throughput risk, but
// GC pressure paid forever for a dashboard. Run with -benchmem; a regression here shows up as allocs/op
// leaving zero.
func BenchmarkHotPathMessage(b *testing.B) {
	e, err := metricstream.New("router-svc", discardSink{})
	if err != nil {
		b.Fatal(err)
	}
	routed := metricstream.Labels{"status": "routed"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Observe("pipeline_duration_seconds", nil, 0.001)
			e.Add("messages_total", routed, 1)
		}
	})
}
