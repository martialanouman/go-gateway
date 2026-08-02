package kafka

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
)

// streamBufferedRecords caps what the stream producer holds in memory. Small on purpose: a snapshot is worth
// something for a few seconds, so buffering more would only mean publishing stale dashboard figures after an
// outage. Past this, TryPublish refuses and the snapshot is dropped — which is the correct outcome.
const streamBufferedRecords = 256

// StreamProducer publishes best-effort records on metrics.stream (§1.6).
//
// It is a SEPARATE client from [Producer], not a second method on it, and that is the whole point. Producer is
// acks=all, idempotent and synchronous because it is the durability frontier that earns a REST 202 or an SMPP
// submit_sm_resp. Sharing it would share its record buffer: a burst of dashboard snapshots could then fill the
// buffer and BLOCK a message being durably accepted. A pixel on a dashboard must never be able to do that.
//
// So this client trades durability for isolation — leader acks only, no waiting, and it drops rather than
// blocks when it cannot keep up.
type StreamProducer struct {
	cl      *kgo.Client
	topic   string
	dropped atomic.Int64
}

// NewStreamProducer connects a best-effort producer for the metrics stream. It does not block on
// reachability, and it is deliberately absent from readiness: the stream is not vital, and a service must
// stay in the load balancer with its dashboard feed down.
func NewStreamProducer(cfg config.Kafka) (*StreamProducer, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// LeaderAck, not AllISRAcks: losing a snapshot on a leader failover costs one dashboard frame. It
		// also disables idempotent producing, which is exactly right — a duplicated snapshot is harmless,
		// and the sequencing it costs is throughput we would rather keep.
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.MaxBufferedRecords(streamBufferedRecords),
	}, dialOpts(cfg)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new stream producer: %w", err)
	}
	return &StreamProducer{cl: cl, topic: TopicMetricsStream}, nil
}

// TryPublish writes one record without blocking.
//
// It uses franz-go's TryProduce, which refuses immediately with ErrMaxBuffered instead of waiting for buffer
// space — the difference between best-effort and "best-effort until Kafka is slow, then blocking". The
// refusal arrives through the promise, which franz-go runs on its OWN goroutine, so it cannot be reported to
// the caller synchronously; it is counted here instead, which is where the outcome is actually known.
//
// The context is deliberately Background: cancelling a dashboard snapshot buys nothing, and passing a
// request-scoped context would tie a best-effort publish to the lifetime of the message that triggered it.
func (p *StreamProducer) TryPublish(key, value []byte) {
	p.cl.TryProduce(context.Background(), &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: value,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			p.dropped.Add(1)
		}
	})
}

// Dropped is the number of snapshots that never reached Kafka — a full buffer, an unreachable broker. It is
// the only signal that a dashboard feed is degraded, since nothing else fails when it does; wire it to a
// Prometheus counter.
func (p *StreamProducer) Dropped() int64 { return p.dropped.Load() }

// Close releases the client. Buffered snapshots are abandoned by design — publishing seconds-old dashboard
// figures during shutdown would delay it for no benefit.
func (p *StreamProducer) Close() { p.cl.Close() }
