//go:build loadref

package e2e_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// TestRoutedProduceCeiling isolates the router's mt.routed produce, because the reference run can say
// the router is behind but never why — the same reason TestCDRWriteCeiling exists beside it, and it is
// deliberately built to the same shape.
//
// It is a diagnostic, not a gate: it asserts only that the path moves, and reports the curve.
//
// # The question
//
// The router publishes with Producer.Produce, which is ProduceSync with acks=all — one record, blocking
// until the broker has durably acknowledged it — called from INSIDE the single consume goroutine
// (internal/router: the fan-out loop over segments). So one message costs at least one broker
// round-trip, paid in series with the next message's processing.
//
//	concurrency 1 lands near the reference run's output rate -> the serialised round-trip IS the
//	                                                            bottleneck, and it is named.
//	the rate rises with concurrency                          -> overlapping the round-trips recovers
//	                                                            throughput: a fan-out of the consume loop,
//	                                                            or a barrier over records in flight,
//	                                                            both work and both are worth the same.
//	the rate plateaus whatever the concurrency               -> a broker-side ceiling. Neither fix helps,
//	                                                            and the conclusion is about Kafka, not the
//	                                                            router.
//
// # Why concurrency is the only dimension
//
// A "records per call" sweep looks like a second dimension and is not one. N goroutines each blocked in
// their own ProduceSync put N records in flight against the same client, which franz-go coalesces into
// broker requests exactly as it would coalesce one N-record call — and N goroutines joined by a barrier
// IS the shape a batching fix would take. So this curve prices both candidate fixes, using only the
// production API, without adding one before the measurement says it is worth having.
//
// The record is the one the router publishes: encoded by pipeline.EncodeRouted, keyed by message id, one
// segment, a GSM-7 body. A synthetic payload would compress differently and price a different message.
func TestRoutedProduceCeiling(t *testing.T) {
	brokers := kafkatest.Brokers(t)

	producer, err := kafka.NewProducer(refKafkaConfig(brokers))
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(producer.Close)

	hold := envDuration(t, envCalHold, 10*time.Second)
	for _, concurrency := range []int{1, 4, 16, 64, 256} {
		rate := measureProduceRate(t, producer, concurrency, hold)
		t.Logf("mt.routed produce: %3d records in flight -> %8.0f produce/s (acks=all)", concurrency, rate)
		if rate == 0 {
			t.Fatalf("the producer moved nothing at concurrency %d", concurrency)
		}
	}
}

// measureProduceRate holds `concurrency` producers against mt.routed for the window and returns the rate
// in acknowledged records per second.
func measureProduceRate(t *testing.T, producer *kafka.Producer, concurrency int, hold time.Duration) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), hold)
	defer cancel()

	var done atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now()
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				// Encoded inside the loop, as the router does: a record hoisted out would share one key
				// across every produce and land the whole measurement on a single partition.
				rec, err := pipeline.EncodeRouted(routedBench())
				if err != nil {
					t.Errorf("encode mt.routed: %v", err)
					return
				}
				if err := producer.Produce(ctx, rec); err != nil {
					// A cancelled produce is the window closing, not a failure; anything else is worth
					// seeing, but one blip must not zero the measurement.
					if ctx.Err() == nil {
						t.Logf("produce: %v", err)
					}
					return
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	return float64(done.Load()) / time.Since(start).Seconds()
}

// routedBench is the record the router publishes after a successful pipeline pass: one segment, a GSM-7
// body of the length the injector sends, distinct ids so keys spread across partitions the way real
// traffic does.
func routedBench() pipeline.RoutedMT {
	const body = "Your one time code is 424242. It expires in ten minutes. Do not share it with anyone, our staff will never ask you for it."
	return pipeline.RoutedMT{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		From:         refSenderID,
		To:           "2250700000000",
		Body:         msg.NewBodyString(body),
		Encoding:     "gsm7",
		ConnectorID:  uuid.New(),
		SegmentSeq:   1,
		SegmentCount: 1,
		SubmittedAt:  time.Now().UTC(),
	}
}
