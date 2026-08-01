package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// TestSnapshotLandsOnMetricsStream is step-182's first acceptance criterion, exercised where it actually
// happens: the real producer, a real broker, the real topic. Everything else in this feature is tested against
// a fake sink, which cannot tell whether the topic exists, whether the record is produced, or whether the JSON
// survives the round trip.
func TestSnapshotLandsOnMetricsStream(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}

	producer, err := kafka.NewStreamProducer(cfg)
	if err != nil {
		t.Fatalf("NewStreamProducer: %v", err)
	}
	t.Cleanup(producer.Close)

	emitter, err := metricstream.New("router-svc", producer, metricstream.WithInstance("router-svc-abc123"))
	if err != nil {
		t.Fatalf("New emitter: %v", err)
	}
	emitter.Add("messages_total", metricstream.Labels{"status": "routed"}, 3)
	emitter.Set("queue_depth_records", metricstream.Labels{"queue": kafka.TopicMTInbound}, 7)
	emitter.PublishNow()

	consumer, err := kafka.NewConsumer(cfg, "metrics-stream-test", kafka.TopicMetricsStream)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := make(chan metricstream.Snapshot, 1)
	go func() {
		_ = consumer.Run(ctx, func(_ context.Context, rec kafka.Record) error {
			var snap metricstream.Snapshot
			if err := json.Unmarshal(rec.Value, &snap); err != nil {
				return err
			}
			select {
			case got <- snap:
			default:
			}
			return nil
		})
	}()

	select {
	case snap := <-got:
		if snap.V != metricstream.SchemaVersion {
			t.Errorf("v = %d, want %d", snap.V, metricstream.SchemaVersion)
		}
		if snap.Instance != "router-svc-abc123" {
			t.Errorf("instance = %q; a consumer cannot aggregate replicas without it", snap.Instance)
		}
		var sawCounter, sawGauge bool
		for _, s := range snap.Samples {
			switch s.Kind {
			case "messages_total":
				sawCounter = s.Value == 3
			case "queue_depth_records":
				sawGauge = s.Value == 7
			}
			// Invariant (a) is structural here: only bounded label names can reach a series at all.
			for name := range s.Labels {
				if name == "msisdn" || name == "message_id" || name == "body" {
					t.Fatalf("unbounded label %q reached metrics.stream", name)
				}
			}
		}
		if !sawCounter || !sawGauge {
			t.Errorf("snapshot did not survive the round trip: %+v", snap.Samples)
		}
	case <-ctx.Done():
		t.Fatal("no snapshot reached metrics.stream within the timeout")
	}
}
