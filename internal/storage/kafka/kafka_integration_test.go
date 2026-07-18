package kafka_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

func TestProduceConsumeRoundTrip(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	cfg := config.Kafka{Brokers: brokers, Timeout: 3 * time.Second}

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	want := kafka.Record{
		Topic: kafka.TopicMTInbound,
		Key:   []byte("acct-hash"),
		Value: []byte("envelope-bytes"),
		Headers: []kafka.Header{
			{Key: kafka.HeaderMessageID, Value: []byte("msg-1")},
			{Key: kafka.HeaderTraceID, Value: []byte("trace-1")},
		},
	}
	if err := producer.Produce(ctx, want); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := kafka.NewConsumer(cfg, "test-roundtrip", kafka.TopicMTInbound)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	got := make(chan kafka.Record, 1)
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(runCtx, func(_ context.Context, rec kafka.Record) error {
			select {
			case got <- rec:
			default:
			}
			return nil
		})
	}()

	select {
	case rec := <-got:
		if string(rec.Value) != string(want.Value) {
			t.Errorf("value: got %q want %q", rec.Value, want.Value)
		}
		if string(rec.Key) != string(want.Key) {
			t.Errorf("key: got %q want %q", rec.Key, want.Key)
		}
		if v, ok := rec.Header(kafka.HeaderMessageID); !ok || string(v) != "msg-1" {
			t.Errorf("message_id header: got %q ok=%v", v, ok)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the record")
	}

	cancel()
	wg.Wait()
}

func TestConsumerCommitsAfterProcessing(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	cfg := config.Kafka{Brokers: brokers, Timeout: 3 * time.Second}
	const group = "test-commit-after"

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	if err := producer.Produce(ctx, kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte("v1")}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// First consumer instance: handle the record, commit, then stop.
	consume := func(handle kafka.Handler) {
		c, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
		if err != nil {
			t.Fatalf("new consumer: %v", err)
		}
		defer c.Close()
		runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		done := make(chan struct{})
		go func() { _ = c.Run(runCtx, handle); close(done) }()
		<-runCtx.Done()
		<-done
	}

	firstSeen := make(chan struct{}, 8)
	consume(func(_ context.Context, _ kafka.Record) error {
		firstSeen <- struct{}{}
		return nil
	})
	if len(firstSeen) == 0 {
		t.Fatal("first consumer never saw the record")
	}

	// Produce a second record; a fresh consumer in the SAME group must resume past the committed
	// first offset and see only the new record, proving the offset was committed.
	if err := producer.Produce(ctx, kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte("v2")}); err != nil {
		t.Fatalf("produce v2: %v", err)
	}

	seen := make(chan string, 8)
	consume(func(_ context.Context, rec kafka.Record) error {
		seen <- string(rec.Value)
		return nil
	})

	var values []string
	for {
		select {
		case v := <-seen:
			values = append(values, v)
			continue
		default:
		}
		break
	}
	for _, v := range values {
		if v == "v1" {
			t.Fatalf("second consumer re-read committed record v1; values=%v", values)
		}
	}
}
