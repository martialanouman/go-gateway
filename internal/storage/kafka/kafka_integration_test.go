package kafka_test

import (
	"context"
	"strconv"
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

// TestConsumerCommitsHandledRecordsOnShutdown: a record already handled when SIGTERM lands is NOT
// redelivered to the next instance.
//
// The window is narrow and routine: the handler returns just as the context is cancelled, so the
// commit that follows runs on a context that is already dead. Before step-260 that commit failed
// instantly and the failure was reclassified as a clean stop (`if ctx.Err() != nil { return nil }`) —
// a graceful drain therefore discarded offsets exactly like a kill -9.
//
// What that costs is not abstract. connectorpool consumes mt.routed and submits to the SMSC; a
// redelivered record is submitted AGAIN, and reroute.go says it plainly: billing is idempotent by
// message_id, "but the extra submit itself is not undone". A duplicate SMS reaches the subscriber on
// every rolling deploy.
//
// The assertion is the redelivery, not the call to CommitRecords: checking the call would replay the
// function under test on both sides of the equals sign and pass under any commit that was merely
// attempted.
func TestConsumerCommitsHandledRecordsOnShutdown(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	cfg := config.Kafka{Brokers: brokers, Timeout: 3 * time.Second}
	group := "test-commit-on-shutdown-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	if err := producer.Produce(ctx, kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte("shutdown-v1")}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// First instance: the handler succeeds at the very moment the drain begins, so the commit that
	// follows it runs on a cancelled context — the SIGTERM race, made deterministic.
	first, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	handled := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() {
		done <- first.Run(runCtx, func(c context.Context, _ kafka.Record) error {
			handled <- struct{}{}
			<-c.Done() // still in flight when the drain starts
			return nil // ...and then succeeds: the work IS done
		})
	}()

	select {
	case <-handled:
	case <-time.After(30 * time.Second):
		t.Fatal("first consumer never received the record")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run on shutdown = %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("first consumer did not stop after cancellation")
	}
	first.Close()

	// Second instance, same group: it must resume PAST the handled record.
	second, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
	if err != nil {
		t.Fatalf("new consumer 2: %v", err)
	}
	defer second.Close()

	redelivered := make(chan string, 8)
	secondCtx, secondCancel := context.WithTimeout(ctx, 10*time.Second)
	defer secondCancel()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		_ = second.Run(secondCtx, func(_ context.Context, rec kafka.Record) error {
			redelivered <- string(rec.Value)
			return nil
		})
	}()
	<-secondCtx.Done()
	<-secondDone

	for {
		select {
		case v := <-redelivered:
			if v == "shutdown-v1" {
				t.Fatalf("record %q was redelivered after a graceful shutdown: the offset of work "+
					"that was already done went uncommitted, so a rolling deploy re-submits it — a "+
					"duplicate SMS to the subscriber", v)
			}
			continue
		default:
		}
		break
	}
}
