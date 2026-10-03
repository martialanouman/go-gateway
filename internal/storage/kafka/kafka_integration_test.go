package kafka_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
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

// TestConsumerReplaysAFailedRecordInPlace is step-285: a transient handler failure used to end Run, and the
// supervisor took the whole process down with it. The consumer now replays the record itself, and commits it.
func TestConsumerReplaysAFailedRecordInPlace(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	cfg := config.Kafka{Brokers: brokers, Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	run := map[string]func(*kafka.Consumer, context.Context, kafka.Handler) error{
		"Run": func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error { return c.Run(ctx, h) },
		"RunBatch": func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error {
			return c.RunBatch(ctx, func(ctx context.Context, recs []kafka.Record) []error {
				out := make([]error, len(recs))
				for i, rec := range recs {
					if out[i] = h(ctx, rec); out[i] != nil {
						for j := i + 1; j < len(recs); j++ {
							out[j] = out[i]
						}
						break
					}
				}
				return out
			})
		},
	}
	for name, runner := range run {
		t.Run(name, func(t *testing.T) {
			group := "test-replay-" + name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			marker := []byte(group)
			if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: marker}); err != nil {
				t.Fatalf("produce: %v", err)
			}

			consumer, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			defer consumer.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failures, handled := 2, make(chan struct{}, 1)
			runErr := make(chan error, 1)
			go func() {
				runErr <- runner(consumer, ctx, func(_ context.Context, rec kafka.Record) error {
					if string(rec.Value) != group {
						return nil
					}
					if failures > 0 {
						failures--
						return errors.New("billing: deadline exceeded")
					}
					handled <- struct{}{}
					return nil
				})
			}()

			select {
			case err := <-runErr:
				t.Fatalf("%s returned %v on a transient failure: the supervisor would restart the process", name, err)
			case <-handled:
			case <-time.After(30 * time.Second):
				t.Fatal("the failed record was never replayed")
			}
			cancel()
			if err := <-runErr; err != nil {
				t.Fatalf("%s after cancel = %v, want nil", name, err)
			}
			consumer.Close() // its partitions must go to the next member of the group before the deadline below

			// A record produced after the replay proves the next member is reading before it is asked what it saw.
			after := group + "-after"
			if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte(after)}); err != nil {
				t.Fatalf("produce: %v", err)
			}
			again, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			defer again.Close()
			actx, acancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer acancel()
			_ = again.Run(actx, func(_ context.Context, rec kafka.Record) error {
				switch string(rec.Value) {
				case group:
					t.Error("the replayed record was redelivered to the group: its offset was never committed")
				case after:
					acancel()
				}
				return nil
			})
			if !errors.Is(actx.Err(), context.Canceled) {
				t.Fatal("the next member never read the record produced after the replay")
			}
		})
	}
}

// TestConsumerResumesAnAbandonedReplay: connector-pool calls RunBatch again on the same consumer after a
// bind drop cancels it. The fetch cursor is already past the records the cancelled call left uncommitted; a
// next call that polled first would commit past them.
func TestConsumerResumesAnAbandonedReplay(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	// asBatch runs a per-record handler through RunBatch, failing the suffix from the first error.
	asBatch := func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error {
		return c.RunBatch(ctx, func(ctx context.Context, recs []kafka.Record) []error {
			out := make([]error, len(recs))
			for i, rec := range recs {
				if out[i] = h(ctx, rec); out[i] != nil {
					for j := i + 1; j < len(recs); j++ {
						out[j] = out[i]
					}
					break
				}
			}
			return out
		})
	}
	run := map[string]func(*kafka.Consumer, context.Context, kafka.Handler) error{
		"Run":      func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error { return c.Run(ctx, h) },
		"RunBatch": asBatch,
	}
	for name, runner := range run {
		t.Run(name, func(t *testing.T) {
			group := "test-abandoned-" + name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			marker, after := group, group+"-after"
			if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte(marker)}); err != nil {
				t.Fatalf("produce: %v", err)
			}
			consumer, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			defer consumer.Close()

			ctx, cancel := context.WithCancel(t.Context())
			err = runner(consumer, ctx, func(_ context.Context, rec kafka.Record) error {
				if string(rec.Value) == marker {
					cancel() // the bind dropped while this record was failing
					return errors.New("connectorpool: submit_sm: bind closed")
				}
				return nil
			})
			if err != nil {
				t.Fatalf("cancelled %s = %v, want nil", name, err)
			}

			if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte(after)}); err != nil {
				t.Fatalf("produce: %v", err)
			}
			ctx, cancel = context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			sawMarker := false
			_ = runner(consumer, ctx, func(_ context.Context, rec kafka.Record) error {
				switch string(rec.Value) {
				case marker:
					sawMarker = true
				case after:
					cancel()
				}
				return nil
			})
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatal("the second call never read the record produced after the first")
			}
			if !sawMarker {
				t.Fatalf("the record the cancelled %s left uncommitted was skipped by the next call on the same consumer", name)
			}
		})
	}
}

// TestConsumerDoesNotStallOnSporadicFailures is the step-285 VPS run: 2 % of reserves outran their deadline,
// and a backoff that grew with every failure of a batch held all of its partitions for 30 s at a time.
func TestConsumerDoesNotStallOnSporadicFailures(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()
	group := "test-sporadic-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	const records = 300
	for i := range records {
		v := group + "-" + strconv.Itoa(i)
		key := []byte("sporadic-" + strconv.Itoa(i)) // fixed, so the failures land on the same partitions every run
		if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: key, Value: []byte(v)}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	consumer, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	failedOnce, handled := map[string]bool{}, map[string]bool{}
	_ = consumer.RunBatch(ctx, func(_ context.Context, recs []kafka.Record) []error {
		out := make([]error, len(recs))
		halted := map[int32]bool{}
		for i, rec := range recs {
			v := string(rec.Value)
			n, err := strconv.Atoi(strings.TrimPrefix(v, group+"-"))
			switch {
			case err != nil: // another test's record
			case halted[rec.Partition]:
				out[i] = errors.New("lane halted")
			case n%20 == 0 && !failedOnce[v]:
				failedOnce[v], halted[rec.Partition] = true, true
				out[i] = errors.New("billing: deadline exceeded")
			default:
				handled[v] = true
			}
		}
		if len(handled) == records {
			cancel()
		}
		return out
	})
	if len(handled) != records {
		t.Fatalf("%d of %d records handled in 15 s: one failure in twenty stalled the consumer", len(handled), records)
	}
}

// TestConsumerBacksOffARecordThatNeverPasses: only an attempt that handled something replays at once. A record
// that fails for good must still wait between attempts, or an outage is hammered at CPU speed.
func TestConsumerBacksOffARecordThatNeverPasses(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	run := map[string]func(*kafka.Consumer, context.Context, kafka.Handler) error{
		"Run": func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error { return c.Run(ctx, h) },
		"RunBatch": func(c *kafka.Consumer, ctx context.Context, h kafka.Handler) error {
			return c.RunBatch(ctx, func(ctx context.Context, recs []kafka.Record) []error {
				out := make([]error, len(recs))
				for i, rec := range recs {
					out[i] = h(ctx, rec)
				}
				return out
			})
		},
	}
	for name, runner := range run {
		t.Run(name, func(t *testing.T) {
			group := "test-backoff-" + name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			if err := producer.Produce(t.Context(), kafka.Record{Topic: kafka.TopicMTRouted, Key: []byte("k"), Value: []byte(group)}); err != nil {
				t.Fatalf("produce: %v", err)
			}
			consumer, err := kafka.NewConsumer(cfg, group, kafka.TopicMTRouted)
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			defer consumer.Close()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var first time.Time
			calls := 0
			_ = runner(consumer, ctx, func(_ context.Context, rec kafka.Record) error {
				if string(rec.Value) != group {
					return nil
				}
				if first.IsZero() {
					first = time.Now()
				}
				if time.Since(first) > 2500*time.Millisecond {
					cancel()
				} else {
					calls++
				}
				return errors.New("billing-svc unavailable")
			})
			// Idle attempts wait 0.8–1.2 s, then 1.6–2.4 s: three calls within 2.5 s, plus whatever immediate
			// replays the other tests' records on mt.routed earn by committing. Without the backoff it is millions.
			if calls > 10 {
				t.Errorf("%d handler calls in 2.5 s on a record that never passes: the replay does not back off", calls)
			}
		})
	}
}
