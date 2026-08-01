package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// Handler processes one consumed record. Returning nil means the record is fully handled —
// including a terminal business outcome already recorded downstream (e.g. an SMSC rejection written
// to the CDR) — so its offset may be committed. Returning an error means a transient infrastructure
// fault: the offset is NOT committed and Run stops, so the supervising service restarts and
// reprocesses from the last commit. A record can therefore be redelivered, so the handler MUST be
// idempotent in whatever it writes downstream (§7.3 — billing is idempotent by message_id, the CDR
// by its versioned rows).
type Handler func(ctx context.Context, rec Record) error

// Consumer is a group consumer that commits offsets only after a record is successfully handled
// (at-least-once, §7.3). Autocommit is disabled so a crash between fetch and handling can never
// silently advance past unprocessed work.
type Consumer struct {
	cl    *kgo.Client
	group string
}

// NewConsumer joins the given consumer group and subscribes to topics. A group with no committed
// offset starts at the earliest record, so durably-queued work is processed rather than skipped.
func NewConsumer(cfg config.Kafka, group string, topics ...string) (*Consumer, error) {
	return newConsumer(cfg, group, kgo.NewOffset().AtStart(), topics...)
}

// NewConsumerFromLatest is like NewConsumer but a group with no committed offset starts at the LATEST
// record. It is for a consumer whose group id is per-instance (e.g. the connector pool's per-connector
// group, step-125): a fresh group must NOT replay the whole retained topic — that would re-send every
// historical message. On a restart the group still resumes from its committed offset; only the very
// first start skips history, which is the intended migration behaviour.
func NewConsumerFromLatest(cfg config.Kafka, group string, topics ...string) (*Consumer, error) {
	return newConsumer(cfg, group, kgo.NewOffset().AtEnd(), topics...)
}

func newConsumer(cfg config.Kafka, group string, reset kgo.Offset, topics ...string) (*Consumer, error) {
	if group == "" {
		return nil, fmt.Errorf("kafka: consumer group must not be empty")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafka: consumer needs at least one topic")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// Commit only after work is done; never let franz-go advance offsets on a timer.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(reset),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	return &Consumer{cl: cl, group: group}, nil
}

// Run polls and processes records until ctx is cancelled, committing each record's offset only
// after handle returns nil. It returns nil on a clean ctx-driven stop and a non-nil error on a
// fetch fault or a handler error (the latter is the signal to restart and reprocess). Run owns the
// poll loop; call it from a single supervised goroutine.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := firstFetchError(fetches); err != nil {
			return fmt.Errorf("kafka: fetch in group %s: %w", c.group, err)
		}

		var (
			procErr error
			handled []*kgo.Record
		)
		fetches.EachRecord(func(kr *kgo.Record) {
			if procErr != nil {
				return // stop at the first failure; its offset stays uncommitted for redelivery
			}
			rec := toRecord(kr)
			if err := handle(ctx, rec); err != nil {
				procErr = fmt.Errorf("handle %s[%d]@%d: %w", kr.Topic, kr.Partition, kr.Offset, err)
				return
			}
			handled = append(handled, kr)
		})

		// Commit the successfully-handled prefix in one request rather than one broker round-trip per
		// record — at the 8000 msg/s target a per-record commit RTT would dominate the consume loop. The
		// at-least-once guarantee is unchanged: only handled records are committed, and anything past the
		// first failure stays uncommitted for redelivery.
		if len(handled) > 0 {
			if err := c.cl.CommitRecords(ctx, handled...); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("kafka: commit in group %s: %w", c.group, err)
			}
		}
		if procErr != nil {
			// A handler that fails purely because ctx was cancelled is a graceful stop, not a fault:
			// PollFetches can hand back a batch a hair before cancellation propagates, so the downstream
			// Produce/Submit aborts with context.Canceled. Treat that like the post-fetch check above —
			// return nil so the supervisor sees a clean shutdown, not a crash.
			if ctx.Err() != nil {
				return nil
			}
			return procErr
		}
	}
}

// BatchHandler processes a whole poll batch and reports, per record aligned by index, whether it was
// handled (nil) or hit a transient fault (non-nil). It lets a consumer fan a batch out across workers
// (the connector pool shards it across parallel binds, step-124) instead of one-at-a-time. The contract
// preserves at-least-once and ordering: the handler MUST fail a record and every LATER record that
// shares its ordering group, so a committed offset never skips unprocessed work. len(results) must equal
// len(recs).
type BatchHandler func(ctx context.Context, recs []Record) []error

// RunBatch polls and processes records a batch at a time, committing — independently per partition — the
// contiguous run of successfully-handled records up to that partition's first failure. It returns nil on
// a clean ctx-driven stop and a non-nil error on a fetch fault or when the batch reported any failure
// (the signal to restart and reprocess the uncommitted records). Like Run, call it from a single
// supervised goroutine.
func (c *Consumer) RunBatch(ctx context.Context, handle BatchHandler) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := firstFetchError(fetches); err != nil {
			return fmt.Errorf("kafka: fetch in group %s: %w", c.group, err)
		}

		var krs []*kgo.Record
		fetches.EachRecord(func(kr *kgo.Record) { krs = append(krs, kr) })
		if len(krs) == 0 {
			continue
		}

		recs := make([]Record, len(krs))
		for i, kr := range krs {
			recs[i] = toRecord(kr)
		}
		results := handle(ctx, recs)

		// Commit each partition up to (but not including) its first failed record. Offsets only compare
		// within a partition, so a global prefix would be wrong: a failure in one partition must not hold
		// back a fully-handled sibling partition, and a success AFTER a failure in the SAME partition must
		// never be committed (it would skip the gap). krs is in per-partition offset order.
		if commit := committablePrefix(krs, results); len(commit) > 0 {
			if err := c.cl.CommitRecords(ctx, commit...); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("kafka: commit in group %s: %w", c.group, err)
			}
		}
		if firstErr := firstNonNil(results); firstErr != nil {
			// A failure that is really just ctx cancellation mid-batch is a graceful stop, not a fault
			// (PollFetches can hand back a batch a hair before cancellation propagates) — mirror Run.
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka: batch handle in group %s: %w", c.group, firstErr)
		}
	}
}

// partitionKey identifies a Kafka partition across topics (a consumer may subscribe to several).
type partitionKey struct {
	topic     string
	partition int32
}

// committablePrefix returns the records safe to commit: those handled successfully whose offset precedes
// their partition's first failed offset. results is aligned with krs by index.
func committablePrefix(krs []*kgo.Record, results []error) []*kgo.Record {
	firstFail := make(map[partitionKey]int64)
	for i, kr := range krs {
		if results[i] == nil {
			continue
		}
		pk := partitionKey{kr.Topic, kr.Partition}
		if off, ok := firstFail[pk]; !ok || kr.Offset < off {
			firstFail[pk] = kr.Offset
		}
	}
	var out []*kgo.Record
	for i, kr := range krs {
		if results[i] != nil {
			continue
		}
		if off, ok := firstFail[partitionKey{kr.Topic, kr.Partition}]; ok && kr.Offset > off {
			continue // a later record in a partition with an earlier failure: leave it for redelivery
		}
		out = append(out, kr)
	}
	return out
}

// firstNonNil returns the first non-nil error in the slice, or nil.
func firstNonNil(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// Ping reports whether the brokers are reachable.
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// ReadyCheck adapts the consumer to a readiness probe.
func (c *Consumer) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, c.cl.Ping)
}

// Close leaves the group and releases the client. Because offsets are committed synchronously as
// records are handled, there is nothing to flush.
func (c *Consumer) Close() { c.cl.Close() }

// firstFetchError returns the first fetch error that is not a context cancellation (those are the
// normal consequence of a shutdown and are handled by the ctx check in Run).
func firstFetchError(fetches kgo.Fetches) error {
	var out error
	fetches.EachError(func(_ string, _ int32, err error) {
		if out != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		out = err
	})
	return out
}

func toRecord(kr *kgo.Record) Record {
	r := Record{
		Topic:     kr.Topic,
		Key:       kr.Key,
		Value:     kr.Value,
		Partition: kr.Partition,
		Offset:    kr.Offset,
	}
	for _, h := range kr.Headers {
		r.Headers = append(r.Headers, Header{Key: h.Key, Value: h.Value})
	}
	return r
}

// Lag reports this consumer group's backlog per topic, in records.
//
// It reuses the consumer's own client rather than opening an admin connection: the group's lag is exactly
// what THIS service is behind on, which is also why the service that consumes a topic is the only one that
// should publish its depth — several services reporting the same topic would double-count without any of
// them being wrong (step-180).
//
// A broker round-trip is involved, so call it on a slow tick, never per message. An error is returned rather
// than swallowed; the caller decides, and for the metrics stream the answer is "skip this tick".
func (c *Consumer) Lag(ctx context.Context) (map[string]int64, error) {
	lags, err := kadm.NewClient(c.cl).Lag(ctx, c.group)
	if err != nil {
		return nil, fmt.Errorf("kafka: lag for group %s: %w", c.group, err)
	}
	described, ok := lags[c.group]
	if !ok {
		return nil, fmt.Errorf("kafka: lag for group %s: group not described", c.group)
	}
	// A described group can still carry a describe or fetch error, in which case its Lag map is empty. Left
	// unchecked that returns "no lag" — the gauge would simply hold its last value and the failure would be
	// invisible.
	if err := described.Error(); err != nil {
		return nil, fmt.Errorf("kafka: lag for group %s: %w", c.group, err)
	}
	out := make(map[string]int64, len(described.Lag))
	for topic, partitions := range described.Lag {
		var total int64
		for _, p := range partitions {
			// A partition whose lag could not be computed reports -1 WITH an error. Skipping it silently
			// would publish a small total that looks perfectly legitimate — the worst failure mode for a
			// backlog gauge, since it reads as "we are caught up". Refuse the whole topic instead.
			if p.Err != nil {
				return nil, fmt.Errorf("kafka: lag for group %s, topic %s partition %d: %w",
					c.group, topic, p.Partition, p.Err)
			}
			if p.Lag > 0 {
				total += p.Lag
			}
		}
		out[topic] = total
	}
	return out, nil
}
