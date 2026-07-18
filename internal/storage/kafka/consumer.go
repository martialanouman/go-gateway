package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

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
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
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

		var procErr error
		fetches.EachRecord(func(kr *kgo.Record) {
			if procErr != nil {
				return // stop at the first failure; its offset stays uncommitted for redelivery
			}
			rec := toRecord(kr)
			if err := handle(ctx, rec); err != nil {
				procErr = fmt.Errorf("handle %s[%d]@%d: %w", kr.Topic, kr.Partition, kr.Offset, err)
				return
			}
			if err := c.cl.CommitRecords(ctx, kr); err != nil {
				procErr = fmt.Errorf("commit %s[%d]@%d: %w", kr.Topic, kr.Partition, kr.Offset, err)
				return
			}
		})
		if procErr != nil {
			// A handler or commit that fails purely because ctx was cancelled is a graceful stop, not a
			// fault: PollFetches can hand back a batch a hair before cancellation propagates, so the
			// downstream Produce/Submit or CommitRecords aborts with context.Canceled. Treat that like the
			// post-fetch check above — return nil so the supervisor sees a clean shutdown, not a crash.
			if ctx.Err() != nil {
				return nil
			}
			return procErr
		}
	}
}

// Ping reports whether the brokers are reachable.
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// ReadyCheck adapts the consumer to a readiness probe.
func (c *Consumer) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return c.cl.Ping(ctx)
		},
	}
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
