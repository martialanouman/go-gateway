package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// Handler processes one consumed record. Returning nil means the record is fully handled —
// including a terminal business outcome already recorded downstream (e.g. an SMSC rejection written
// to the CDR) — so its offset may be committed. Returning an error means a transient infrastructure
// fault: the offset is NOT committed and Run handles the record again, in place, after a backoff. A
// record can therefore be handled more than once, so the handler MUST be
// idempotent in whatever it writes downstream (§7.3 — billing is idempotent by message_id, the CDR
// by its versioned rows).
type Handler func(ctx context.Context, rec Record) error

// Consumer is a group consumer that commits offsets only after a record is successfully handled
// (at-least-once, §7.3). Autocommit is disabled so a crash between fetch and handling can never
// silently advance past unprocessed work.
type Consumer struct {
	cl    *kgo.Client
	group string

	// commitTimeout bounds the ONE commit that matters most: the last one, issued while the pod is
	// already draining and its own context is dead. config validates KAFKA_TIMEOUT as positive.
	commitTimeout time.Duration

	// fromEnd records where a fresh group starts, so a caller can assert it. The choice is a
	// durability property — a group that starts at the end skips whatever was produced before it first
	// joined — and one that no test could observe was one no test could guard (step-201c D9).
	fromEnd bool

	// held are the records a cancelled Run or RunBatch left uncommitted; see [Consumer.next]. Only the one
	// goroutine that runs the consumer touches it.
	held []*kgo.Record
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
	shared, err := consumerOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		// Commit only after work is done; never let franz-go advance offsets on a timer.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(reset),
	}, shared...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}
	return &Consumer{
		cl:            cl,
		group:         group,
		commitTimeout: cfg.Timeout,
		fromEnd:       reset.EpochOffset().Offset == kgo.NewOffset().AtEnd().EpochOffset().Offset,
	}, nil
}

// commit commits the handled records, and retries ONCE on a detached context when the failure was the
// drain itself.
//
// The retry is the point. A commit issued on a context that SIGTERM has already cancelled fails
// instantly without reaching the broker, and treating that as a clean stop discards the offsets of work
// that was actually done — a graceful shutdown then loses exactly as much as a kill -9, and the next
// instance redelivers records the SMSC has already been sent (guide de codage §5 [MUST]: "les consumers
// Kafka valident leurs offsets en cours puis s'arrêtent").
//
// Detaching on shutdown is the established shape here: smppserver's releaseToken and
// observability.DrainTracing both do the same thing for the same reason — the work outlives the context
// that asked for it to stop.
func (c *Consumer) commit(ctx context.Context, recs ...*kgo.Record) error {
	err := c.cl.CommitRecords(ctx, recs...)
	if err == nil || ctx.Err() == nil {
		return err
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.commitTimeout)
	defer cancel()
	return c.cl.CommitRecords(dctx, recs...)
}

// Run polls and processes records until ctx is cancelled, committing each record's offset only
// after handle returns nil. A handler error is replayed in place (see [Consumer.replay]); Run returns nil on
// a clean ctx-driven stop and a non-nil error only on a fetch or commit fault. Run owns the poll loop; call
// it from a single supervised goroutine.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	for ctx.Err() == nil {
		krs, err := c.next(ctx)
		if err != nil {
			return err
		}

		for attempt := 0; len(krs) > 0; attempt++ {
			failed, procErr := len(krs), error(nil)
			for i, kr := range krs {
				if err := handle(ctx, toRecord(kr)); err != nil {
					failed, procErr = i, err
					break
				}
			}
			// Commit the successfully-handled prefix in one request rather than one broker round-trip per
			// record — at the 8000 msg/s target a per-record commit RTT would dominate the consume loop.
			// A groupless tail reader has nothing to commit to.
			if failed > 0 && c.group != "" {
				if err := c.commit(ctx, krs[:failed]...); err != nil {
					if ctx.Err() != nil {
						// The detached retry inside commit failed too: the broker is unreachable, not merely
						// cancelled. Nothing more to try on a pod that is going away — the records stay
						// uncommitted and are redelivered, which is the at-least-once contract doing its job.
						c.held = krs[failed:]
						return nil
					}
					return fmt.Errorf("kafka: commit in group %s: %w", c.group, err)
				}
			}
			if procErr != nil && !c.replay(ctx, attempt, krs[failed], procErr) {
				c.held = krs[failed:]
				return nil
			}
			krs = krs[failed:]
		}
	}
	return nil
}

// next returns the records to handle: those a cancelled call left uncommitted, before any new poll.
//
// The fetch cursor is already past them, so a call that polled first would commit past them. connector-pool
// calls RunBatch again on the same consumer after every bind drop or reconfiguration, which cancels it.
// Records a poll returns as ctx ends are held the same way.
func (c *Consumer) next(ctx context.Context) ([]*kgo.Record, error) {
	if held := c.held; len(held) > 0 {
		c.held = nil
		return held, nil
	}
	fetches := c.cl.PollFetches(ctx)
	if ctx.Err() != nil {
		c.held = fetches.Records()
		return nil, nil
	}
	if err := firstFetchError(fetches); err != nil {
		return nil, fmt.Errorf("kafka: fetch in group %s: %w", c.group, err)
	}
	return fetches.Records(), nil
}

// replay waits before a failed record is handled again, and reports false when ctx ended the wait.
//
// A handler error is transient by contract, and it used to end Run: the supervisor then took the whole process
// down, and Kubernetes' restart backoff, up to five minutes, decided when the backlog moved again (step-285).
// Replaying in place keeps the group membership, so no partition moves and nothing is republished by a new
// owner. It never re-polls first: the fetch cursor is already past the failed records, and a later commit
// would skip them.
func (c *Consumer) replay(ctx context.Context, attempt int, failed *kgo.Record, err error) bool {
	if ctx.Err() != nil {
		return false // a drain, not a fault: nothing to warn about
	}
	delay := replayDelay(attempt)
	slog.Default().WarnContext(ctx, "kafka: handler failed, replaying in place",
		"group", c.group, "topic", failed.Topic, "partition", failed.Partition, "offset", failed.Offset,
		"attempt", attempt+1, "delay", delay, "err", err)
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// replayDelay doubles from one second to a 30-second ceiling, with ±20 % jitter so replicas failing on the
// same dependency do not retry in lockstep.
func replayDelay(attempt int) time.Duration {
	d := min(time.Second<<min(attempt, 5), 30*time.Second)
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64())) //nolint:gosec // G404: jitter, not a secret.
}

// BatchHandler processes a whole poll batch and reports, per record aligned by index, whether it was
// handled (nil) or hit a transient fault (non-nil). It lets a consumer fan a batch out across workers
// (the connector pool shards it across parallel binds, step-124) instead of one-at-a-time. The contract
// preserves at-least-once and ordering: the handler MUST fail a record and every LATER record that
// shares its ordering group, so a committed offset never skips unprocessed work. len(results) must equal
// len(recs).
type BatchHandler func(ctx context.Context, recs []Record) []error

// RunBatch polls and processes records a batch at a time, committing — independently per partition — the
// contiguous run of successfully-handled records up to that partition's first failure, then replaying what
// it could not commit in place (see [Consumer.replay]) before polling again. It returns nil on a clean
// ctx-driven stop and a non-nil error on a fetch or commit fault, or when the handler breaks its contract.
// Like Run, call it from a single supervised goroutine.
func (c *Consumer) RunBatch(ctx context.Context, handle BatchHandler) error {
	for ctx.Err() == nil {
		krs, err := c.next(ctx)
		if err != nil {
			return err
		}

		for attempt := 0; len(krs) > 0; attempt++ {
			recs := make([]Record, len(krs))
			for i, kr := range krs {
				recs[i] = toRecord(kr)
			}
			results := handle(ctx, recs)

			// Commit each partition up to (but not including) its first failed record. Offsets only compare
			// within a partition, so a global prefix would be wrong: a failure in one partition must not hold
			// back a fully-handled sibling partition, and a success AFTER a failure in the SAME partition must
			// never be committed (it would skip the gap). krs is in per-partition offset order.
			commit, pending, err := committablePrefix(krs, results)
			if err != nil {
				// The handler broke its contract. Fail closed: commit nothing and restart, so the batch is
				// redelivered and reprocessed rather than half-committed on a verdict we cannot read.
				return fmt.Errorf("kafka: batch handle in group %s: %w", c.group, err)
			}
			if len(commit) > 0 && c.group != "" {
				if err := c.commit(ctx, commit...); err != nil {
					if ctx.Err() != nil {
						// See Run: the detached retry failed too, so the broker is gone, not just the context.
						c.held = pending
						return nil
					}
					return fmt.Errorf("kafka: commit in group %s: %w", c.group, err)
				}
			}
			if i := firstFailure(results); i >= 0 && !c.replay(ctx, attempt, krs[i], results[i]) {
				c.held = pending
				return nil
			}
			krs = pending
		}
	}
	return nil
}

// PartitionKey identifies a Kafka partition across topics (a consumer may subscribe to several).
//
// It is the unit offsets are compared within — see [committablePrefix] — and therefore the ordering group
// a [BatchHandler] must halt as a whole. A handler that groups its work by anything else has to keep that
// grouping aligned with this one by hand; grouping by PartitionKey makes the two the same thing, so they
// cannot silently diverge (step-201d D11).
type PartitionKey struct {
	Topic     string
	Partition int32
}

// PartitionKey returns the record's partition identity: its ordering group.
func (r Record) PartitionKey() PartitionKey {
	return PartitionKey{Topic: r.Topic, Partition: r.Partition}
}

// committablePrefix splits a handled batch into the records safe to commit — those handled successfully
// whose offset precedes their partition's first failed offset — and everything else, still in batch order,
// which must be replayed. results is aligned with krs by index.
//
// pending is the complement of commit, never just the failures: a success above a failure in its partition
// is not committed, and the fetch cursor is already past it, so a record left out of both would be skipped
// by the next commit.
//
// A results slice that does not line up with krs is a handler bug, and it is reported rather than
// indexed through: this runs inside a data-plane pod's consume loop, where an index-out-of-range takes
// the process down mid-batch. [BatchHandler] states the requirement; this is what enforces it.
func committablePrefix(krs []*kgo.Record, results []error) (commit, pending []*kgo.Record, err error) {
	if len(results) != len(krs) {
		return nil, nil, fmt.Errorf("batch handler returned %d results for %d records", len(results), len(krs))
	}
	firstFail := make(map[PartitionKey]int64)
	for i, kr := range krs {
		if results[i] == nil {
			continue
		}
		pk := PartitionKey{Topic: kr.Topic, Partition: kr.Partition}
		if off, ok := firstFail[pk]; !ok || kr.Offset < off {
			firstFail[pk] = kr.Offset
		}
	}
	for i, kr := range krs {
		off, failed := firstFail[PartitionKey{Topic: kr.Topic, Partition: kr.Partition}]
		if results[i] != nil || (failed && kr.Offset > off) {
			pending = append(pending, kr)
			continue
		}
		commit = append(commit, kr)
	}
	return commit, pending, nil
}

// firstFailure returns the index of the first non-nil error, or -1.
func firstFailure(errs []error) int {
	for i, e := range errs {
		if e != nil {
			return i
		}
	}
	return -1
}

// Ping reports whether the brokers are reachable.
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// ReadyCheck adapts the consumer to a readiness probe.
func (c *Consumer) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, c.cl.Ping)
}

// StartsFromEnd reports whether a group with no committed offset begins at the end of the topic,
// skipping whatever was produced before it first joined. It exists so a wiring choice that is a
// durability property can be asserted instead of merely commented.
func (c *Consumer) StartsFromEnd() bool { return c.fromEnd }

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
	byPartition, err := c.LagByPartition(ctx)
	if err != nil {
		return nil, err
	}
	return sumLag(byPartition), nil
}

// LagByPartition reports the same backlog as [Consumer.Lag], kept split per partition.
//
// The split is what tells a flat total apart from a balanced one. A group whose keys all hash to one
// partition is serialised no matter how many partitions the topic has or how many pods join the group,
// and the totals look identical either way — so a measurement that concludes anything about parallelism
// has to read this, not the sum (step-201d, D5/M4).
//
// Same cost and same refusals as Lag: one broker round-trip, and a partition whose lag cannot be computed
// fails the whole call rather than being dropped from a plausible-looking total.
func (c *Consumer) LagByPartition(ctx context.Context) (map[string]map[int32]int64, error) {
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
	return splitLag(c.group, described.Lag)
}

// splitLag reduces a described group's lag to topic -> partition -> records, refusing any topic it cannot
// report honestly.
//
// Both refusals serve one rule: a backlog gauge may hold a stale value, but it may never publish a small
// number that reads as "we are caught up".
//
//   - A partition whose lag could not be computed reports -1 WITH an error. Skipping it silently would
//     publish a plausible-looking partial total.
//   - A topic with NO partitions at all sums to a perfect 0, which is the same lie with no error to catch
//     it. kadm seeds its map from the topics in the members' Join and leaves the partitions empty when the
//     topic is missing from endOffsets — a shard error, or a topic that does not exist (step-201c, D20).
//
// A negative lag is clamped to zero, not summed: kadm reports -1 for "unknown", and letting it through
// would subtract from a sibling partition's real backlog.
func splitLag(group string, lag kadm.GroupLag) (map[string]map[int32]int64, error) {
	out := make(map[string]map[int32]int64, len(lag))
	for topic, partitions := range lag {
		if len(partitions) == 0 {
			return nil, fmt.Errorf("kafka: lag for group %s, topic %s: no partitions described", group, topic)
		}
		byPartition := make(map[int32]int64, len(partitions))
		for _, p := range partitions {
			if p.Err != nil {
				return nil, fmt.Errorf("kafka: lag for group %s, topic %s partition %d: %w",
					group, topic, p.Partition, p.Err)
			}
			byPartition[p.Partition] = max(p.Lag, 0)
		}
		out[topic] = byPartition
	}
	return out, nil
}

// sumLag totals splitLag's output per topic. It cannot fail: splitLag already refused everything that
// would make a total dishonest, which is precisely why the two are separate — the refusals belong to the
// reading, the arithmetic to the reporting.
func sumLag(byPartition map[string]map[int32]int64) map[string]int64 {
	out := make(map[string]int64, len(byPartition))
	for topic, partitions := range byPartition {
		var total int64
		for _, lag := range partitions {
			total += lag
		}
		out[topic] = total
	}
	return out
}

// NewTailReader consumes a topic from the END of the log with NO consumer group.
//
// For a live feed there is nothing to resume: a group would commit offsets, so a restart would replay the
// retained backlog into clients that only want what is happening now — and a per-instance group name would
// instead accumulate abandoned groups on the broker. Groupless also means every replica sees every record,
// which is what a fan-out needs.
//
// It takes the same fetch levers as a group consumer, so raising KAFKA_FETCH_MIN_BYTES or
// KAFKA_FETCH_MAX_WAIT in a service that also tails a topic delays its live frames by that much. The
// process-wide prefix leaves no room to separate the two; the levers are meant for the CDR path, and a
// service running both should be tuned with that in mind.
func NewTailReader(cfg config.Kafka, topics ...string) (*Consumer, error) {
	shared, err := consumerOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: new tail reader: %w", err)
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}, shared...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new tail reader: %w", err)
	}
	return &Consumer{cl: cl}, nil
}
