//go:build loadref

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/test/load/redpandametrics"
)

const (
	// ceilingJoinDeadline bounds the wait for the consumer group to join and deliver its first record.
	// Past it the palier has not started, and a rate of zero would be reported as a measurement.
	ceilingJoinDeadline = 30 * time.Second

	// ceilingSamplingAttempts bounds the rejection sampling below. Covering P partitions is a coupon
	// collector — ~54 draws for 16 — so this is orders of magnitude of headroom, there to turn an
	// impossible hash into a failed test rather than a hung one.
	ceilingSamplingAttempts = 100000

	// prefillBatchMaxBytes is the broker's ceiling, not production's. See prefill's godoc: the 16MiB
	// production setting is unreachable under ProduceSync and rejected under async producing.
	prefillBatchMaxBytes = 1 << 20
)

// measureRouterCeiling runs one palier: prefill a private topic of `partitions` partitions, run the
// router alone over it for `hold`, and report the rate with the facts needed to read it.
func measureRouterCeiling(t *testing.T, brokers []string, partitions, records int, hold time.Duration) {
	t.Helper()

	topic := newCeilingTopic(t, brokers, "inbound", partitions)
	accounts := partitionAccounts(topic, partitions)
	prefillRate := prefill(t, brokers, topic, records, func(i int) (kafka.Record, error) {
		return pipeline.EncodeInbound(inboundBench(accounts[i%len(accounts)]))
	})

	if err := prefillBalance(endOffsets(t, brokers, topic), partitions); err != nil {
		t.Fatalf("%d partitions: %v", partitions, err)
	}

	cfg := refKafkaConfig(brokers)
	// A group id per palier: a reused one carries committed offsets and would consume none of the
	// prefill, measuring an empty topic at a plausible-looking zero.
	cons := newConsumer(t, cfg, "loadref-router-ceiling-"+uuid.NewString(), topic)
	inner, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(inner.Close)

	prod := newCountingProducer(inner)
	counted := &countingConsumer{inner: cons}
	cdr := &countingCDR{}
	tracer := observability.Tracer(noop.NewTracerProvider(), "loadref-ceiling")

	rtr := router.New(router.Deps{
		Consumer: counted,
		Producer: prod,
		Metrics:  metrics.NewCatalog(),
		Pipeline: pipeline.New(pipeline.Deps{
			Tracer:    tracer,
			Resolver:  ceilResolver{conn: uuid.New()},
			SenderIDs: ceilSenderIDs{},
			OptOut:    ceilOptOut{},
			Antispam:  ceilAntispam{},
		}),
		CDR:    cdr,
		Tracer: tracer,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- rtr.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-runErr; err != nil {
			t.Errorf("%d partitions: router run: %v", partitions, err)
		}
	}()

	waitUntilConsuming(t, prod, partitions)

	// The clock opens here, not at Run: see the join argument in TestRouterConsumeCeiling's godoc.
	first := lagOf(t, cons, topic)
	brokerBefore, brokerErr := scrapeBroker(t)
	start := time.Now()
	base, baseNanos, baseBuckets := prod.snapshot()
	baseBatches, baseRecords, baseLanes := counted.snapshot()

	select {
	case err := <-runErr:
		runErr <- err // give the deferred read something to drain
		t.Fatalf("%d partitions: the router stopped inside the window: %v", partitions, err)
	case <-time.After(hold):
	}

	elapsed := time.Since(start)
	produced, nanos, buckets := prod.snapshot()
	done := produced - base
	brokerAfter, afterErr := scrapeBroker(t)
	// Read while the group is still alive: kadm seeds a group's lag from its members, so a reading taken
	// after cancel() describes a group that has left.
	last := lagOf(t, cons, topic)
	batches, recs, lanes := counted.snapshot()

	// The rate is computed before the guards because one of them now judges it, and the "moved nothing"
	// check comes first of all: a dead palier has its own diagnosis, and letting it fail on sourcesAgree
	// would answer a question about disagreeing sources when the answer is that there was no traffic.
	rate := float64(done) / elapsed.Seconds()
	if rate == 0 {
		t.Fatalf("the router moved nothing at %d partitions", partitions)
	}

	if err := backlogHeld(first, last); err != nil {
		t.Fatalf("%d partitions: %v", partitions, err)
	}
	if err := sourcesAgree(rate, first, last, elapsed); err != nil {
		t.Fatalf("%d partitions: %v", partitions, err)
	}
	if n := cdr.n.Load(); n != 0 {
		t.Fatalf("%d partitions: %d messages were rejected: this palier did not measure the path it reports",
			partitions, n)
	}

	t.Logf("router alone: %2d partitions -> %8.0f msg/s · %s · %s · prefill %.0f rec/s",
		partitions, rate,
		crossCheck(rate, first, last, elapsed),
		laneShape(batches-baseBatches, recs-baseRecords, lanes-baseLanes, partitions),
		prefillRate)
	t.Logf("             %2d partitions    %s", partitions, brokerReport(brokerBefore, brokerAfter, brokerErr, afterErr))
	t.Logf("             %2d partitions    %s", partitions,
		stageLatency(refProduceStage, refProduceExcludes, done, nanos-baseNanos, deltaBuckets(baseBuckets, buckets), elapsed, done))

}

// scrapeBroker reads the test broker's own exposition (step-201e D2).
//
// It DEGRADES rather than fails: the broker reading answers "is the ceiling Kafka's?", and a scrape
// that did not come back must not destroy a throughput measurement that did. The instrument's own
// health is asserted by TestBrokerExpositionIsReadable instead, so a silently broken reader still
// fails something.
func scrapeBroker(t *testing.T) (redpandametrics.Snapshot, error) {
	t.Helper()
	c, err := redpandametrics.NewClient(kafkatest.AdminAPI(t))
	if err != nil {
		return redpandametrics.Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Scrape(ctx)
}

// brokerReport renders what the broker did during the window, or why it could not be read.
func brokerReport(before, after redpandametrics.Snapshot, beforeErr, afterErr error) string {
	if err := errors.Join(beforeErr, afterErr); err != nil {
		return fmt.Sprintf("broker unreadable: %v", err)
	}
	rep, err := redpandametrics.Rate(before, after)
	if err != nil {
		return fmt.Sprintf("broker unreadable: %v", err)
	}
	return rep.Render()
}

// newCeilingTopic creates a private topic of the given shape with exactly `partitions` partitions. The
// shape names the record type the caller will prefill it with ("inbound", "routed"), so a topic left
// behind by a crashed run says what it held.
//
// The sweep does NOT move KAFKATEST_PARTITIONS: that is read once with the shared container, so it
// would be one container per point, and it widens mt.routed too — varying the output alongside the
// input. Here the bench's lane count is the only thing that changes between rows.
func newCeilingTopic(t *testing.T, brokers []string, shape string, partitions int) string {
	t.Helper()
	topic := fmt.Sprintf("loadref.%s.p%d.%s", shape, partitions, uuid.NewString()[:8])

	adm, closeAdm := adminClient(t, brokers)
	defer closeAdm()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := adm.CreateTopics(ctx, int32(partitions), 1, nil, topic)
	if err != nil {
		t.Fatalf("create %s: %v", topic, err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			t.Fatalf("create %s: %v", r.Topic, r.Err)
		}
	}

	// Each palier writes ~100MB. Left behind, five of them fill the single-node broker's volume, which
	// fails the run somewhere far away from the cause.
	t.Cleanup(func() {
		adm, closeAdm := adminClient(t, brokers)
		defer closeAdm()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := adm.DeleteTopics(ctx, topic); err != nil {
			t.Logf("delete %s: %v", topic, err)
		}
	})
	return topic
}

func adminClient(t *testing.T, brokers []string) (*kadm.Client, func()) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	return kadm.NewClient(cl), cl.Close
}

// partitionAccounts picks one account id per partition, by rejection sampling against the partitioner.
//
// This is an OPTIMISATION, not a guarantee, and the distinction matters. kafka.NewProducer configures
// no RecordPartitioner, so franz-go's default decides — today UniformBytesPartitioner, whose keyed
// branch is KafkaHasher(murmur2), the same hash StickyKeyPartitioner wraps. A version bump could move
// that without a single test going red, which is why the guard that actually protects the measurement
// is prefillBalance: it reads the end offsets and observes where the records landed. If this prediction
// ever goes wrong, the run fails loudly instead of quietly drawing a curve against idle lanes.
//
// Random ids would spread balls-into-bins: at 16 partitions one routinely takes twice another's share,
// and the light one drains mid-window (step-201d, reference_test.go's per-partition backlog).
func partitionAccounts(topic string, partitions int) []uuid.UUID {
	tp := kgo.StickyKeyPartitioner(nil).ForTopic(topic)
	out := make([]uuid.UUID, partitions)
	filled := make([]bool, partitions)
	left := partitions
	for attempt := 0; left > 0; attempt++ {
		if attempt >= ceilingSamplingAttempts {
			panic(fmt.Sprintf("partitionAccounts: %d partitions uncovered after %d draws: the partitioner "+
				"is not spreading keys as assumed", left, attempt))
		}
		id := uuid.New()
		key := id[:]
		p := tp.Partition(&kgo.Record{Topic: topic, Key: key}, partitions)
		if p < 0 || p >= partitions || filled[p] {
			continue
		}
		out[p], filled[p] = id, true
		left--
	}
	return out
}

// prefill writes `records` records built by `encode` to topic, and returns the rate it achieved in
// records per second.
//
// The record shape is the caller's: mt.inbound for the router bench, mt.routed for the pool's. What
// this function owns is the writing — the client, the durability and the failure accounting — because
// those are what every backlog needs and none of them belong to a record type.
//
// It uses a raw kgo client rather than kafka.Producer because Producer.Produce is ProduceSync with
// acks=all: at the ~2 000/s that path has been measured at, 150 000 records would cost 75 seconds per
// palier — twenty-five times the window. This is legitimate because the prefill is FIXTURE GENERATION:
// the fidelity that matters is the record's (pipeline.EncodeInbound, keyed by account, a GSM-7 body)
// and its placement, not the client that writes it.
//
// It departs from kafka.NewProducer on one option, and the departure is forced. Production sets
// ProducerBatchMaxBytes to 16MiB, which it never reaches: ProduceSync sends one record and waits, so a
// batch holds one record. Producing asynchronously fills batches for real, and the broker rejects them
// past its own limit — MESSAGE_TOO_LARGE against Redpanda's 1MiB default. So the batch ceiling here is
// the broker's, not production's; acks=all is kept, because a prefill that is not durable is not a
// backlog.
//
// The returned rate is not decoration. It is what the broker absorbs WITH batching, against what the
// router achieves one synchronous acks=all record at a time — a free upper bound on the broker before
// step-201e D2 scrapes it.
func prefill(t *testing.T, brokers []string, topic string, records int, encode func(i int) (kafka.Record, error)) float64 {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(prefillBatchMaxBytes),
	)
	if err != nil {
		t.Fatalf("prefill client: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()
	var failed atomic.Uint64
	var once sync.Once
	var firstErr error

	start := time.Now()
	for i := range records {
		rec, err := encode(i)
		if err != nil {
			t.Fatalf("encode record %d: %v", i, err)
		}
		// The encoders hard-code the production topic. Only the destination is overridden; the key, the
		// value and the headers stay exactly what production publishes.
		kr := &kgo.Record{Topic: topic, Key: rec.Key, Value: rec.Value}
		for _, h := range rec.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
		}
		cl.Produce(ctx, kr, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
				once.Do(func() { firstErr = err })
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("prefill flush: %v", err)
	}
	// A partial prefill is a backlog of unknown size, which is the one thing this measurement may not
	// have: every guard downstream assumes the number written is the number asked for.
	if n := failed.Load(); n > 0 {
		t.Fatalf("prefill: %d of %d records failed, first: %v", n, records, firstErr)
	}
	return float64(records) / time.Since(start).Seconds()
}

// endOffsets reports how many records each partition of topic holds.
func endOffsets(t *testing.T, brokers []string, topic string) map[int32]int64 {
	t.Helper()
	adm, closeAdm := adminClient(t, brokers)
	defer closeAdm()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listed, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("end offsets for %s: %v", topic, err)
	}
	if err := listed.Error(); err != nil {
		t.Fatalf("end offsets for %s: %v", topic, err)
	}
	out := make(map[int32]int64)
	listed.Each(func(o kadm.ListedOffset) { out[o.Partition] = o.Offset })
	return out
}

// lagOf reports the router group's remaining backlog per partition of topic.
func lagOf(t *testing.T, cons *kafka.Consumer, topic string) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	byTopic, err := cons.LagByPartition(ctx)
	if err != nil {
		t.Fatalf("lag: %v", err)
	}
	return byTopic[topic]
}

// waitUntilConsuming blocks until the router has published its first record, so the measured window
// excludes the group's join. It fails rather than returning a zero rate: "the group never joined" and
// "the router is infinitely slow" are opposite findings, and only one of them is about the router.
func waitUntilConsuming(t *testing.T, prod *countingProducer, partitions int) {
	t.Helper()
	deadline := time.Now().Add(ceilingJoinDeadline)
	for time.Now().Before(deadline) {
		if prod.ok.Load() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%d partitions: nothing was consumed in %v — the group did not join, or the topic is empty",
		partitions, ceilingJoinDeadline)
}

// countingProducer counts acknowledged produces. It decorates rather than replaces, so the measured
// path is the production one; router.Producer being a one-method interface declared consumer-side is
// what makes that free (step-201e: no hot-path change).
//
// It counts SUCCESSES only. At the close of the window cancel() fails whatever is in flight, and
// counting those would put a tail of cancellations into the rate — and, since step-201e D3, into the
// latency histogram, where a tail of cancelled produces would read as a broker stalling.
//
// The histogram is buckets of atomic counters, never a slice of samples: a palier at 16 lanes produces
// ~280 000 records in ten seconds from as many goroutines as there are partitions, and a shared slice
// would need a mutex on the very path being measured. Buckets are per-class — one atomic add per
// observation, not one per bucket above the value as a Prometheus histogram does.
type countingProducer struct {
	inner   router.Producer
	ok      atomic.Uint64
	nanos   atomic.Uint64
	buckets []atomic.Uint64
}

func newCountingProducer(inner router.Producer) *countingProducer {
	return &countingProducer{inner: inner, buckets: make([]atomic.Uint64, len(produceBounds)+1)}
}

func (p *countingProducer) Produce(ctx context.Context, rec kafka.Record) error {
	started := time.Now()
	if err := p.inner.Produce(ctx, rec); err != nil {
		return err
	}
	took := time.Since(started)
	p.ok.Add(1)
	p.nanos.Add(uint64(took))
	p.buckets[produceBucket(took)].Add(1)
	return nil
}

// snapshot reads the three counters together, on the pattern of countingConsumer.snapshot: two reads
// bracket the window and the call site subtracts.
func (p *countingProducer) snapshot() (count, nanos uint64, buckets []uint64) {
	buckets = make([]uint64, len(p.buckets))
	for i := range p.buckets {
		buckets[i] = p.buckets[i].Load()
	}
	return p.ok.Load(), p.nanos.Load(), buckets
}

// countingConsumer records the SHAPE of each poll batch: how many records, and across how many
// partitions.
//
// The second number is the one the sweep needs. handleBatch opens one goroutine per partition present
// in the batch, and the batch is bounded by FetchMaxPartitionBytes and FetchMaxBytes rather than by the
// topic — so a 16-partition topic whose batches span three partitions is a three-lane measurement, and
// labelling that row "16" asserts something nobody measured.
type countingConsumer struct {
	inner   router.Consumer
	batches atomic.Uint64
	records atomic.Uint64
	lanes   atomic.Uint64
}

func (c *countingConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	return c.inner.RunBatch(ctx, func(ctx context.Context, recs []kafka.Record) []error {
		seen := make(map[kafka.PartitionKey]struct{}, len(recs))
		for _, r := range recs {
			seen[r.PartitionKey()] = struct{}{}
		}
		c.batches.Add(1)
		c.records.Add(uint64(len(recs)))
		c.lanes.Add(uint64(len(seen)))
		return handle(ctx, recs)
	})
}

func (c *countingConsumer) snapshot() (batches, records, lanes uint64) {
	return c.batches.Load(), c.records.Load(), c.lanes.Load()
}

// countingCDR counts rejections without keeping them. Retaining the rows would grow an unbounded slice
// behind one mutex, and that mutex sits on every lane — the palier would be measuring its own stub.
// Any non-zero count fails the palier: a rejection means the run is not exercising the path it reports.
type countingCDR struct{ n atomic.Uint64 }

func (c *countingCDR) Insert(context.Context, clickhouse.CDRRow) error {
	c.n.Add(1)
	return nil
}

// The four stages below are stubbed. Their cost is bounded by a figure already measured rather than
// assumed: Pipeline.Process is 2.3% of the per-message budget and these four are ~10% of the pipeline
// (step-201d D8), so under 0.3% of the budget. e164 normalisation and segmentation — 74% of the
// pipeline — run for real, and the reference run this curve is compared against has the rate-limit,
// credit and Redis anti-spam stages in pass-through too (test/load/README.md, step-201d D4).
//
// They are copied from internal/router's tests rather than shared: those are unexported in package
// router_test, and exporting a stub from internal/router would be production code existing only for a
// bench.
type ceilResolver struct{ conn uuid.UUID }

func (r ceilResolver) Resolve(context.Context, pipeline.RouteRequest) (pipeline.Route, error) {
	return pipeline.Route{ConnectorID: r.conn}, nil
}

type ceilSenderIDs struct{}

func (ceilSenderIDs) Authorize(context.Context, uuid.UUID, uuid.UUID, string) error { return nil }

type ceilOptOut struct{}

func (ceilOptOut) IsOptedOut(context.Context, uuid.UUID, uuid.UUID, string, string) (bool, error) {
	return false, nil
}

type ceilAntispam struct{}

func (ceilAntispam) Evaluate(context.Context, uuid.UUID, uuid.UUID, string, string, []byte) (cp.AntispamAction, error) {
	return "", nil
}

// inboundBench is the record the REST API and the SMPP server publish: one GSM-7 segment of the length
// the injector sends, keyed by the account so the prefill lands where partitionAccounts intended. A
// synthetic payload would segment differently and price a different message.
func inboundBench(account uuid.UUID) pipeline.InboundMT {
	const body = "Your one time code is 424242. It expires in ten minutes. Do not share it with anyone, our staff will never ask you for it."
	return pipeline.InboundMT{
		MessageID:   uuid.New(),
		TraceID:     uuid.New(),
		AccountID:   account,
		CustomerID:  uuid.New(),
		From:        refSenderID,
		To:          "2250700000000",
		Body:        msg.NewBodyString(body),
		Encoding:    "auto",
		SubmittedAt: time.Now().UTC(),
	}
}
