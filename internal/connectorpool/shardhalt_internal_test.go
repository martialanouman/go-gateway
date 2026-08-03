package connectorpool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// This is an INTERNAL test because errShardHalted is what it asserts on, and an error that reads
// "halted" from the outside is worth nothing: the property is that the halted records were never
// SUBMITTED. Until step-201c the sentinel had no test at all, and the step publishes from inside the
// shard loop, so the halt is now on the path of a Kafka fault.

// haltCDR is a CDR writer that records nothing: the send path no longer writes it (D1), and the halt
// scenario never reaches the cancel/reroute sites that still do.
type haltCDR struct{}

func (haltCDR) Insert(context.Context, clickhouse.CDRRow) error { return nil }

// haltProducer fails the mt.outcome publish of ONE chosen message and accepts every other. Choosing by
// message id rather than by arrival order is what makes the fixture deterministic: the shards run
// concurrently, so "fail the first produce" would fail whichever shard won the race.
type haltProducer struct {
	target uuid.UUID
	err    error
}

func (p *haltProducer) Produce(_ context.Context, rec kafka.Record) error {
	if rec.Topic != kafka.TopicMTOutcome {
		return nil
	}
	out, err := pipeline.DecodeOutcome(rec)
	if err != nil || out.MessageID != p.target {
		return nil
	}
	return p.err
}

// oneBatchConsumer hands every record as ONE poll batch and keeps the per-record results.
type oneBatchConsumer struct {
	records []kafka.Record
	results []error
}

func (c *oneBatchConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	c.results = handle(ctx, c.records)
	return nil
}

// idForShard mints a message id that hashes to shard want in a pool of n binds — the batch handler's
// own shardIndex, so the fixture cannot disagree with the code about where a record lands.
func idForShard(t *testing.T, want, n int) uuid.UUID {
	t.Helper()
	for i := 0; i < 10_000; i++ {
		id := uuid.New()
		if shardIndex(id[:], n) == want {
			return id
		}
	}
	t.Fatalf("no message id hashed to shard %d of %d", want, n)
	return uuid.Nil
}

// TestShardHaltsAfterAFailureAndSparesOtherShards: a record that fails leaves every LATER record of its
// own shard unprocessed and uncommitted — marked errShardHalted, and above all never submitted, so a
// segment can never overtake an earlier one on redelivery (§7.3) — while another shard's records go
// through untouched.
func TestShardHaltsAfterAFailureAndSparesOtherShards(t *testing.T) {
	const poolSize = 2

	var submitted sync.Map // destination address -> struct{}
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(sm smpp.SubmitSM) fakesmsc.Resp {
		submitted.Store(sm.DestinationAddr, struct{}{})
		return fakesmsc.OK()
	}})

	// Three records: two on shard 0 (the failing one first, then the one that must be halted) and one on
	// shard 1, which must be unaffected. Distinct destinations so the SMSC's view identifies each.
	dests := []string{"+2250700000001", "+2250700000002", "+2250700000003"}
	shards := []int{0, 0, 1}
	ids := make([]uuid.UUID, len(dests))
	recs := make([]kafka.Record, 0, len(dests))
	for i, dest := range dests {
		ids[i] = idForShard(t, shards[i], poolSize)
		rec, err := pipeline.EncodeRouted(pipeline.RoutedMT{
			MessageID: ids[i], TraceID: uuid.New(),
			AccountID: uuid.New(), CustomerID: uuid.New(),
			From: "GATEWAY", To: dest, Body: msg.NewBodyString("hi"),
			Encoding: "gsm7", ConnectorID: uuid.New(), SegmentSeq: 1, SegmentCount: 1,
			SubmittedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("encode %s: %v", dest, err)
		}
		recs = append(recs, rec)
	}

	produceErr := errors.New("kafka unavailable")
	consumer := &oneBatchConsumer{records: recs}
	svc := New(Deps{
		Consumer: consumer,
		CDR:      haltCDR{},
		Producer: &haltProducer{target: ids[0], err: produceErr},
		Bind: BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
			BindPoolSize: poolSize,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(consumer.results) != len(recs) {
		t.Fatalf("results = %d, want %d", len(consumer.results), len(recs))
	}
	if !errors.Is(consumer.results[0], produceErr) {
		t.Errorf("results[0] = %v, want the produce failure", consumer.results[0])
	}
	if !errors.Is(consumer.results[1], errShardHalted) {
		t.Errorf("results[1] = %v, want errShardHalted (later record of the failed shard)", consumer.results[1])
	}
	if consumer.results[2] != nil {
		t.Errorf("results[2] = %v, want nil: another shard must not be halted", consumer.results[2])
	}

	// The halt is only real if the record never reached the SMSC. Marking it errored while still sending
	// it would redeliver a message that was already submitted — the very duplicate the halt exists to
	// prevent.
	if _, ok := submitted.Load(dests[0]); !ok {
		t.Errorf("%s was never submitted — the fixture never reached the failure it is about", dests[0])
	}
	if _, ok := submitted.Load(dests[1]); ok {
		t.Errorf("%s was submitted: a halted record must not reach the SMSC (§7.3)", dests[1])
	}
	if _, ok := submitted.Load(dests[2]); !ok {
		t.Errorf("%s was not submitted: an unrelated shard must keep going", dests[2])
	}
}
