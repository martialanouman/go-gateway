package outcome_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"log/slog"

	configpkg "github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/outcome"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// A projector that joins AFTER outcomes were produced must still write them. This is the deploy
// ordering nothing in the repo constrains: connector-pool-svc may roll out before router-svc, and every
// message it sends in between has its offset committed on mt.routed while its outcome waits on
// mt.outcome. A group that starts at the LATEST offset skips exactly that window — for ever, and in
// silence: those messages read "accepted" until they are purged, and billing.Reaper, which settles
// orphan reservations against the recorded CDR outcome, holds their credit for good.
//
// The write-storm argument that justifies starting at the end elsewhere has no object here: mt.outcome
// is a new topic, so on the deploy that introduces it there is nothing to replay.
func TestProjectorReadsOutcomesProducedBeforeItJoined(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	cfg := configpkg.Kafka{Brokers: brokers, Timeout: 5 * time.Second}

	// Produce first, join second — the ordering under test.
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(producer.Close)

	env := enrouteEvent()
	rec, err := pipeline.EncodeOutcome(env)
	if err != nil {
		t.Fatalf("encode outcome: %v", err)
	}
	if err := producer.Produce(context.Background(), rec); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := kafka.NewConsumer(cfg, "outcome-latejoin-"+uuid.NewString(), kafka.TopicMTOutcome)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	sink := &lockedCDR{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- outcome.NewProjector(consumer, sink, slog.New(slog.DiscardHandler)).Run(ctx) }()

	// The broker is shared across runs and AtStart replays the whole topic, so the sink also holds
	// outcomes from earlier runs: look for THIS message rather than expecting a count.
	found := func() (clickhouse.CDRRow, bool) {
		for _, r := range sink.rows() {
			if r.MessageID == env.MessageID {
				return r, true
			}
		}
		return clickhouse.CDRRow{}, false
	}

	deadline := time.After(15 * time.Second)
	for {
		if _, ok := found(); ok {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("the projector never wrote the outcome produced before it joined: a group starting at " +
				"the LATEST offset skips it for ever, and the message reads accepted until it is purged")
		case <-time.After(200 * time.Millisecond):
		}
	}
	cancel()
	<-done

	row, ok := found()
	if !ok {
		t.Fatal("the outcome vanished between the poll and the assertion")
	}
	if row.Status != clickhouse.StatusEnroute {
		t.Errorf("status = %v, want %v", row.Status, clickhouse.StatusEnroute)
	}
}

// lockedCDR is fakeCDR with a mutex. The shared fake is unsynchronised on purpose — every other test
// in this package drives the projector on the calling goroutine — but this one runs it concurrently
// and polls the sink, so it needs its own.
type lockedCDR struct {
	mu     sync.Mutex
	stored []clickhouse.CDRRow
}

func (c *lockedCDR) InsertBatch(_ context.Context, rows []clickhouse.CDRRow) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stored = append(c.stored, rows...)
	return nil
}

func (c *lockedCDR) rows() []clickhouse.CDRRow {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]clickhouse.CDRRow(nil), c.stored...)
}
