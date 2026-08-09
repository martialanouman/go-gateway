//go:build loadref

package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/test/load/redpandametrics"
)

// TestBrokerExpositionIsReadable is the health check for the broker reader itself.
//
// The ceiling bench degrades when a scrape fails — a broker reading must not destroy a throughput
// measurement that came back fine — so without this test a reader that silently stopped working would
// print "broker unreadable" on every row and fail nothing. This is where it fails.
//
// It also pins the two things a wrong endpoint would break invisibly: that the address really is the
// admin API and not the Kafka port, and that the exposition served there carries the produce family
// the attribution is read from. A scrape of /metrics instead of /public_metrics parses perfectly and
// yields none of it.
func TestBrokerExpositionIsReadable(t *testing.T) {
	admin := kafkatest.AdminAPI(t)
	if !strings.Contains(admin, "http") {
		t.Fatalf("AdminAPI must be an http origin, got %q — the Kafka seed broker is not one", admin)
	}

	client, err := redpandametrics.NewClient(admin)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if !strings.HasSuffix(client.URL(), "/public_metrics") {
		t.Fatalf("the reader must target the curated exposition, got %s", client.URL())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	before, err := client.Scrape(ctx)
	if err != nil {
		t.Fatalf("first scrape: %v", err)
	}

	// Real traffic between the readings: a window in which the broker did nothing cannot tell a working
	// reader from a broken one, which is the same trap the fixture avoids by being captured under load.
	producer, err := kafka.NewProducer(refKafkaConfig(kafkatest.Brokers(t)))
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(producer.Close)
	for range 200 {
		rec, err := pipeline.EncodeRouted(routedBench())
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := producer.Produce(ctx, rec); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Rate refuses a window under a second, and it is right to: below that the elapsed time is scrape
	// jitter. The bench's own windows are ten seconds, so this wait is an artefact of the test, not a
	// property of the instrument.
	time.Sleep(redpandametricsMinWindow)

	after, err := client.Scrape(ctx)
	if err != nil {
		t.Fatalf("second scrape: %v", err)
	}

	rep, err := redpandametrics.Rate(before, after)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	if len(rep.Handlers) == 0 {
		t.Fatal("200 produces went through the broker and the reader saw no request served: " +
			"the exposition is not the one this reader parses")
	}
	var producedSeen bool
	for _, h := range rep.Handlers {
		if h.API == "produce" {
			producedSeen = true
		}
	}
	if !producedSeen {
		t.Errorf("the produce handler must be visible after 200 produces, got %+v", rep.Handlers)
	}
	if rep.Shards == 0 {
		t.Error("a core figure with no shard count behind it cannot be read")
	}
	t.Logf("broker reader: %s", rep.Render())
}

// redpandametricsMinWindow mirrors the reader's own floor. It is spelled out here rather than exported
// from the package: exporting it would invite a caller to build a window that only just clears it.
const redpandametricsMinWindow = 1100 * time.Millisecond
