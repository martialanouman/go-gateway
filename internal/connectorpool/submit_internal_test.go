package connectorpool

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// Characterisation tests written before step-260i split processOne: each pins a branch the split moves
// and that no other test reached (coverage baseline), and each was seen to fall under a mutation.

// runOneBatch runs a pool of one bind over recs against smsc and returns the per-record results.
func runOneBatch(t *testing.T, smsc *fakesmsc.Server, connectorID uuid.UUID, stream StreamEmitter, recs []kafka.Record) []error {
	t.Helper()
	consumer := &oneBatchConsumer{records: recs}
	svc := New(Deps{
		Consumer:    consumer,
		CDR:         haltCDR{},
		Producer:    &haltProducer{},
		ConnectorID: connectorID,
		Stream:      stream,
		Bind: BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
			BindPoolSize: 1,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return consumer.results
}

func routedRecord(t *testing.T, connectorID uuid.UUID, dest string) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeRouted(pipeline.RoutedMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: dest, Body: msg.NewBodyString("hi"), Encoding: "gsm7",
		ConnectorID: connectorID, SegmentSeq: 1, SegmentCount: 1, SubmittedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec
}

// TestUndecodableRecordIsNotCommitted: a record that is not an mt.routed envelope is reported non-nil —
// which RunBatch leaves uncommitted — and never reaches the SMSC.
func TestUndecodableRecordIsNotCommitted(t *testing.T) {
	var submits atomic.Int32
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		submits.Add(1)
		return fakesmsc.OK()
	}})

	results := runOneBatch(t, smsc, uuid.Nil, nil, []kafka.Record{{Topic: kafka.TopicMTRouted, Value: []byte("{")}})

	if len(results) != 1 || results[0] == nil {
		t.Fatalf("results = %v, want one non-nil error: an undecodable record must not be committed", results)
	}
	if n := submits.Load(); n != 0 {
		t.Errorf("the SMSC received %d submit_sm, want 0", n)
	}
}

// TestRetryKeyDistinguishesPartitions: the retry window is keyed by (partition, offset); two partitions
// share every offset value, so a key on the offset alone would merge their windows.
func TestRetryKeyDistinguishesPartitions(t *testing.T) {
	p0 := kafka.Record{Partition: 0, Offset: 5}
	p1 := kafka.Record{Partition: 1, Offset: 5}
	if retryKeyOf(p0) == retryKeyOf(p1) {
		t.Errorf("retryKeyOf(%v) == retryKeyOf(%v): the partition is ignored", p0, p1)
	}
	if retryKeyOf(p0) != retryKeyOf(kafka.Record{Partition: 0, Offset: 5}) {
		t.Errorf("retryKey is not stable for the same record")
	}
}

// recordingEmitter keeps every Add so a test can assert the stream sees each submit outcome.
type recordingEmitter struct {
	mu   sync.Mutex
	adds map[string]float64 // kind + sorted labels -> total
}

func (e *recordingEmitter) key(kind string, labels metricstream.Labels) string {
	return kind + "{connector_id=" + labels["connector_id"] + ",status=" + labels["status"] + ",code=" + labels["code"] + "}"
}

func (e *recordingEmitter) Add(kind string, labels metricstream.Labels, delta float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.adds == nil {
		e.adds = map[string]float64{}
	}
	e.adds[e.key(kind, labels)] += delta
}
func (e *recordingEmitter) Set(string, metricstream.Labels, float64)                        {}
func (e *recordingEmitter) SetOneHot(string, metricstream.Labels, string, []string, string) {}

// TestSubmitOutcomesReachTheStream: an accepted and a permanently rejected submit each count once on the
// realtime feed, and only the rejection carries a code.
func TestSubmitOutcomesReachTheStream(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(sm smpp.SubmitSM) fakesmsc.Resp {
		if sm.DestinationAddr == "+2250700000002" {
			return fakesmsc.SubmitFailed()
		}
		return fakesmsc.OK()
	}})
	connectorID := uuid.New()
	emitter := &recordingEmitter{}

	results := runOneBatch(t, smsc, connectorID, emitter, []kafka.Record{
		routedRecord(t, connectorID, "+2250700000001"),
		routedRecord(t, connectorID, "+2250700000002"),
	})

	for i, err := range results {
		if err != nil {
			t.Fatalf("results[%d] = %v, want nil (both outcomes are terminal)", i, err)
		}
	}
	id := connectorID.String()
	want := map[string]float64{
		"submits_total{connector_id=" + id + ",status=ok,code=}":                    1,
		"submits_total{connector_id=" + id + ",status=rejected,code=}":              1,
		"submit_rejected_total{connector_id=" + id + ",status=,code=submit_failed}": 1,
	}
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	for k, v := range want {
		if emitter.adds[k] != v {
			t.Errorf("stream %s = %v, want %v (all adds: %v)", k, emitter.adds[k], v, emitter.adds)
		}
	}
}
