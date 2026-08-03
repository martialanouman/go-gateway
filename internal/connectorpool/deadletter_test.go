package connectorpool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// recordingDeadLetter counts DeadLetterMetric.Inc calls by reason.
type recordingDeadLetter struct {
	mu     sync.Mutex
	byName map[string]int
}

func newRecordingDeadLetter() *recordingDeadLetter {
	return &recordingDeadLetter{byName: map[string]int{}}
}

func (m *recordingDeadLetter) Inc(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byName[reason]++
}

func (m *recordingDeadLetter) count(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byName[reason]
}

// deadLetterDeps wires a single-connector pool (ConnectorID nil → processes every record) with a
// recording producer, CDR and dead-letter metric, plus the given guards, and drives it through the given
// consumer. It returns the sinks after Run completes.
func deadLetterDeps(t *testing.T, consumer connectorpool.Consumer, resp func(smpp.SubmitSM) fakesmsc.Resp, retryWindow, maxAge time.Duration) (*recordingProducer, *fakeCDR, *recordingDeadLetter) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	prod := newRecordingProducer()
	cdr := &fakeCDR{}
	dl := newRecordingDeadLetter()
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:      consumer,
		CDR:           cdr,
		Producer:      prod,
		DeadLetter:    dl,
		RetryWindow:   retryWindow,
		MaxMessageAge: maxAge,
		Bind:          poolBind(smsc.Addr(), 1),
		Tracer:        observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return prod, cdr, dl
}

// TestMaxAgeDeadLetters: a message older than MaxMessageAge is dead-lettered as delivery_expired before
// any submit, with the reason on the record header, a failed CDR row and a counted metric.
func TestMaxAgeDeadLetters(t *testing.T) {
	r := routed()
	r.SubmittedAt = time.Now().Add(-2 * time.Hour).UTC() // well past a 1h SLA
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	prod, cdr, dl := deadLetterDeps(t, &fakeConsumer{records: []kafka.Record{rec}},
		func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0, time.Hour)

	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTDeadLetter {
		t.Fatalf("produced %+v, want one mt.dead-letter record", recs)
	}
	if reason, ok := recs[0].Header(kafka.HeaderDeadLetterReason); !ok || string(reason) != "delivery_expired" {
		t.Errorf("dead_letter_reason header = %q (present=%v), want delivery_expired", reason, ok)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusFailed || cdr.rows[0].ErrorCode == nil || *cdr.rows[0].ErrorCode != "delivery_expired" {
		t.Fatalf("cdr rows = %+v, want one failed delivery_expired", cdr.rows)
	}
	if dl.count("delivery_expired") != 1 {
		t.Errorf("delivery_expired metric = %d, want 1", dl.count("delivery_expired"))
	}
}

// TestRetryWindowDeadLetters: a message with NO fallback chain that keeps hitting a connector-health
// failure (system error) past the retry window is dead-lettered as retries_exhausted rather than
// redelivered without end. Two redeliveries of the same offset separated by a real gap: the first
// stores the first-failure time; the second, past the window, dead-letters.
func TestRetryWindowDeadLetters(t *testing.T) {
	rec, err := pipeline.EncodeRouted(routed()) // no fallback chain
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// pausingConsumer redelivers the same record (same partition/offset 0) twice with a real 10ms gap,
	// ignoring the transient error the first delivery returns. 10ms comfortably exceeds the 5ms window
	// (elapsed time only grows under load, so this cannot flake short), so the second delivery is past
	// the window and dead-letters.
	prod, cdr, dl := deadLetterDeps(t, &pausingConsumer{records: []kafka.Record{rec, rec}, pause: 10 * time.Millisecond},
		func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SysErr() }, 5*time.Millisecond, 0)

	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTDeadLetter {
		t.Fatalf("produced %+v, want one mt.dead-letter record after the window", recs)
	}
	if reason, ok := recs[0].Header(kafka.HeaderDeadLetterReason); !ok || string(reason) != "retries_exhausted" {
		t.Errorf("dead_letter_reason header = %q (present=%v), want retries_exhausted", reason, ok)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].ErrorCode == nil || *cdr.rows[0].ErrorCode != "retries_exhausted" {
		t.Fatalf("cdr rows = %+v, want one failed retries_exhausted", cdr.rows)
	}
	if dl.count("retries_exhausted") != 1 {
		t.Errorf("retries_exhausted metric = %d, want 1", dl.count("retries_exhausted"))
	}
}

// pausingConsumer redelivers each record one-per-batch with a real pause between deliveries, ignoring
// handler errors — so a retry-window test can let wall-clock advance past the window between redeliveries.
type pausingConsumer struct {
	records []kafka.Record
	pause   time.Duration
}

func (c *pausingConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	for i, r := range c.records {
		if i > 0 {
			time.Sleep(c.pause)
		}
		_ = handle(ctx, []kafka.Record{r})
	}
	return nil
}

// handlerConsumer feeds records to a kafka.Handler (the Replayer's consumer shape).
type handlerConsumer struct{ records []kafka.Record }

func (h *handlerConsumer) Run(ctx context.Context, handle kafka.Handler) error {
	for _, r := range h.records {
		if err := handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// deadLetterRecord builds an mt.dead-letter record for r with the given reason header, as the pool parks it.
func deadLetterRecord(t *testing.T, r pipeline.RoutedMT, reason string) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec.Topic = kafka.TopicMTDeadLetter
	rec.Headers = append(rec.Headers, kafka.Header{Key: kafka.HeaderDeadLetterReason, Value: []byte(reason)})
	return rec
}

// TestReplayReinjectsCorrelated: the replayer republishes a dead-lettered message to mt.routed under the
// same ids (correlation preserved → billing idempotent when M9 lands), stamps a replayed_at header,
// drops the dead_letter_reason, and preserves the body in the value.
func TestReplayReinjectsCorrelated(t *testing.T) {
	r := routed()
	r.Body = msg.NewBodyString("replay me")
	in := deadLetterRecord(t, r, "retries_exhausted")

	replayAt := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{
		Producer: prod,
		Now:      func() time.Time { return replayAt },
	})
	if err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{in}}); err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if got := replayer.Replayed(); got != 1 {
		t.Fatalf("Replayed() = %d, want 1", got)
	}

	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTRouted {
		t.Fatalf("produced %+v, want one mt.routed record", recs)
	}
	if _, ok := recs[0].Header(kafka.HeaderDeadLetterReason); ok {
		t.Errorf("dead_letter_reason header must be stripped on replay")
	}
	if raw, ok := recs[0].Header(kafka.HeaderReplayedAt); !ok || string(raw) != replayAt.Format(time.RFC3339Nano) {
		t.Errorf("replayed_at header = %q (present=%v), want %s", raw, ok, replayAt.Format(time.RFC3339Nano))
	}
	out, err := pipeline.DecodeRouted(recs[0])
	if err != nil {
		t.Fatalf("decode replay: %v", err)
	}
	if out.MessageID != r.MessageID || out.TraceID != r.TraceID {
		t.Errorf("replay ids = (%s,%s), want (%s,%s) — correlation lost", out.MessageID, out.TraceID, r.MessageID, r.TraceID)
	}
	if string(out.Body.Reveal()) != "replay me" {
		t.Errorf("replay body = %q, want preserved", out.Body.Reveal())
	}
	if out.ReplayedAt == nil || !out.ReplayedAt.Equal(replayAt) {
		t.Errorf("decoded ReplayedAt = %v, want %s", out.ReplayedAt, replayAt)
	}
}

// TestReplaySkipsUndecodable: a malformed dead-letter record must not wedge the drain — the replayer
// skips it and still replays the valid records that follow.
func TestReplaySkipsUndecodable(t *testing.T) {
	corrupt := kafka.Record{Topic: kafka.TopicMTDeadLetter, Value: []byte("{not json")}
	good := deadLetterRecord(t, routed(), "retries_exhausted")

	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod})
	if err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{corrupt, good}}); err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if got := replayer.Replayed(); got != 1 {
		t.Errorf("Replayed() = %d, want 1 (corrupt skipped, good replayed)", got)
	}
	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTRouted {
		t.Fatalf("produced %+v, want one mt.routed record", recs)
	}
}

// TestReplayedMessageNotReExpired is the Fable-flagged trap: a replayed message keeps its immutable
// (old) SubmittedAt, so a naive max-age check would re-expire it the instant it is replayed. Because the
// pool bases age on max(SubmittedAt, ReplayedAt), a message submitted 2h ago but replayed now is NOT
// expired under a 1h SLA — it submits normally and records a delivered CDR, not a second dead-letter.
func TestReplayedMessageNotReExpired(t *testing.T) {
	r := routed()
	r.SubmittedAt = time.Now().Add(-2 * time.Hour).UTC() // old, past the 1h SLA
	now := time.Now().UTC()
	r.ReplayedAt = &now // replayed just now
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	prod, cdr, dl := deadLetterDeps(t, &fakeConsumer{records: []kafka.Record{rec}},
		func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0, time.Hour)

	// The pool shares one producer across the dead-letter and outcome paths, so the assertion is on the
	// TOPIC: nothing may be parked, and the one record published must be the enroute outcome of a normal
	// submit (step-201c, D1).
	recs := prod.records()
	for _, r := range recs {
		if r.Topic == kafka.TopicMTDeadLetter {
			t.Fatalf("a replayed message must not be re-dead-lettered, got %+v", r)
		}
	}
	if dl.count("delivery_expired") != 0 {
		t.Errorf("delivery_expired metric = %d, want 0", dl.count("delivery_expired"))
	}
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTOutcome {
		t.Fatalf("produced %+v, want one mt.outcome record (submitted normally)", recs)
	}
	out, err := pipeline.DecodeOutcome(recs[0])
	if err != nil {
		t.Fatalf("decode mt.outcome: %v", err)
	}
	if out.Status != string(clickhouse.StatusEnroute) {
		t.Fatalf("outcome status = %q, want enroute (submitted normally)", out.Status)
	}
	if len(cdr.rows) != 0 {
		t.Fatalf("cdr rows = %+v, want none: the send path no longer writes ClickHouse", cdr.rows)
	}
}
