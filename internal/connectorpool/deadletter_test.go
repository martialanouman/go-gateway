package connectorpool_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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
		CDR:      permissiveReader(),
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
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod, CDR: permissiveReader()})
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

// fakeCDRReader answers Current from a per-message table, so ONE test can hold several messages in
// different lifecycle states. A reader that answered the same thing to every id could not tell a guard
// that refuses cancelled messages from a guard that refuses every message — the fixture would be blind
// to the very mutation it exists to catch (step-230).
//
// A message id absent from the table reads as "no CDR row", which is a distinct verdict from an error.
type fakeCDRReader struct {
	mu     sync.Mutex
	status map[uuid.UUID]clickhouse.Status
	// fallback answers for any id absent from status. The zero value means "no CDR row", which is a
	// verdict of its own — so a test that wants the ordinary population must set it explicitly.
	fallback clickhouse.Status
	err      error
	calls    int
	lastFor  uuid.UUID
	customer uuid.UUID
	account  uuid.UUID
}

func (f *fakeCDRReader) Current(_ context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastFor, f.customer, f.account = messageID, customerID, accountID
	if f.err != nil {
		return clickhouse.CDRRow{}, false, f.err
	}
	st, ok := f.status[messageID]
	if !ok {
		if f.fallback == "" {
			return clickhouse.CDRRow{}, false, nil
		}
		st = f.fallback
	}
	return clickhouse.CDRRow{MessageID: messageID, CustomerID: customerID, AccountID: accountID, Status: st}, true, nil
}

// permissiveReader answers "failed" to every message: the ordinary population of mt.dead-letter. It is
// for the tests that are about something OTHER than the cancellation guard — they must still carry a
// reader, because a nil one refuses by design and letting nil mean "no guard" is the defect step-240
// removes.
func permissiveReader() *fakeCDRReader {
	return &fakeCDRReader{fallback: clickhouse.StatusFailed}
}

// TestReplayRefusesACancelledMessageAndReplaysAFailedOne is the defect of step-240, and the guard
// against over-correcting it, in one fixture.
//
// A message cancelled before it was parked must NOT go back on the wire: past the 72h cancel-token TTL
// the connector would claim a free token and dispatch it, and the cancelled CDR row (rank 60) would then
// bury the enroute and delivered rows that follow — the exact symptom step-209 closed.
//
// The two message ids carry DIFFERENT statuses on purpose. With a single cancelled message, a guard that
// refused every record would pass this test just as well as the correct one; the assertion is on WHICH
// message comes out, not on how many.
func TestReplayRefusesACancelledMessageAndReplaysAFailedOne(t *testing.T) {
	cancelled, ordinary := routed(), routed()
	cdr := &fakeCDRReader{status: map[uuid.UUID]clickhouse.Status{
		cancelled.MessageID: clickhouse.StatusCancelled,
		ordinary.MessageID:  clickhouse.StatusFailed,
	}}

	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod, CDR: cdr})
	err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{
		deadLetterRecord(t, cancelled, "delivery_expired"),
		deadLetterRecord(t, ordinary, "retries_exhausted"),
	}})
	if err != nil {
		t.Fatalf("a cancelled record is a committed verdict, not a run failure: %v", err)
	}

	recs := prod.records()
	if len(recs) != 1 {
		t.Fatalf("produced %d records, want 1: the cancelled message must stay parked", len(recs))
	}
	out, decErr := pipeline.DecodeRouted(recs[0])
	if decErr != nil {
		t.Fatalf("decode replay: %v", decErr)
	}
	if out.MessageID != ordinary.MessageID {
		t.Errorf("replayed %s, want the FAILED message %s — the guard let the wrong one through",
			out.MessageID, ordinary.MessageID)
	}
	if got := replayer.Replayed(); got != 1 {
		t.Errorf("Replayed() = %d, want 1", got)
	}
	if got := replayer.Refused(); got != 1 {
		t.Errorf("Refused() = %d, want 1", got)
	}

	// The guard must read within the envelope's OWN scope. That is what makes the lookup cheap (the
	// sorting-key prefix) and what guarantees it finds the row the pool wrote from this same envelope.
	if cdr.customer != ordinary.CustomerID || cdr.account != ordinary.AccountID {
		t.Errorf("read scope = (%s,%s), want the envelope's own (%s,%s)",
			cdr.customer, cdr.account, ordinary.CustomerID, ordinary.AccountID)
	}
}

// TestReplayRefusesARejectedMessage closes a shadow in the guard rather than widening its scope.
//
// The message-level aggregate resolves status by a fixed precedence — rejected, THEN cancelled, then
// failed (internal/storage/clickhouse/cdr.go). A message carrying both a rejected and a cancelled row
// therefore reads "rejected", and a guard testing only for "cancelled" would wave it through. That the
// case is unreachable today (a rejected message never reaches mt.routed) is a property of ANOTHER
// service; this guard should not depend on it. Both statuses mean the same thing here — the message
// never left the gateway — so both refuse.
func TestReplayRefusesARejectedMessage(t *testing.T) {
	r := routed()
	cdr := &fakeCDRReader{status: map[uuid.UUID]clickhouse.Status{r.MessageID: clickhouse.StatusRejected}}

	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod, CDR: cdr})
	if err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{
		deadLetterRecord(t, r, "retries_exhausted"),
	}}); err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if recs := prod.records(); len(recs) != 0 {
		t.Fatalf("produced %d records, want none: a rejected message never left the gateway either", len(recs))
	}
}

// TestReplayFailsClosedOnCDRError pins what happens when the guard cannot answer.
//
// Returning the error leaves the offset UNCOMMITTED: the consumer commits only the prefix it handled
// successfully, so the tool stops, prints what it replayed, and a re-run resumes at exactly this record.
// Nothing is lost. Skipping-and-committing would be the only true silent data loss in the design — and
// for a message that is very likely legitimate. Replaying anyway would open the guard precisely during
// the outage where nobody is watching.
func TestReplayFailsClosedOnCDRError(t *testing.T) {
	r := routed()
	cdr := &fakeCDRReader{err: errors.New("clickhouse down")}

	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod, CDR: cdr})
	err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{
		deadLetterRecord(t, r, "retries_exhausted"),
	}})
	if err == nil {
		t.Fatal("an unreadable status must stop the drain: a committed offset here is a lost message")
	}
	if !strings.Contains(err.Error(), r.MessageID.String()) {
		t.Errorf("the error must name the record it stopped on, got: %v", err)
	}
	if recs := prod.records(); len(recs) != 0 {
		t.Fatalf("produced %d records, want none: nothing may be replayed on an unreadable status", len(recs))
	}
}

// TestReplayRefusesWhenTheCDRRowIsAbsent pins the third verdict, the one that is neither a cancellation
// nor an error.
//
// deadLetterWith writes the failed CDR row BEFORE it produces to mt.dead-letter, from the same RoutedMT
// the replay decodes — so every parked record has a CDR row in the exact scope this guard reads. An
// absent row therefore cannot come from the normal path: it means the row was purged by retention (90
// days) or erased under GDPR. Putting such a message back on the wire is worse than leaving it parked.
//
// It commits, unlike the error case: the verdict is settled, re-reading it would give the same answer.
// The separate counter is what makes the invariant falsifiable — if it ever moves, someone broke it.
func TestReplayRefusesWhenTheCDRRowIsAbsent(t *testing.T) {
	r := routed()
	cdr := &fakeCDRReader{status: map[uuid.UUID]clickhouse.Status{}} // the id is absent from the table

	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod, CDR: cdr})
	if err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{
		deadLetterRecord(t, r, "retries_exhausted"),
	}}); err != nil {
		t.Fatalf("an absent row is a settled verdict, not a run failure: %v", err)
	}
	if recs := prod.records(); len(recs) != 0 {
		t.Fatalf("produced %d records, want none: a message with no CDR row was purged or erased", len(recs))
	}
	if got := replayer.RefusedAbsent(); got != 1 {
		t.Errorf("RefusedAbsent() = %d, want 1 — the counter is what makes the invariant falsifiable", got)
	}
	if got := replayer.Refused(); got != 0 {
		t.Errorf("Refused() = %d, want 0: an absent row is not a cancellation, and conflating the two "+
			"would hide a broken invariant behind a normal-looking count", got)
	}
}

// TestReplayerWithoutACDRReaderFailsClosed pins the wiring regression that would be silent otherwise.
//
// A binary built without a CDR reader must not degrade into "no guard": that is exactly the behaviour
// step-240 exists to remove, and it would come back with no test failing and no log saying so.
func TestReplayerWithoutACDRReaderFailsClosed(t *testing.T) {
	prod := newRecordingProducer()
	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: prod})
	err := replayer.Run(context.Background(), &handlerConsumer{records: []kafka.Record{
		deadLetterRecord(t, routed(), "retries_exhausted"),
	}})
	if err == nil {
		t.Fatal("a replayer with no CDR reader must refuse to replay, not replay without the guard")
	}
	if recs := prod.records(); len(recs) != 0 {
		t.Fatalf("produced %d records, want none", len(recs))
	}
}
