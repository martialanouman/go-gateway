package connectorpool_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// settleLog is the BillingSettler double for the flapping test. It records, PER message_id, the ordered
// settle operations the pool performed.
//
// It records rather than counts, and that is the whole point of it. The spySettler next door
// (billing_settle_test.go:27) keeps two integers, captureCalls and releaseCalls — enough for a
// single-attempt scenario, useless here: "message A settled twice, message B never settled" leaves those
// two totals reading exactly what a correct run leaves. Invariant (c) is a per-message property, so the
// double has to be keyed by message.
type settleLog struct {
	mu  sync.Mutex
	ops map[uuid.UUID][]string
}

func newSettleLog() *settleLog { return &settleLog{ops: make(map[uuid.UUID][]string)} }

func (s *settleLog) record(id uuid.UUID, op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops[id] = append(s.ops[id], op)
}

func (s *settleLog) Capture(_ context.Context, r pipeline.RoutedMT) (bool, *int32) {
	s.record(r.MessageID, "capture")
	return true, nil
}

func (s *settleLog) Release(_ context.Context, r pipeline.RoutedMT) { s.record(r.MessageID, "release") }

// snapshot copies the log so assertions read a stable map.
func (s *settleLog) snapshot() map[uuid.UUID][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uuid.UUID][]string, len(s.ops))
	for id, ops := range s.ops {
		out[id] = append([]string(nil), ops...)
	}
	return out
}

// flappingConfig makes the breaker cycle in hundreds of milliseconds instead of the production tens of
// seconds. scenarioBreakerConfig cannot be reused: its 5s cooldown was chosen so the breaker STAYS open
// past a poll deadline, which is the opposite of what a flapping test needs.
//
// Cooldown is the pacing constant, and the bind's ResponseTimeout must stay strictly below it — the
// breaker attributes each outcome to its CURRENT state, not to the probe episode that caused it, so a
// response arriving after a full cooldown would be credited to a later episode (step-121 godoc).
var flappingConfig = breaker.Config{
	MinRequests: 2, FailureRate: 0.5, Window: 2 * time.Second, Cooldown: 300 * time.Millisecond,
	HalfOpenProbes: 1, HalfOpenTimeout: 300 * time.Millisecond,
}

const flapResponseTimeout = 100 * time.Millisecond // < Cooldown, per the invariant above

// collectingProducer records every produced record and never blocks.
//
// recordingProducer next door cannot be used here: it signals each Produce into a 16-slot channel, so a
// test that produces more than 16 records without draining it deadlocks in the pool's own goroutine.
// That helper is built for tests that wait on `got`; a flapping run publishes an outcome or a
// dead-letter per attempt and blows straight through the buffer.
type collectingProducer struct {
	mu   sync.Mutex
	recs []kafka.Record
}

func (p *collectingProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recs = append(p.recs, rec)
	return nil
}

func (p *collectingProducer) records() []kafka.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]kafka.Record(nil), p.recs...)
}

// flapFeeder feeds the pool for as long as the flapping needs, minting a fresh message each pass and
// REDELIVERING ONLY WHAT WAS NOT COMMITTED.
//
// The commit rule is the fidelity of this double. kafka.Consumer commits the successfully-handled
// prefix, so a record whose handler returned nil never comes back; only a handler error leaves the
// offset in place. A consumer that replayed every record every pass would not be modelling Kafka — it
// would re-feed messages the gateway had already finished with, and the pool would appear to settle them
// again and again. The double-billing this test hunts must come from the pool, never from a harness
// inventing redeliveries Kafka would not perform.
//
// It mints new work rather than replaying a fixed list because the run has to OUTLAST several breaker
// cooldowns: once the connector recovers, the backlog commits and drains, and a fixed list would empty
// long before the breaker could close and reopen. Each record gets its own offset — retryKey is
// (partition, offset), so records sharing one would share a single retry-window entry and the first
// message's failure clock would govern every other message.
type flapFeeder struct {
	pause    time.Duration
	deadline time.Duration
	enough   func() bool

	mu          sync.Mutex
	submitted   []uuid.UUID
	offset      int64
	redelivered int
}

// mint builds one fresh billable message and remembers its id.
func (f *flapFeeder) mint(t *testing.T) kafka.Record {
	t.Helper()
	r := routed()
	r.Billable = true // the production scenario; the Billable gate lives in settle.Settler, not the pool
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec.Offset = f.offset
	f.offset++
	f.submitted = append(f.submitted, r.MessageID)
	return rec
}

// redeliveries counts how many handler errors sent a record back for another delivery.
func (f *flapFeeder) redeliveries() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.redelivered
}

// ids returns every message id fed so far.
func (f *flapFeeder) ids() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.submitted...)
}

func (f *flapFeeder) run(t *testing.T) func(context.Context, kafka.BatchHandler) error {
	return func(ctx context.Context, handle kafka.BatchHandler) error {
		stop := time.Now().Add(f.deadline)
		var pending []kafka.Record
		for ctx.Err() == nil && time.Now().Before(stop) {
			if f.enough() && len(pending) == 0 {
				return nil // the flapping goal is met and nothing is in flight
			}
			batch := make([]kafka.Record, 0, len(pending)+1)
			batch = append(batch, pending...)
			batch = append(batch, f.mint(t))
			pending = nil
			for _, rec := range batch {
				errs := handle(ctx, []kafka.Record{rec})
				if len(errs) > 0 && errs[0] != nil {
					pending = append(pending, rec) // uncommitted: Kafka brings it back
					f.mu.Lock()
					f.redelivered++
					f.mu.Unlock()
				}
				time.Sleep(f.pause)
			}
		}
		return nil
	}
}

// feederConsumer adapts flapFeeder to the Consumer interface.
type feederConsumer struct {
	fn func(context.Context, kafka.BatchHandler) error
}

func (c feederConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	return c.fn(ctx, handle)
}

// flapper alternates the SMSC between sick and healthy, following the breaker rather than the clock: it
// flips to healthy once the breaker has opened, and back to sick once it has closed. Driving it off the
// observed state is what produces real CYCLES — a time-based script would only prove the SMSC changed
// its mind on schedule.
type flapper struct {
	sick atomic.Bool

	mu          sync.Mutex
	transitions []breaker.State
}

func (f *flapper) resp(smpp.SubmitSM) fakesmsc.Resp {
	if f.sick.Load() {
		return fakesmsc.SysErr() // failover-class: opens the breaker, and retries the message
	}
	return fakesmsc.OK()
}

// observed returns the breaker states seen, in order, deduplicated.
func (f *flapper) observed() []breaker.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]breaker.State(nil), f.transitions...)
}

// run watches the reported breaker state until ctx ends, recording every transition and steering the
// SMSC so the breaker keeps cycling.
func (f *flapper) run(ctx context.Context, agg *fakeAgg) {
	f.sick.Store(true)
	last := breaker.State(-1)
	for ctx.Err() == nil {
		s := agg.state(0)
		if s != last {
			f.mu.Lock()
			f.transitions = append(f.transitions, s)
			f.mu.Unlock()
			last = s
			switch s {
			case breaker.Open:
				f.sick.Store(false) // let the half-open probe succeed, so the breaker can close
			case breaker.Closed:
				f.sick.Store(true) // and sicken again, so it can reopen: that is the flap
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// countCycles counts how many times the breaker went Open after having been Closed — i.e. how many
// distinct sickness episodes the pool lived through.
func countCycles(states []breaker.State) int {
	cycles, closedSince := 0, false
	for _, s := range states {
		switch s {
		case breaker.Closed:
			closedSince = true
		case breaker.Open:
			if closedSince {
				cycles++
				closedSince = false
			}
		}
	}
	return cycles
}

// TestFlappingConnectorSettlesEachMessageExactlyOnce is the step-250b acceptance test: under a connector
// that sickens, recovers and sickens again, no message is lost and none is billed twice (invariant c).
//
// The two properties are asserted per message_id, never as totals — the step-250 lesson. A total is
// satisfied by a message lost and a message duplicated at once: the two errors cancel and the count
// reads clean while the system did the two worst things it could do.
//
// What makes double-settling possible here is REDELIVERY. A failover-class rejection (ESME_RSYSERR with
// no fallback chain) runs through healthRetry: inside the retry window the handler returns an error, the
// offset is not committed and the record comes back. So each message may be attempted many times, and
// the invariant is that only the ONE attempt that reached a terminal outcome settled it.
func TestFlappingConnectorSettlesEachMessageExactlyOnce(t *testing.T) {
	const wantCycles = 2

	flap := &flapper{}
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: flap.resp})
	agg := &fakeAgg{}
	settles := newSettleLog()
	prod, cdr := &collectingProducer{}, &fakeCDR{}
	rrec := otelrec.New(t)

	feeder := &flapFeeder{
		pause:    10 * time.Millisecond,
		deadline: 30 * time.Second,
		enough:   func() bool { return countCycles(flap.observed()) >= wantCycles },
	}

	bind := poolBind(smsc.Addr(), 1)
	bind.ResponseTimeout = flapResponseTimeout
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:         feederConsumer{fn: feeder.run(t)},
		Producer:         prod,
		CDR:              cdr,
		DeadLetter:       newRecordingDeadLetter(),
		Billing:          settles,
		Breaker:          agg,
		BreakerConfig:    flappingConfig,
		BreakerHeartbeat: 5 * time.Millisecond,
		// A message still failing on its SECOND delivery dead-letters instead of retrying for ever, so the
		// run exercises the release leg as well as capture. The redelivery gap is a full feeder pass (at
		// least one 10ms pause plus a submit), comfortably past this window — and elapsed time only grows
		// under load, so a busy machine can make this fire sooner, never later.
		RetryWindow: 10 * time.Millisecond,
		Bind:        bind,
		Tracer:      observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	flapCtx, stopFlapper := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); flap.run(flapCtx, agg) }()

	runErr := svc.Run(ctx)
	stopFlapper()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	// The chaos must have actually happened. Without this the whole test would pass against a healthy
	// SMSC — every message would simply be captured once, satisfying every assertion below while
	// exercising no flapping at all. This is the mutation that caught step-250's hollow chaos test.
	states := flap.observed()
	if cycles := countCycles(states); cycles < wantCycles {
		t.Fatalf("the breaker opened-after-closing %d time(s), want at least %d — the connector never "+
			"flapped, so nothing below tests what this step is about (states seen: %v)",
			cycles, wantCycles, states)
	}

	submitted := feeder.ids()
	if len(submitted) < 2 {
		t.Fatalf("only %d message(s) were fed; the run is too small to mean anything", len(submitted))
	}

	// Redelivery is the mechanism that makes double-settling possible at all: a handler error leaves the
	// offset uncommitted and the message comes back for another attempt. Asserting it is what stops this
	// test from silently degrading into "each message was handled once and settled once" — true, but
	// proving nothing about the Kafka hop the invariant is written against.
	if n := feeder.redeliveries(); n == 0 {
		t.Fatalf("not one record was redelivered: every message was handled exactly once, so the run " +
			"never exercised the at-least-once hop that makes double-billing possible")
	}

	// Invariant (c), per message: at most one settle, and never both legs. Two captures double-charge; a
	// capture AND a release charge and refund the same message, leaving the CDR contradicting itself.
	ops := settles.snapshot()
	for id, got := range ops {
		if len(got) > 1 {
			t.Errorf("message %s was settled %d times (%v): billing must be idempotent by message_id "+
				"across the Kafka hop, and a flapping connector redelivers", id, len(got), got)
		}
	}

	// No message lost: every fed id reached exactly one terminal outcome — published enroute, or
	// recorded in the CDR (a permanent failure or a dead-letter).
	terminal := make(map[uuid.UUID]string, len(submitted))
	claim := func(id uuid.UUID, what string) {
		t.Helper()
		if prev, dup := terminal[id]; dup {
			t.Errorf("message %s reached TWO terminal outcomes (%s then %s)", id, prev, what)
		}
		terminal[id] = what
	}
	for _, rec := range prod.records() {
		if rec.Topic == kafka.TopicMTOutcome {
			out, err := pipeline.DecodeOutcome(rec)
			if err != nil {
				t.Fatalf("decode outcome: %v", err)
			}
			claim(out.MessageID, "outcome:"+out.Status)
		}
	}
	for _, row := range cdr.rows {
		claim(row.MessageID, "cdr:"+string(row.Status))
	}

	// Both fates must actually occur across the flapping, or the reconciliation is only exercising one
	// of them: a run where everything succeeded would never test the release leg, and one where
	// everything failed would never test capture.
	var captured, released int
	for _, got := range ops {
		switch {
		case len(got) == 1 && got[0] == "capture":
			captured++
		case len(got) == 1 && got[0] == "release":
			released++
		}
	}
	if captured == 0 || released == 0 {
		t.Errorf("across the flapping: %d captured, %d released — a run must exercise BOTH settle legs, "+
			"or half the invariant is untested (terminal outcomes: %v)", captured, released, terminal)
	}

	for _, id := range submitted {
		what, ok := terminal[id]
		if !ok {
			t.Errorf("message %s VANISHED across the flapping: no outcome, no CDR row — the connector "+
				"neither delivered it, refused it, nor parked it", id)
			continue
		}
		// A message that reached a terminal outcome must carry exactly one settle: the settle is what
		// accompanies the terminal decision.
		if got := ops[id]; len(got) != 1 {
			t.Errorf("message %s ended %s with %d settles (%v), want exactly 1", id, what, len(got), got)
		}
	}
}
