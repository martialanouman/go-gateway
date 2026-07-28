package connectorpool_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// fakeRerouteLimiter allows once its allow flag is set; it can start denying to exercise pacing.
type fakeRerouteLimiter struct {
	allow      atomic.Bool
	calls      atomic.Int32
	allowAfter int32 // deny until this many calls have been made (0 = follow `allow`)
}

func (f *fakeRerouteLimiter) AllowConnector(_ context.Context, _ uuid.UUID, _ int) bool {
	n := f.calls.Add(1)
	if f.allowAfter > 0 {
		return n >= f.allowAfter
	}
	return f.allow.Load()
}

// fakeDrainConsumer feeds records to the drainer's handler once, then returns.
type fakeDrainConsumer struct{ records []kafka.Record }

func (c *fakeDrainConsumer) Run(ctx context.Context, handle kafka.Handler) error {
	for _, r := range c.records {
		if err := handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func parkedRecord(t *testing.T, target uuid.UUID) kafka.Record {
	t.Helper()
	r := routed()
	r.ConnectorID = target
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec
}

// TestDrainerReplaysToRouted: a parked record for this connector is replayed to mt.routed once the target
// has capacity.
func TestDrainerReplaysToRouted(t *testing.T) {
	b := uuid.New()
	prod := &recordingProducer{got: make(chan struct{}, 4)}
	lim := &fakeRerouteLimiter{}
	lim.allow.Store(true)
	d := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    &fakeDrainConsumer{records: []kafka.Record{parkedRecord(t, b)}},
		Producer:    prod,
		Limiter:     lim,
		ConnectorID: b,
	})
	if err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTRouted {
		t.Fatalf("produced %+v, want one mt.routed replay", recs)
	}
	got, _ := pipeline.DecodeRouted(recs[0])
	if got.ConnectorID != b {
		t.Errorf("replayed connector = %s, want %s", got.ConnectorID, b)
	}
}

// TestDrainerSkipsForeign: a parked record for another connector is skipped-and-committed, not replayed.
func TestDrainerSkipsForeign(t *testing.T) {
	mine, other := uuid.New(), uuid.New()
	prod := &recordingProducer{got: make(chan struct{}, 1)}
	lim := &fakeRerouteLimiter{}
	lim.allow.Store(true)
	d := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    &fakeDrainConsumer{records: []kafka.Record{parkedRecord(t, other)}},
		Producer:    prod,
		Limiter:     lim,
		ConnectorID: mine,
	})
	if err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.records()) != 0 {
		t.Errorf("replayed a foreign connector's parked record: %+v", prod.records())
	}
}

// TestDrainerPacesUntilAllowed: the drainer waits for the target's ceiling before replaying (a burst is
// drained no faster than the fallback connector's limit).
func TestDrainerPacesUntilAllowed(t *testing.T) {
	b := uuid.New()
	prod := &recordingProducer{got: make(chan struct{}, 4)}
	lim := &fakeRerouteLimiter{allowAfter: 3} // deny the first two ceiling checks, allow the third
	d := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    &fakeDrainConsumer{records: []kafka.Record{parkedRecord(t, b)}},
		Producer:    prod,
		Limiter:     lim,
		ConnectorID: b,
		Retry:       5 * time.Millisecond,
	})
	if err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.records()) != 1 {
		t.Fatalf("produced %d, want 1 after pacing", len(prod.records()))
	}
	if lim.calls.Load() < 3 {
		t.Errorf("ceiling checks = %d, want >= 3 (paced through denials)", lim.calls.Load())
	}
}

// TestDrainerHonoursContext: a drainer blocked pacing on a permanently-saturated target returns when the
// context is cancelled (no leak).
func TestDrainerHonoursContext(t *testing.T) {
	b := uuid.New()
	lim := &fakeRerouteLimiter{} // allow=false forever
	d := connectorpool.NewDrainer(connectorpool.DrainerDeps{
		Consumer:    &fakeDrainConsumer{records: []kafka.Record{parkedRecord(t, b)}},
		Producer:    &recordingProducer{got: make(chan struct{}, 1)},
		Limiter:     lim,
		ConnectorID: b,
		Retry:       5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); err == nil {
		t.Error("Run returned nil, want the context error while pacing a saturated target")
	}
}
