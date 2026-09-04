package status_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestPublishBindDerivesConnectorLoad: every PublishBind recomputes connectorload:{id} — the derived,
// multi-pod in-flight gauge least_loaded reads (Appendix B) — as the sum of the live per-bind entries,
// with a TTL so a dead connector's gauge fades. Before step-260d nothing wrote this key at all.
func TestPublishBindDerivesConnectorLoad(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	id := uuid.New()
	r := status.NewReader(rdb)

	for _, pub := range []struct {
		pod      string
		idx      int
		inFlight int
	}{{"pod-a", 0, 3}, {"pod-a", 1, 4}, {"pod-b", 0, 5}} {
		if err := r.PublishBind(ctx, id, pub.pod, pub.idx, status.LinkUp, pub.inFlight); err != nil {
			t.Fatalf("publish %s:%d: %v", pub.pod, pub.idx, err)
		}
	}

	got, err := rdb.Get(ctx, status.LoadKey(id)).Int()
	if err != nil {
		t.Fatalf("GET %s: %v", status.LoadKey(id), err)
	}
	if got != 12 {
		t.Errorf("connectorload = %d, want 3+4+5 = 12", got)
	}
	if ttl := rdb.PTTL(ctx, status.LoadKey(id)).Val(); ttl <= 0 {
		t.Errorf("connectorload has no TTL (%v): a dead connector's gauge would never fade", ttl)
	}

	// The reader the router wires sees the same number (cache disabled: this asserts the key, not the cache).
	lr := status.NewLoadReader(rdb, status.WithLoadCacheTTL(0))
	if n := lr.InFlight(ctx, id); n != 12 {
		t.Errorf("LoadReader.InFlight = %d, want 12", n)
	}
	if n := lr.InFlight(ctx, uuid.New()); n != 0 {
		t.Errorf("InFlight of an unpublished connector = %d, want 0", n)
	}
}

// TestPublishBindSweepsStaleBinds: a per-bind entry whose ts is older than the bind TTL (a shrunk bind
// or a crashed pod) is swept from the hash and excluded from the derived gauge — otherwise a dead pod's
// last in_flight would inflate the connector's load until the whole key expired.
func TestPublishBindSweepsStaleBinds(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	id := uuid.New()
	r := status.NewReader(rdb)

	stale := status.BindEntry{LinkStatus: status.LinkUp, InFlight: 99, TS: time.Now().Add(-10 * time.Minute).UnixMilli()}
	if err := rdb.HSet(ctx, status.BindsKey(id), "pod-dead:0", string(stale.Encode())).Err(); err != nil {
		t.Fatalf("seed stale field: %v", err)
	}
	if err := r.PublishBind(ctx, id, "pod-a", 0, status.LinkUp, 2); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if got := rdb.Get(ctx, status.LoadKey(id)).Val(); got != "2" {
		t.Errorf("connectorload = %s, want 2 (the stale bind's 99 must not count)", got)
	}
	if rdb.HExists(ctx, status.BindsKey(id), "pod-dead:0").Val() {
		t.Error("stale field pod-dead:0 still in connector:binds: the publish script must sweep it")
	}
}

// countingStore is a LoadReader store that serves one value and counts the GETs it receives.
type countingStore struct {
	val  string
	gets int
}

func (s *countingStore) Get(_ context.Context, _ string) *goredis.StringCmd {
	s.gets++
	return goredis.NewStringResult(s.val, nil)
}

// TestLoadReaderCachesWithinTTL: least_loaded resolves per message, but the gauge is republished only
// every heartbeat, so the reader serves a per-connector value from memory for the cache TTL. Two reads
// inside the TTL cost one GET; a read after the TTL costs another.
func TestLoadReaderCachesWithinTTL(t *testing.T) {
	store := &countingStore{val: "7"}
	now := time.Unix(1_700_000_000, 0)
	lr := status.NewLoadReader(store, status.WithLoadCacheTTL(time.Second), status.WithLoadClock(func() time.Time { return now }))
	id := uuid.New()

	if n := lr.InFlight(context.Background(), id); n != 7 {
		t.Fatalf("InFlight = %d, want 7", n)
	}
	if n := lr.InFlight(context.Background(), id); n != 7 {
		t.Fatalf("second InFlight = %d, want 7", n)
	}
	if store.gets != 1 {
		t.Errorf("two reads inside the TTL cost %d GETs, want 1", store.gets)
	}

	now = now.Add(1500 * time.Millisecond)
	store.val = "9"
	if n := lr.InFlight(context.Background(), id); n != 9 {
		t.Errorf("InFlight after the TTL = %d, want the refreshed 9", n)
	}
	if store.gets != 2 {
		t.Errorf("a read after the TTL cost %d GETs in total, want 2", store.gets)
	}
}

// scriptedStore answers each Get with the next scripted result: a hit, an absent key, a Redis fault.
type scriptedStore struct{ results []*goredis.StringCmd }

func (s *scriptedStore) Get(_ context.Context, _ string) *goredis.StringCmd {
	r := s.results[0]
	s.results = s.results[1:]
	return r
}

type recordingMeter struct{ outcomes []string }

func (m *recordingMeter) ObserveLoadRead(outcome string) { m.outcomes = append(m.outcomes, outcome) }

// TestLoadReaderCountsEveryOutcome: the counter is what makes "no gauge published" distinguishable from
// "every connector idle" — both read 0 to the strategy. Each read lands on exactly one of the three
// closed outcomes, and a Redis fault reads 0 rather than failing the resolve.
func TestLoadReaderCountsEveryOutcome(t *testing.T) {
	store := &scriptedStore{results: []*goredis.StringCmd{
		goredis.NewStringResult("7", nil),
		goredis.NewStringResult("", goredis.Nil),
		goredis.NewStringResult("", errors.New("redis: connection refused")),
	}}
	meter := &recordingMeter{}
	lr := status.NewLoadReader(store, status.WithLoadCacheTTL(0), status.WithLoadMeter(meter),
		status.WithLoadLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	id := uuid.New()

	for i, want := range []int{7, 0, 0} {
		if got := lr.InFlight(context.Background(), id); got != want {
			t.Errorf("read %d: InFlight = %d, want %d", i, got, want)
		}
	}
	want := []string{status.LoadReadHit, status.LoadReadMissing, status.LoadReadError}
	if !slices.Equal(meter.outcomes, want) {
		t.Errorf("observed outcomes = %v, want %v", meter.outcomes, want)
	}
}
