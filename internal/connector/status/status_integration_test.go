package status_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestReaderAssemblesDistinctStates: the reader merges the link hash (connector:binds), the breaker
// sub-bind hash (breaker:binds) and the breaker aggregate into one status, keeping link_status and
// breaker_state distinct per bind.
func TestReaderAssemblesDistinctStates(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	id := uuid.New()
	r := status.NewReader(rdb)

	// pod-a bind 0: link up, breaker open (distinct); bind 1: link reconnecting, breaker closed.
	rdb.HSet(ctx, status.BindsKey(id), "pod-a:0", string(status.BindEntry{LinkStatus: status.LinkUp, InFlight: 7}.Encode()))
	rdb.HSet(ctx, status.BindsKey(id), "pod-a:1", string(status.BindEntry{LinkStatus: status.LinkReconnecting}.Encode()))
	rdb.HSet(ctx, "breaker:binds:{"+id.String()+"}", "pod-a:0", "open:1700000000000")
	rdb.HSet(ctx, "breaker:binds:{"+id.String()+"}", "pod-a:1", "closed:1700000000000")
	rdb.Set(ctx, "breaker:state:{"+id.String()+"}", "open", 0)

	got, err := r.Read(ctx, id)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.BreakerState != "open" {
		t.Errorf("aggregate breaker = %q, want open", got.BreakerState)
	}
	byIdx := map[int]status.Bind{}
	for _, b := range got.Binds {
		byIdx[b.BindIndex] = b
	}
	if len(byIdx) != 2 {
		t.Fatalf("binds = %+v, want 2", got.Binds)
	}
	if b := byIdx[0]; b.LinkStatus != "up" || b.BreakerState != "open" || b.InFlight != 7 || b.PodID != "pod-a" {
		t.Errorf("bind 0 = %+v, want link up / breaker open / in_flight 7 / pod-a (distinct states)", b)
	}
	if b := byIdx[1]; b.LinkStatus != "reconnecting" || b.BreakerState != "closed" {
		t.Errorf("bind 1 = %+v, want link reconnecting / breaker closed", b)
	}
}

// TestReaderEmptyWhenNothingPublished: an unknown/idle connector yields an empty bind list and a closed
// aggregate, not an error.
func TestReaderEmptyWhenNothingPublished(t *testing.T) {
	rdb := redistest.Client(t)
	got, err := status.NewReader(rdb).Read(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.BreakerState != "closed" || len(got.Binds) != 0 {
		t.Errorf("status = %+v, want closed aggregate + no binds", got)
	}
}

// TestSignalReconfigureIncrements: SignalReconfigure bumps the generation the pool polls.
func TestSignalReconfigureIncrements(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	id := uuid.New()
	r := status.NewReader(rdb)

	g0, _ := r.Gen(ctx, id)
	if g0 != 0 {
		t.Fatalf("initial gen = %d, want 0", g0)
	}
	if err := r.SignalReconfigure(ctx, id); err != nil {
		t.Fatalf("signal: %v", err)
	}
	g1, _ := r.Gen(ctx, id)
	if g1 != 1 {
		t.Errorf("gen after signal = %d, want 1", g1)
	}
}
