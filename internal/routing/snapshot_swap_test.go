package routing_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/routing"
)

// snapshotFor builds a one-route snapshot (prefix 225 → conn) for swap tests.
func snapshotFor(t *testing.T, conn uuid.UUID) *routing.Snapshot {
	t.Helper()
	snap, err := routing.BuildSnapshot(context.Background(), fakeLister{routes: []cp.Route{
		{ID: uuid.New(), Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
			MatchDestPattern: ptr("225"), TargetConnectorID: &conn},
	}})
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	return snap
}

// TestSwapChangesResolution: after Swap, subsequent reads resolve against the new snapshot.
func TestSwapChangesResolution(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	r := routing.NewResolver(snapshotFor(t, connA))

	got, err := r.Resolve(context.Background(), "+2250700000000")
	if err != nil || got.ConnectorID != connA {
		t.Fatalf("before swap = (%s, %v), want connA %s", got.ConnectorID, err, connA)
	}

	r.Swap(snapshotFor(t, connB))

	got, err = r.Resolve(context.Background(), "+2250700000000")
	if err != nil || got.ConnectorID != connB {
		t.Fatalf("after swap = (%s, %v), want connB %s", got.ConnectorID, err, connB)
	}
}

// TestConcurrentReadsDuringSwap: heavy concurrent resolution while the snapshot is swapped repeatedly
// must never see a partial/zero snapshot and must be race-free (run under -race). Every read returns
// one of the two valid connectors — never the zero UUID, never an error.
func TestConcurrentReadsDuringSwap(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	snapA, snapB := snapshotFor(t, connA), snapshotFor(t, connB)
	r := routing.NewResolver(snapA)

	valid := map[uuid.UUID]bool{connA: true, connB: true}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	const readers = 8
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, err := r.Resolve(context.Background(), "+2250700000000")
				if err != nil || !valid[got.ConnectorID] {
					t.Errorf("read during swap = (%s, %v), want a valid connector", got.ConnectorID, err)
					return
				}
			}
		}()
	}

	// Swapper: flip the served snapshot many times while readers run.
	for i := 0; i < 5000; i++ {
		if i%2 == 0 {
			r.Swap(snapB)
		} else {
			r.Swap(snapA)
		}
	}
	close(stop)
	wg.Wait()
}
