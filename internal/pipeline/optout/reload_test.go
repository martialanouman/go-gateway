package optout_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
)

// errSuppressions always fails ListSuppressions, to drive the reload-failure path.
type errSuppressions struct{}

func (errSuppressions) ListSuppressions(context.Context) ([]cp.Suppression, error) {
	return nil, errors.New("postgres unavailable")
}

// mutableSuppressions is a SuppressionLister whose set the test can swap between reloads.
type mutableSuppressions struct {
	mu   sync.Mutex
	rows []cp.Suppression
}

func (m *mutableSuppressions) set(msisdns ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = m.rows[:0]
	for _, n := range msisdns {
		m.rows = append(m.rows, cp.Suppression{Scope: cp.SuppressionScopePlatform, MSISDN: n})
	}
}

func (m *mutableSuppressions) ListSuppressions(context.Context) ([]cp.Suppression, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]cp.Suppression(nil), m.rows...), nil
}

// alwaysConfirm is an ExactChecker that confirms any Bloom hit, so a Guard result reflects the Bloom
// snapshot alone (a hit → true, a definitive miss → false without any exact call).
type alwaysConfirm struct{}

func (alwaysConfirm) IsSuppressed(context.Context, cp.SuppressionScope, *uuid.UUID, string) (bool, error) {
	return true, nil
}

// TestGuardReloadAddsAndRemoves: a reload reflects added suppressions (a newly suppressed number now
// blocks) and removed ones (an un-suppressed number passes again — a definitive Bloom miss).
func TestGuardReloadAddsAndRemoves(t *testing.T) {
	ctx := context.Background()
	lister := &mutableSuppressions{}
	lister.set("2250700000001")

	snap, err := optout.LoadSnapshot(ctx, lister)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	g := optout.NewGuard(snap, alwaysConfirm{})

	if ok, _ := g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000001"); !ok {
		t.Fatal("seeded suppression does not block")
	}
	if ok, _ := g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000002"); ok {
		t.Fatal("un-suppressed number blocks before it is added")
	}

	lister.set("2250700000001", "2250700000002")
	if err := g.Reload(ctx, lister); err != nil {
		t.Fatalf("reload (add): %v", err)
	}
	if ok, _ := g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000002"); !ok {
		t.Error("added suppression does not block after reload")
	}

	lister.set("2250700000002")
	if err := g.Reload(ctx, lister); err != nil {
		t.Fatalf("reload (remove): %v", err)
	}
	if ok, _ := g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000001"); ok {
		t.Error("removed suppression still blocks after reload")
	}
}

// TestGuardReloadFailureKeepsOldSnapshot: a build failure returns the error and leaves the current
// snapshot serving — a transient database blip never empties the opt-out gate.
func TestGuardReloadFailureKeepsOldSnapshot(t *testing.T) {
	ctx := context.Background()
	lister := &mutableSuppressions{}
	lister.set("2250700000001")
	snap, err := optout.LoadSnapshot(ctx, lister)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	g := optout.NewGuard(snap, alwaysConfirm{})

	if err := g.Reload(ctx, errSuppressions{}); err == nil {
		t.Fatal("Reload with a failing lister returned nil, want an error")
	}
	if ok, _ := g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000001"); !ok {
		t.Error("failed reload dropped the current snapshot; the seeded suppression no longer blocks")
	}
}

// TestGuardReloadUnderTraffic: continuous IsSuppressed reads while Reload swaps the snapshot must be
// race-free and never see a nil/partial snapshot (no opt-out gap during reload). Run under -race.
func TestGuardReloadUnderTraffic(t *testing.T) {
	ctx := context.Background()
	lister := &mutableSuppressions{}
	lister.set("2250700000001")
	snap, err := optout.LoadSnapshot(ctx, lister)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	g := optout.NewGuard(snap, alwaysConfirm{})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = g.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, "2250700000001")
				}
			}
		}()
	}

	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			lister.set("2250700000002")
		} else {
			lister.set("2250700000001")
		}
		if err := g.Reload(ctx, lister); err != nil {
			t.Errorf("reload: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()
}
