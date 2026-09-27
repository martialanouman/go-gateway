package optout_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// gatedSuppressions returns the rows current when ListSuppressions is CALLED, but holds the first call
// until release is closed — a reload that read the table just before a STOP committed and is still
// building its filter.
type gatedSuppressions struct {
	mutableSuppressions
	// A flag, not a sync.Once: Once.Do makes concurrent callers wait for the first, which would hold the
	// fresh reload too and serialise the two reloads for the test instead of for the code.
	gated   atomic.Bool
	reading chan struct{}
	release chan struct{}
}

func (g *gatedSuppressions) ListSuppressions(ctx context.Context) ([]cp.Suppression, error) {
	rows, err := g.mutableSuppressions.ListSuppressions(ctx)
	if g.gated.CompareAndSwap(false, true) {
		close(g.reading)
		<-g.release
	}
	return rows, err
}

// TestEnforcerReloadKeepsTheFreshestRead: the router reloads the opt-out filter from two watchers — the
// full config rebuild and the STOP announcement (step-398). A full rebuild that read suppressions just
// before a STOP committed must not swap its stale filter in AFTER the STOP watcher installed the fresh
// one: that is exactly the false negative §6.20 forbids, a recipient who said STOP receiving MTs again.
func TestEnforcerReloadKeepsTheFreshestRead(t *testing.T) {
	ctx := context.Background()
	const stopped = "2250700000001"
	lister := &gatedSuppressions{reading: make(chan struct{}), release: make(chan struct{})}
	snap, err := optout.LoadSnapshot(ctx, &lister.mutableSuppressions)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	e := optout.NewEnforcer(optout.NewGuard(snap, alwaysConfirm{}), nil)
	inbound := fakeInboundLister{}

	staleDone := make(chan error, 1)
	go func() { staleDone <- e.Reload(ctx, lister, inbound) }()
	<-lister.reading // the full rebuild has read the table WITHOUT the STOP

	lister.set(stopped) // the STOP commits
	freshDone := make(chan error, 1)
	go func() { freshDone <- e.Reload(ctx, lister, inbound) }()
	// Unserialised, the fresh reload completes now and the stale one lands on top of it below.
	select {
	case <-freshDone:
		freshDone <- nil
	// Generous on purpose: this wait only has to outlast the fresh reload when the lock is ABSENT, and a
	// short one would let that mutation pass on a loaded -race runner.
	case <-time.After(time.Second):
	}

	close(lister.release)
	for _, done := range []chan error{staleDone, freshDone} {
		if err := <-done; err != nil {
			t.Fatalf("Reload: %v", err)
		}
	}
	blocked, err := e.IsOptedOut(ctx, uuid.New(), uuid.New(), "ACME", stopped)
	if err != nil {
		t.Fatalf("IsOptedOut: %v", err)
	}
	if !blocked {
		t.Error("the stale reload overwrote the fresh one: a recipient who sent STOP is no longer opted out")
	}
}
