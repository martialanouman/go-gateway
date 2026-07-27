package exact

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

// errLister always fails List, to drive the reload-failure path.
type errLister struct{}

func (errLister) List(context.Context, string, int) ([]Route, error) {
	return nil, errors.New("postgres unavailable")
}

// mutableLister is a paging MSISDNLister whose set the test can swap between reloads.
type mutableLister struct {
	mu      sync.Mutex
	msisdns []string
}

func (l *mutableLister) set(m []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msisdns = append([]string(nil), m...)
	sort.Strings(l.msisdns)
}

func (l *mutableLister) List(_ context.Context, after string, limit int) ([]Route, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Route, 0, limit)
	for _, m := range l.msisdns {
		if m > after {
			out = append(out, Route{MSISDN: m})
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// TestBloomReloadAddsAndRemoves: a reload reflects added numbers (a new number becomes a possible-hit)
// and dropped numbers (a removed number is a definitive miss again).
func TestBloomReloadAddsAndRemoves(t *testing.T) {
	ctx := context.Background()
	lister := &mutableLister{}
	lister.set([]string{"2250700000001"})

	b, err := LoadBloom(ctx, lister)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}
	if !b.MightContain("2250700000001") {
		t.Fatal("seeded number is not a possible-hit")
	}
	if b.MightContain("2250700000002") {
		t.Fatal("unseeded number is a possible-hit before it is added (unexpected collision)")
	}

	// Add a number, reload: it becomes a possible-hit (no false negatives).
	lister.set([]string{"2250700000001", "2250700000002"})
	if err := b.Reload(ctx, lister); err != nil {
		t.Fatalf("reload (add): %v", err)
	}
	if !b.MightContain("2250700000002") {
		t.Error("added number is not a possible-hit after reload")
	}

	// Remove the original, reload: it is a definitive miss again.
	lister.set([]string{"2250700000002"})
	if err := b.Reload(ctx, lister); err != nil {
		t.Fatalf("reload (remove): %v", err)
	}
	if b.MightContain("2250700000001") {
		t.Error("removed number is still a possible-hit after reload")
	}
}

// TestBloomReloadFailureKeepsOldFilter: a build failure returns the error and leaves the current
// filter serving — a transient Postgres blip never empties the L0 gate.
func TestBloomReloadFailureKeepsOldFilter(t *testing.T) {
	ctx := context.Background()
	lister := &mutableLister{}
	lister.set([]string{"2250700000001"})
	b, err := LoadBloom(ctx, lister)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}

	if err := b.Reload(ctx, errLister{}); err == nil {
		t.Fatal("Reload with a failing lister returned nil, want an error")
	}
	if !b.MightContain("2250700000001") {
		t.Error("failed reload dropped the current filter; the seeded number is no longer a possible-hit")
	}
}

// TestBloomReloadUnderTraffic: continuous MightContain reads while Reload swaps the filter must be
// race-free and never see a nil/partial filter (the M7 acceptance criterion: no routing hole). Run
// under -race.
func TestBloomReloadUnderTraffic(t *testing.T) {
	ctx := context.Background()
	a := &mutableLister{}
	a.set([]string{"2250700000001"})
	bList := &mutableLister{}
	bList.set([]string{"2250700000002", "2250700000003"})

	b, err := LoadBloom(ctx, a)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}

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
					// The result may be either filter's answer, but the call must never panic or race.
					_ = b.MightContain("2250700000002")
				}
			}
		}()
	}

	for i := 0; i < 1000; i++ {
		src := a
		if i%2 == 0 {
			src = bList
		}
		if err := b.Reload(ctx, src); err != nil {
			t.Errorf("reload: %v", err)
			break
		}
	}
	close(stop)
	wg.Wait()

	// After the last reload (i=999 is odd → src=a), a possible-hit for a's member holds — the swap took.
	if !b.MightContain("2250700000001") {
		t.Error("final reload did not take effect")
	}
}
