package session_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// clock is a controllable clock so TTL expiry is tested by advancing time, not by sleeping.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func bindFor(accountID string) session.Bind {
	return session.Bind{AccountID: accountID, PodID: "pod-1", BindID: "bind-" + uuid.NewString()}
}

func TestBindEnforcesMaxSessions(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	first := bindFor(account)
	active, err := reg.Bind(ctx, first, 2)
	if err != nil || active != 1 {
		t.Fatalf("first bind: active=%d err=%v, want active=1 err=nil", active, err)
	}

	active, err = reg.Bind(ctx, bindFor(account), 2)
	if err != nil || active != 2 {
		t.Fatalf("second bind: active=%d err=%v, want active=2 err=nil", active, err)
	}

	// The third bind is over the max_sessions=2 quota — invariant (d).
	active, err = reg.Bind(ctx, bindFor(account), 2)
	if !errors.Is(err, errs.ErrMaxSessionsExceeded) {
		t.Fatalf("third bind: err=%v, want ErrMaxSessionsExceeded", err)
	}
	if active != 2 {
		t.Fatalf("third bind: active=%d, want 2 (quota unchanged)", active)
	}
}

func TestUnbindFreesSlot(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	held := bindFor(account)
	if _, err := reg.Bind(ctx, held, 1); err != nil {
		t.Fatalf("bind: %v", err)
	}

	if _, err := reg.Bind(ctx, bindFor(account), 1); !errors.Is(err, errs.ErrMaxSessionsExceeded) {
		t.Fatalf("bind over quota: err=%v, want ErrMaxSessionsExceeded", err)
	}

	removed, err := reg.Unbind(ctx, held)
	if err != nil || !removed {
		t.Fatalf("unbind: removed=%v err=%v, want removed=true err=nil", removed, err)
	}

	// The freed slot lets a new bind through.
	if _, err := reg.Bind(ctx, bindFor(account), 1); err != nil {
		t.Fatalf("bind after unbind: %v", err)
	}
}

func TestExpiryFreesSlot(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()

	if _, err := reg.Bind(ctx, bindFor(account), 1); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Still within the TTL: the slot is held.
	clk.advance(20 * time.Second)
	if _, err := reg.Bind(ctx, bindFor(account), 1); !errors.Is(err, errs.ErrMaxSessionsExceeded) {
		t.Fatalf("bind within TTL: err=%v, want ErrMaxSessionsExceeded", err)
	}

	// Past the TTL: the lapsed session is swept, so a new bind is admitted.
	clk.advance(11 * time.Second)
	if _, err := reg.Bind(ctx, bindFor(account), 1); err != nil {
		t.Fatalf("bind after expiry: %v", err)
	}

	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("lookup after expiry: %d live sessions, want 1", len(live))
	}
}

func TestTouchRefreshesTTL(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()

	held := bindFor(account)
	if _, err := reg.Bind(ctx, held, 1); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Refresh just before expiry, then advance again: the session survives because Touch pushed the
	// expiry forward.
	clk.advance(20 * time.Second)
	refreshed, err := reg.Touch(ctx, held)
	if err != nil || !refreshed {
		t.Fatalf("touch: refreshed=%v err=%v, want refreshed=true err=nil", refreshed, err)
	}

	clk.advance(20 * time.Second) // 40s since bind, but only 20s since touch
	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("lookup after touch: %d live sessions, want 1", len(live))
	}

	// Touch on a lapsed session does not resurrect it.
	clk.advance(31 * time.Second)
	refreshed, err = reg.Touch(ctx, held)
	if err != nil {
		t.Fatalf("touch lapsed: %v", err)
	}
	if refreshed {
		t.Fatal("touch lapsed: refreshed=true, want false")
	}
}

func TestLookupReturnsLiveSessions(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	want := map[string]session.Bind{}
	for i := 0; i < 3; i++ {
		b := session.Bind{AccountID: account, PodID: "pod-1", BindID: "bind-" + uuid.NewString()}
		if _, err := reg.Bind(ctx, b, 5); err != nil {
			t.Fatalf("bind %d: %v", i, err)
		}
		want[b.BindID] = b
	}

	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != len(want) {
		t.Fatalf("lookup: %d sessions, want %d", len(live), len(want))
	}
	for _, b := range live {
		w, ok := want[b.BindID]
		if !ok || w != b {
			t.Fatalf("lookup returned unexpected session %+v", b)
		}
	}
}

// TestConcurrentBindsRespectQuota proves the quota holds atomically: with max_sessions=1, N parallel
// binds yield exactly one success. Run under -race.
func TestConcurrentBindsRespectQuota(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	const n = 32
	var (
		wg        sync.WaitGroup
		successes atomic.Int64
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines at once to maximise contention
			_, err := reg.Bind(ctx, bindFor(account), 1)
			switch {
			case err == nil:
				successes.Add(1)
			case errors.Is(err, errs.ErrMaxSessionsExceeded):
				// expected rejection
			default:
				t.Errorf("unexpected bind error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := successes.Load(); got != 1 {
		t.Fatalf("concurrent binds on max_sessions=1: %d succeeded, want exactly 1", got)
	}
}
