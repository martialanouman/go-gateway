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

func TestRebindRefreshesTTL(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()

	held := bindFor(account)
	if _, err := reg.Bind(ctx, held, 1); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Refresh just before expiry, then advance again: the session survives because the re-Bind pushed
	// the expiry forward. max_sessions is 1 and this is the second Bind of the same member, which must
	// therefore NOT count twice — the rebind rule of bind.lua, on the path production actually uses.
	clk.advance(20 * time.Second)
	if _, err := reg.Bind(ctx, held, 1); err != nil {
		t.Fatalf("rebind: %v — a refresh of a held session must not count against its own quota", err)
	}

	clk.advance(20 * time.Second) // 40s since the first bind, but only 20s since the refresh
	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("lookup after refresh: %d live sessions, want 1", len(live))
	}

	// Once lapsed, the slot is genuinely free: the sweep drops it and Lookup reports nothing.
	clk.advance(31 * time.Second)
	live, err = reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup after expiry: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("lookup after expiry: %d live sessions, want 0", len(live))
	}
}

func TestLookupReturnsLiveSessions(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	// A unique pod: server_integration_test publishes an address for "pod-1" that outlives it by 61 s,
	// and a second run (-count=2) would read it back on these binds.
	pod := "pod-" + uuid.NewString()
	want := map[string]session.Bind{}
	for i := 0; i < 3; i++ {
		b := session.Bind{AccountID: account, PodID: pod, BindID: "bind-" + uuid.NewString()}
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

// TestLookupCarriesEachPodsDialAddress is step-302's core: the return path dials the address the
// registry hands it, never a name it composes itself. Each pod publishes its own address at bind time,
// and Lookup must give it back per pod — an empty one is a bind mo-dlr-router-svc skips, which is
// exactly how the SMPP return channel went silently dark before this step.
//
// The IPv6 address is not decoration: the registry's member separator is ':', so an address is the one
// thing that must never be folded into pod_id.
func TestLookupCarriesEachPodsDialAddress(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	// Pod ids are unique per run, like account ids: redistest shares one Redis across the package, the
	// address key is keyed on pod_id, and it outlives the test by its 61 s TTL. Literal ids would let a
	// second run (-count=2) read the first run's address and stay green with the write removed.
	want := map[string]string{
		"pod-" + uuid.NewString(): "10.1.2.3:7000",
		"pod-" + uuid.NewString(): "[fd00::2]:7000",
	}
	for pod, addr := range want {
		b := session.Bind{AccountID: account, PodID: pod, BindID: "bind-" + uuid.NewString(), Addr: addr}
		if _, err := reg.Bind(ctx, b, 5); err != nil {
			t.Fatalf("bind on %s: %v", pod, err)
		}
	}

	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != len(want) {
		t.Fatalf("lookup: %d sessions, want %d", len(live), len(want))
	}
	for _, b := range live {
		// Assert the pod is one we bound BEFORE comparing addresses: without this, a mutation that
		// emptied PodID would compare want[""] == "" against an empty Addr and pass on both binds.
		expected, known := want[b.PodID]
		if !known {
			t.Fatalf("lookup returned a session on unknown pod %q", b.PodID)
		}
		if b.Addr != expected {
			t.Errorf("session %s on %s: addr %q, want %q — the return path dials what Lookup returns",
				b.BindID, b.PodID, b.Addr, expected)
		}
	}
}

// TestLookupToleratesAPodWithNoAddress is the degradation the rollout relies on, and the reason
// podAddrs inspects each command instead of the pipeline's aggregate error: a pod that published
// nothing yields "" for its own binds WITHOUT failing the lookup, so the other pods' binds still carry
// their address and stay deliverable.
func TestLookupToleratesAPodWithNoAddress(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()

	withAddr := session.Bind{AccountID: account, PodID: "pod-" + uuid.NewString(),
		BindID: "bind-" + uuid.NewString(), Addr: "10.1.2.3:7000"}
	silent := session.Bind{AccountID: account, PodID: "pod-" + uuid.NewString(),
		BindID: "bind-" + uuid.NewString()} // a replica from before step-302
	for _, b := range []session.Bind{withAddr, silent} {
		if _, err := reg.Bind(ctx, b, 5); err != nil {
			t.Fatalf("bind on %s: %v", b.PodID, err)
		}
	}

	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v — a pod without an address must degrade, not fail the whole account", err)
	}
	got := map[string]string{}
	for _, b := range live {
		got[b.PodID] = b.Addr
	}
	if got[withAddr.PodID] != withAddr.Addr {
		t.Errorf("pod with an address: got %q, want %q — one silent pod must not blind the others",
			got[withAddr.PodID], withAddr.Addr)
	}
	if got[silent.PodID] != "" {
		t.Errorf("pod with no address: got %q, want empty", got[silent.PodID])
	}
}

// TestRebindRenewsThePodAddress pins the half of the refresh that is easy to lose. The address expires
// on the session TTL, so a refresh that renews the token without renewing the address keeps a bind live
// while the return path loses the way to reach it — an SMPP channel that stops delivering a minute
// after the bind, with nothing to see. refreshLoop re-Binds, so the renewal has to live in Bind.
//
// The address key is deleted directly rather than waited out: its TTL is 61 s.
func TestRebindRenewsThePodAddress(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()

	b := session.Bind{AccountID: account, PodID: "pod-" + uuid.NewString(),
		BindID: "bind-" + uuid.NewString(), Addr: "10.5.6.7:7000"}
	if _, err := reg.Bind(ctx, b, 5); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := rdb.Del(ctx, "sess:pod:{"+b.PodID+"}").Err(); err != nil {
		t.Fatalf("drop the published address: %v", err)
	}

	// At refreshLoop's cadence: a rebind right after the bind skips the write on purpose (step-304).
	clk.advance(session.DefaultSessionTTL / 2)
	if _, err := reg.Bind(ctx, b, 5); err != nil {
		t.Fatalf("rebind: %v", err)
	}

	live, err := reg.Lookup(ctx, account)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if len(live) != 1 || live[0].Addr != b.Addr {
		t.Errorf("after the refresh, lookup = %+v, want one session carrying addr %q — a refresh that renews "+
			"the token but not the address lets the return path go dark under a live bind", live, b.Addr)
	}
}

// TestRefreshDoesNotRewriteAFreshAddress is step-304: every live bind re-Binds every 30 s, and each
// re-Bind used to SET the same address on the same key. A freshly published address is skipped; one
// older than a quarter TTL is rewritten, which keeps the key alive under any live bind (see the Design
// arrêté of step-304 for the bound).
func TestRefreshDoesNotRewriteAFreshAddress(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()
	pod := "pod-" + uuid.NewString()
	podKey := "sess:pod:{" + pod + "}"
	bind := func(addr string) {
		t.Helper()
		b := session.Bind{AccountID: account, PodID: pod, BindID: "bind-" + uuid.NewString(), Addr: addr}
		if _, err := reg.Bind(ctx, b, 100); err != nil {
			t.Fatalf("Bind: %v", err)
		}
	}
	published := func() string {
		t.Helper()
		v, err := rdb.Get(ctx, podKey).Result()
		if err != nil {
			return ""
		}
		return v
	}

	bind("10.0.0.1:7000")
	if got := published(); got != "10.0.0.1:7000" {
		t.Fatalf("address after the first bind = %q, want it published", got)
	}

	// Deleting the key makes a skipped write observable: a rewrite would put it back.
	if err := rdb.Del(ctx, podKey).Err(); err != nil {
		t.Fatalf("drop the published address: %v", err)
	}
	clk.advance(session.DefaultSessionTTL/4 - time.Second)
	for range 5 {
		bind("10.0.0.1:7000")
	}
	if got := published(); got != "" {
		t.Errorf("address rewritten to %q within a quarter TTL of its publication, want the write skipped", got)
	}

	bind("10.0.0.2:7000")
	if got := published(); got != "10.0.0.2:7000" {
		t.Errorf("a changed address = %q, want it published at once", got)
	}

	if err := rdb.Del(ctx, podKey).Err(); err != nil {
		t.Fatalf("drop the published address: %v", err)
	}
	clk.advance(session.DefaultSessionTTL / 4)
	bind("10.0.0.2:7000")
	if got := published(); got != "10.0.0.2:7000" {
		t.Errorf("address = %q a quarter TTL after its publication, want it rewritten", got)
	}
}

// TestAFailedAddressWriteIsRetriedOnTheNextBind: only a SET that landed may suppress the next one, or a
// Redis blip would leave the address missing for a quarter TTL under live binds.
func TestAFailedAddressWriteIsRetriedOnTheNextBind(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	account := uuid.NewString()
	pod := "pod-" + uuid.NewString()
	b := session.Bind{AccountID: account, PodID: pod, BindID: "bind-" + uuid.NewString(), Addr: "10.0.0.1:7000"}

	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reg.Bind(dead, b, 100); err == nil {
		t.Fatal("Bind on a cancelled context succeeded, want the address write to fail")
	}
	ctx := context.Background()
	if _, err := reg.Bind(ctx, b, 100); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got, _ := rdb.Get(ctx, "sess:pod:{"+pod+"}").Result(); got != "10.0.0.1:7000" {
		t.Errorf("address after a failed then a good Bind = %q, want it published", got)
	}
}
