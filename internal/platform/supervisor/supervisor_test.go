package supervisor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestGroupCleanShutdown: cancelling the parent context stops every component and Run returns nil.
func TestGroupCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var started atomic.Int32
	block := func(c context.Context) error {
		started.Add(1)
		<-c.Done()
		return nil
	}

	var g supervisor.Group
	g.Add("a", block)
	g.Add("b", block)

	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, quietLogger()) }()

	// Let both components start, then trigger a graceful shutdown.
	waitFor(t, func() bool { return started.Load() == 2 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// TestGroupFirstErrorWins: a failing component brings the group down, its error is returned wrapped
// with the component name, and the other component is cancelled.
func TestGroupFirstErrorWins(t *testing.T) {
	boom := errors.New("boom")

	var peerCancelled atomic.Bool
	peer := func(c context.Context) error {
		<-c.Done()
		peerCancelled.Store(true)
		return nil
	}
	failing := func(context.Context) error { return boom }

	var g supervisor.Group
	g.Add("peer", peer)
	g.Add("failing", failing)

	err := g.Run(context.Background(), quietLogger())
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want it to wrap boom", err)
	}
	if !peerCancelled.Load() {
		t.Error("the surviving component was not cancelled when its peer failed")
	}
}

// TestOrderedDrainsInReverseRegistrationOrder: on a graceful shutdown the components stop in the
// reverse of their registration order (the last added drains first), and Run returns nil.
func TestOrderedDrainsInReverseRegistrationOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var mu sync.Mutex
	var stopped []string
	var started atomic.Int32
	comp := func(name string) supervisor.Component {
		return func(c context.Context) error {
			started.Add(1)
			<-c.Done()
			mu.Lock()
			stopped = append(stopped, name)
			mu.Unlock()
			return nil
		}
	}

	var o supervisor.Ordered
	o.Add("a", comp("a"))
	o.Add("b", comp("b"))
	o.Add("c", comp("c"))

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, quietLogger()) }()

	waitFor(t, func() bool { return started.Load() == 3 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"c", "b", "a"}; !reflect.DeepEqual(stopped, want) {
		t.Errorf("drain order = %v, want reverse registration %v", stopped, want)
	}
}

// TestOrderedFirstErrorWins: a failing component brings the ordered group down, its error is returned
// wrapped with the component name, and a surviving peer is drained.
func TestOrderedFirstErrorWins(t *testing.T) {
	boom := errors.New("boom")

	var peerCancelled atomic.Bool
	peer := func(c context.Context) error {
		<-c.Done()
		peerCancelled.Store(true)
		return nil
	}
	failing := func(context.Context) error { return boom }

	var o supervisor.Ordered
	o.Add("peer", peer)
	o.Add("failing", failing)

	err := o.Run(context.Background(), quietLogger())
	if !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want it to wrap boom", err)
	}
	if !peerCancelled.Load() {
		t.Error("the surviving component was not drained when its peer failed")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestOrderedRunsTheDrainHookBeforeAnyComponent: the hook registered with OnDrain runs at the START of
// the drain, before the first component is cancelled.
//
// The assertion records the hook and the components into ONE ordered slice rather than checking a
// boolean, because "the hook ran" is not the property that matters: a hook that fires after the
// listener has already closed removes the pod from the load balancer too late, which is precisely the
// race this hook exists to close. Only the position proves it.
func TestOrderedRunsTheDrainHookBeforeAnyComponent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var mu sync.Mutex
	var events []string
	var started atomic.Int32
	comp := func(name string) supervisor.Component {
		return func(c context.Context) error {
			started.Add(1)
			<-c.Done()
			mu.Lock()
			events = append(events, name)
			mu.Unlock()
			return nil
		}
	}

	var o supervisor.Ordered
	o.OnDrain(func(context.Context) {
		mu.Lock()
		events = append(events, "hook")
		mu.Unlock()
	})
	o.Add("a", comp("a"))
	o.Add("b", comp("b"))

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, quietLogger()) }()

	waitFor(t, func() bool { return started.Load() == 2 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"hook", "b", "a"}; !reflect.DeepEqual(events, want) {
		t.Errorf("drain sequence = %v, want %v: the hook must run before the first component is "+
			"drained, or /readyz flips to 503 after the listener has already closed", events, want)
	}
}

// TestGroupRunsTheDrainHookBeforeCancelling: same guarantee on the unordered supervisor, which
// router-svc, billing-svc and connector-pool-svc use. They too must leave the load balancer first.
func TestGroupRunsTheDrainHookBeforeCancelling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var mu sync.Mutex
	var events []string
	var started atomic.Int32

	var g supervisor.Group
	g.OnDrain(func(context.Context) {
		mu.Lock()
		events = append(events, "hook")
		mu.Unlock()
	})
	g.Add("a", func(c context.Context) error {
		started.Add(1)
		<-c.Done()
		mu.Lock()
		events = append(events, "a")
		mu.Unlock()
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, quietLogger()) }()

	waitFor(t, func() bool { return started.Load() == 1 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"hook", "a"}; !reflect.DeepEqual(events, want) {
		t.Errorf("drain sequence = %v, want %v", events, want)
	}
}

// TestDrainHooksRunOnComponentFailureToo pins a case the drain hook's own name does not suggest: the
// hooks run when a COMPONENT FAILS, not only on SIGTERM.
//
// It is intended, and the reason is the runtime failure rather than the startup one. When a consumer
// dies under load the pod is still in the Service endpoints and still being handed work; announcing
// itself not-ready before tearing the rest down is exactly right. The supervisor cannot tell that case
// from a boot failure — distinguishing them means knowing whether this pod was ever ready, which lives
// above it — so both pay the drain delay, and a pod that fails to bind its port takes DRAIN_DELAY
// longer to report it. That is the accepted cost, not an oversight.
//
// Without this test the behaviour is deliberate but invisible, which is indistinguishable from an
// accident: the next reader could "fix" it in either direction and break the runtime case silently.
func TestDrainHooksRunOnComponentFailureToo(t *testing.T) {
	var g supervisor.Group
	var ran atomic.Int32
	g.OnDrain(func(context.Context) { ran.Add(1) })
	g.Add("boom", func(context.Context) error { return errors.New("bind: address already in use") })

	err := g.Run(context.Background(), quietLogger())
	if err == nil {
		t.Fatal("Run() = nil, want the component's failure")
	}
	if got := ran.Load(); got != 1 {
		t.Errorf("drain hook ran %d times on a component failure, want exactly 1: a pod whose "+
			"consumer dies under load is still in the load balancer, and must leave it before the "+
			"rest tears down", got)
	}
}
