package supervisor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
