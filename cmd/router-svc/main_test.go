package main

import (
	"net"
	"testing"
	"time"
)

// TestRunExitsWhenTheOpsServerCannotStart pins the supervision contract run's component block
// states: a component that fails on its own must bring the service down, not leave a pod alive
// with a dead ops port that no probe can reach.
//
// The failure is a bind collision because that is the one an operator actually meets — a sidecar
// on the same port, or a previous pod that has not released it yet. No signal is ever sent: run
// must return on the component failure alone. It blocked here until the errCh select landed.
func TestRunExitsWhenTheOpsServerCannotStart(t *testing.T) {
	// The wildcard address, not 127.0.0.1: the ops server binds ":port", and a loopback listener
	// does not collide with a wildcard one — the bind would succeed and this test would prove
	// nothing.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	t.Setenv("OPS_PORT", port)
	// No collector is listening here, and a real exporter would make the drain wait out its whole
	// timeout on a flush that can never land.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")

	done := make(chan error, 1)
	go func() { done <- run() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run() = nil, want the ops server's bind failure")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run() never returned: a failed ops server left the service running")
	}
}
