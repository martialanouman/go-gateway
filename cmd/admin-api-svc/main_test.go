package main

import (
	"testing"
	"time"
)

// TestRunExitsWhenPostgresIsUnreachable pins the boot contract: Postgres is vital to the Admin API
// (plan §1.5), so NewPool pings it eagerly and run must fail fast rather than start a server that
// answers every request with an error. No signal is sent — run returns on the boot failure alone.
func TestRunExitsWhenPostgresIsUnreachable(t *testing.T) {
	// A refused connection resolves immediately, so the ping fails without waiting out its timeout.
	t.Setenv("POSTGRES_URL", "postgres://gateway:gateway@127.0.0.1:1/gateway?sslmode=disable")
	t.Setenv("POSTGRES_TIMEOUT", "1s")
	// No collector is listening; a real exporter would stall the drain on a flush that never lands.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")

	done := make(chan error, 1)
	go func() { done <- run() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run() = nil, want the unreachable-Postgres boot failure")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run() never returned: an unreachable vital dependency did not fail the boot")
	}
}
