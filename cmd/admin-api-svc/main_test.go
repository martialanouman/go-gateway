package main

import (
	"strings"
	"testing"
	"time"
)

// TestRunRequiresAdminTokensInProduction pins the operator-token policy: a production Admin API with
// no HTTP_ADMIN_TOKENS must fail the boot (fast, before touching Postgres) rather than come up and
// answer every request with 401. The guard runs right after config.Load, so no database is needed.
func TestRunRequiresAdminTokensInProduction(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	// Non-localhost dependency addresses so the production config guards pass; the pool and the
	// session-manager client are never opened because the token check fires first. SMPP_SESSION_MANAGER_ADDR
	// is required now that the Admin API calls session-manager to force-disconnect on revoke/suspend (step-032).
	t.Setenv("POSTGRES_URL", "postgres://u:p@db.internal:5432/gw")
	t.Setenv("SMPP_SESSION_MANAGER_ADDR", "session-manager.internal:7000")
	t.Setenv("BILLING_ADDR", "billing.internal:7001")
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse.internal:9000")
	t.Setenv("CONTENT_KEY_ADDR", "content-key.internal:7002")
	t.Setenv("REDIS_URL", "redis://redis.internal:6379")
	t.Setenv("KAFKA_BROKERS", "kafka.internal:9092")
	// HTTP_ADMIN_TOKENS deliberately unset.

	err := run()
	if err == nil {
		t.Fatal("run() = nil, want a boot failure for missing admin tokens in production")
	}
	if !strings.Contains(err.Error(), "HTTP_ADMIN_TOKENS") {
		t.Errorf("error %q should name HTTP_ADMIN_TOKENS", err)
	}
}

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
