// Package ciguard turns a skipped integration test into a failure when the suite runs in CI.
//
// The rule it enforces: OUTSIDE CI, skipping is right — a laptop with Docker off must still run
// `make test` to completion, and the integration helpers (redistest, pgtest, kafkatest, chtest,
// smscsim) all skip cleanly for that reason. IN CI, skipping is a lie. A skipped test is reported
// exactly like a passing one, so a dependency that silently goes missing takes its whole suite with it
// and nothing turns red.
//
// That is not hypothetical. Ten M8 resilience tests went a full milestone without ever running in CI,
// because the workflow never built the SMSC simulator image and every one of them skipped for want of
// it (found in step-250b, closed in step-250c). The same shape is latent in every other helper: were
// Docker to break on a runner, testcontainers.SkipIfProviderIsNotHealthy would skip the entire
// integration suite and the pipeline would stay green.
package ciguard

import (
	"context"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// tb is the slice of testing.TB the guard needs. It exists so the guard's own behaviour can be
// asserted by a double: a real *testing.T cannot report that it was skipped without skipping the test
// doing the asserting.
type tb interface {
	Helper()
	Skipf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// skipIsFatal reports whether a skip must be escalated to a failure. CI is the de-facto standard
// variable and is set to "true" by GitHub Actions; any non-empty value counts, so a local shell that
// exports it gets the strict behaviour it asked for.
func skipIsFatal() bool { return os.Getenv("CI") != "" }

// skip is the decision, split from Skip so a test can drive both branches without needing a real CI.
func skip(t tb, fatal bool, format string, args ...any) {
	t.Helper()
	if fatal {
		t.Fatalf("this test may not be skipped in CI: "+format, args...)
		return
	}
	t.Skipf(format, args...)
}

// Skip ends the test as skipped, or fails it when running in CI. Callers pass the reason they would
// have passed to t.Skipf; it is reported either way, because an operator staring at a red CI needs to
// know WHICH dependency was missing.
func Skip(t *testing.T, format string, args ...any) {
	t.Helper()
	skip(t, skipIsFatal(), format, args...)
}

// RequireDocker ensures a healthy Docker provider, skipping without one — or failing in CI.
//
// It replaces testcontainers.SkipIfProviderIsNotHealthy, which can only ever skip. It performs the
// same two checks that function does (ProviderDocker.GetProvider, then provider.Health) and routes the
// verdict through Skip instead, so an unreachable Docker daemon cannot silence the integration suite
// on a runner.
func RequireDocker(t *testing.T) {
	t.Helper()
	requireDocker(t, skipIsFatal(), dockerHealth)
}

// requireDocker is the testable body: health is injected so a test can drive the faulty branch without
// having to break the local Docker daemon.
func requireDocker(t tb, fatal bool, health func() error) {
	t.Helper()
	if err := health(); err != nil {
		skip(t, fatal, "docker is not usable: %v", err)
	}
}

// dockerHealth reports whether a Docker provider can be obtained and answers a health check.
func dockerHealth() error {
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		return err
	}
	return provider.Health(context.Background())
}
