package ciguard

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// fakeTB records what the guard did to a test, which is the only way to assert a skip or a failure:
// a real *testing.T cannot report that it was skipped without actually skipping this test too.
type fakeTB struct {
	skipped string
	failed  string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Skipf(format string, args ...any) {
	f.skipped = fmt.Sprintf(format, args...)
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.failed = fmt.Sprintf(format, args...)
}

// TestSkipIsASkipOutsideCI is the half that protects everyday work: a laptop with Docker off must still
// run `make test` to completion. A guard that failed everywhere would make the integration suite
// unrunnable for anyone not on a CI runner, which is a worse outcome than the hole it closes.
func TestSkipIsASkipOutsideCI(t *testing.T) {
	tb := &fakeTB{}
	skip(tb, false, "no docker here (%s)", "laptop")

	if tb.failed != "" {
		t.Errorf("outside CI the guard failed the test (%q); skipping is the correct behaviour there", tb.failed)
	}
	if !strings.Contains(tb.skipped, "no docker here (laptop)") {
		t.Errorf("skip reason = %q, want the caller's reason", tb.skipped)
	}
}

// TestSkipIsAFailureInCI is the half that closes the hole. In CI an integration test that skips is
// indistinguishable from one that passed, which is exactly how ten resilience tests went a whole
// milestone without ever running (step-250b).
func TestSkipIsAFailureInCI(t *testing.T) {
	tb := &fakeTB{}
	skip(tb, true, "image %q not available", "smsc-simulator:dev")

	if tb.skipped != "" {
		t.Errorf("in CI the guard skipped (%q): a skipped integration test reads exactly like a passing "+
			"one, which is the silence this guard exists to break", tb.skipped)
	}
	if !strings.Contains(tb.failed, `image "smsc-simulator:dev" not available`) {
		t.Errorf("failure message = %q, want the caller's reason so the operator knows what is missing", tb.failed)
	}
}

// TestRequireDockerNamesTheProviderFault: when Docker cannot be reached the verdict must carry the
// underlying error. "docker unusable" with no cause sends an operator to read the guard's source
// instead of their daemon.
func TestRequireDockerNamesTheProviderFault(t *testing.T) {
	boom := errors.New("cannot connect to the docker daemon")
	tb := &fakeTB{}
	requireDocker(tb, true, func() error { return boom })
	if !strings.Contains(tb.failed, boom.Error()) {
		t.Errorf("failure = %q, want it to name the provider fault %q", tb.failed, boom)
	}

	healthy := &fakeTB{}
	requireDocker(healthy, true, func() error { return nil })
	if healthy.failed != "" || healthy.skipped != "" {
		t.Errorf("a healthy provider must neither fail nor skip, got failed=%q skipped=%q",
			healthy.failed, healthy.skipped)
	}
}

// TestSkipIsFatalReadsTheCIEnvironment pins WHICH signal decides. Reading the wrong variable would
// leave the guard permanently in laptop mode on the very runner it was written for, and nothing else
// in the suite would notice.
func TestSkipIsFatalReadsTheCIEnvironment(t *testing.T) {
	t.Setenv("CI", "")
	if skipIsFatal() {
		t.Error("with CI unset the guard must allow skips")
	}
	t.Setenv("CI", "true")
	if !skipIsFatal() {
		t.Error("with CI=true the guard must turn skips into failures")
	}
}
