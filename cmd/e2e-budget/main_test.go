package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/gatewaymetrics"
)

// The exit rule is the whole point of this command: a verdict that is not a proven pass must not exit
// zero. Indeterminate is the case that matters — the exposition did not resolve the budget, and
// treating "not proven to fail" as success is how a load run reports a budget it never measured.
func TestVerdictExitRule(t *testing.T) {
	q := gatewaymetrics.Quantile{Lower: time.Second, Upper: 2 * time.Second}

	tests := []struct {
		name    string
		verdict gatewaymetrics.Verdict
		wantErr bool
		wantMsg string
	}{
		{"pass is the only success", gatewaymetrics.Pass, false, ""},
		{"fail is a failure", gatewaymetrics.Fail, true, "over budget"},
		{"indeterminate is a failure, not a pass", gatewaymetrics.Indeterminate, true, "did not resolve"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verdictError(tc.verdict, q, 2*time.Second)
			if tc.wantErr && err == nil {
				t.Fatalf("verdictError(%v) = nil, want a failure", tc.verdict)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("verdictError(%v) = %v, want nil", tc.verdict, err)
			}
			if tc.wantErr && !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("verdictError(%v) = %q, want it to say %q", tc.verdict, err, tc.wantMsg)
			}
		})
	}
}

// An unknown verdict must not be read as success either. Verdict is a string type, so a future value
// added to the package would otherwise fall through whatever branch happens to be last.
func TestUnknownVerdictIsNotASuccess(t *testing.T) {
	err := verdictError(gatewaymetrics.Verdict("something-new"), gatewaymetrics.Quantile{}, time.Second)
	if err == nil {
		t.Fatal("verdictError of an unknown verdict = nil, want a failure")
	}
	if !strings.Contains(err.Error(), "something-new") {
		t.Errorf("error = %q, want it to name the unknown verdict", err)
	}
}

// A run that observed nothing is the failure mode this command exists to catch: an unfed or
// unregistered metric, or a gateway that took no traffic. It must never be reported as a budget held.
func TestNoObservationsIsAFailure(t *testing.T) {
	err := checkBudget(gatewaymetrics.Histogram{}, 0.99, 2*time.Second)
	if err == nil {
		t.Fatal("checkBudget on an empty histogram = nil, want a failure")
	}
	if !errors.Is(err, gatewaymetrics.ErrNoObservations) {
		t.Errorf("error = %v, want it to wrap ErrNoObservations", err)
	}
}
