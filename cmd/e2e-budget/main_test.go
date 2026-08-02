package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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

// TestBaselineRoundTripsThroughAFile exercises what the unit tests above cannot: the command's only
// real path. run() scrapes, writes a baseline, scrapes again and scores the difference — and every
// step of that had never been executed once. A histogram always carries a +Inf bucket, which
// encoding/json refuses outright, so the documented workflow failed on its first command.
func TestBaselineRoundTripsThroughAFile(t *testing.T) {
	var exposition atomic.Pointer[string]
	set := func(s string) { exposition.Store(&s) }
	set(e2eExposition(map[float64]uint64{0.4: 10, 2: 10, math.Inf(1): 10}, 10, 1.0))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, *exposition.Load())
	}))
	t.Cleanup(srv.Close)

	baseline := filepath.Join(t.TempDir(), "baseline.json")
	o := opts{metricsURL: srv.URL, baseline: baseline, quantile: 0.99, budget: 2 * time.Second, timeout: 5 * time.Second}

	if err := run(o); err != nil {
		t.Fatalf("recording a baseline: %v", err)
	}
	if _, err := os.Stat(baseline); err != nil {
		t.Fatalf("baseline file: %v", err)
	}

	// The run adds 100 observations, all of them inside the 0.4s bucket: comfortably under budget.
	set(e2eExposition(map[float64]uint64{0.4: 110, 2: 110, math.Inf(1): 110}, 110, 11.0))

	o.check = true
	if err := run(o); err != nil {
		t.Fatalf("checking against the baseline: %v", err)
	}
}

// TestCheckFailsWhenTheRunBlewTheBudget is the other half: the command must not report a held budget
// when the window's observations sit above it. Without this, the round trip above could pass with a
// scoring step that always returns nil.
func TestCheckFailsWhenTheRunBlewTheBudget(t *testing.T) {
	var exposition atomic.Pointer[string]
	set := func(s string) { exposition.Store(&s) }
	set(e2eExposition(map[float64]uint64{0.4: 10, 2: 10, math.Inf(1): 10}, 10, 1.0))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, *exposition.Load())
	}))
	t.Cleanup(srv.Close)

	baseline := filepath.Join(t.TempDir(), "baseline.json")
	o := opts{metricsURL: srv.URL, baseline: baseline, quantile: 0.99, budget: 2 * time.Second, timeout: 5 * time.Second}
	if err := run(o); err != nil {
		t.Fatalf("recording a baseline: %v", err)
	}

	// 100 observations added, none of which reached the 2s bucket: the whole run is above budget.
	set(e2eExposition(map[float64]uint64{0.4: 10, 2: 10, math.Inf(1): 110}, 110, 400.0))

	o.check = true
	err := run(o)
	if err == nil {
		t.Fatal("run(-check) = nil, want a failure — every observation of the window was over budget")
	}
	if !strings.Contains(err.Error(), "over budget") {
		t.Errorf("error = %q, want it to name the budget", err)
	}
}

// e2eExposition renders a Prometheus text exposition of message_e2e_duration_seconds with the given
// cumulative bucket counts. Written by hand rather than through the catalogue so the test controls the
// exact shape it feeds the reader.
func e2eExposition(buckets map[float64]uint64, count uint64, sum float64) string {
	var b strings.Builder
	b.WriteString("# HELP message_e2e_duration_seconds probe\n")
	b.WriteString("# TYPE message_e2e_duration_seconds histogram\n")
	edges := make([]float64, 0, len(buckets))
	for le := range buckets {
		edges = append(edges, le)
	}
	sort.Float64s(edges)
	for _, le := range edges {
		bound := strconv.FormatFloat(le, 'g', -1, 64)
		if math.IsInf(le, 1) {
			bound = "+Inf"
		}
		fmt.Fprintf(&b, "message_e2e_duration_seconds_bucket{connector_id=\"c1\",status=\"ok\",le=\"%s\"} %d\n", bound, buckets[le])
	}
	fmt.Fprintf(&b, "message_e2e_duration_seconds_sum{connector_id=\"c1\",status=\"ok\"} %g\n", sum)
	fmt.Fprintf(&b, "message_e2e_duration_seconds_count{connector_id=\"c1\",status=\"ok\"} %d\n", count)
	return b.String()
}
