package main

import (
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/ceiling"
)

// recordable is a sweep whose tiers were measured long enough for their figures to be worth writing
// down. Everything below varies one thing from it at a time.
func recordable() ceiling.Result {
	return ceiling.Result{
		Tiers: []ceiling.Tier{
			{Binds: 10, Status: ceiling.TierCounted},
			{Binds: 20, Status: ceiling.TierCounted},
		},
		Ceiling:          12_430,
		CeilingBinds:     80,
		ReferenceBinds:   20,
		ReferenceCeiling: 3_329,
		Measure:          ceiling.MinRecordableMeasure,
	}
}

func joined(res ceiling.Result) string { return strings.Join(reportLines(res), "\n") }

// TestReportRefusesToCallALowerBoundACeiling is the whole reason the marker exists. The run of 02/08
// printed "CEILING: 12430 submit_sm/s at 80 binds" for a sweep that never saturated the peer — which
// went on absorbing 43 498/s at 320 binds with not one non-success outcome. Whoever read that line
// would have sized a system against a limit that was never found.
func TestReportRefusesToCallALowerBoundACeiling(t *testing.T) {
	out := joined(recordable()) // Saturated is false: no tier shed, the curve never bent

	if strings.Contains(out, "CEILING:") {
		t.Errorf("report claims a ceiling for a sweep that never saturated the peer:\n%s", out)
	}
	for _, want := range []string{"LOWER BOUND", "at least", "12430"} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q, it must say what the figure actually is:\n%s", want, out)
		}
	}
}

func TestReportCallsACeilingACeilingWhenTheSweepFoundOne(t *testing.T) {
	res := recordable()
	res.Saturated = true
	res.SaturationReason = "the peer shed traffic at 80 binds: throttled=17"

	out := joined(res)
	if !strings.Contains(out, "CEILING: 12430 submit_sm/s at 80 binds") {
		t.Errorf("report does not state the ceiling it found:\n%s", out)
	}
	if !strings.Contains(out, res.SaturationReason) {
		t.Errorf("report does not say what showed the limit:\n%s", out)
	}
	if strings.Contains(out, "LOWER BOUND") {
		t.Errorf("report hedges a ceiling it actually measured:\n%s", out)
	}
}

// TestReportMarksASmokeRun keeps the warning attached to the figures instead of 200 lines above them.
func TestReportMarksASmokeRun(t *testing.T) {
	res := recordable()
	res.Measure = 5 * time.Second

	out := joined(res)
	if !strings.Contains(out, "SMOKE RUN") {
		t.Errorf("a %v window is not a figure to record and the report does not say so:\n%s", res.Measure, out)
	}
	if full := joined(recordable()); strings.Contains(full, "SMOKE RUN") {
		t.Errorf("a full-length sweep must not be branded a smoke run:\n%s", full)
	}
}

// TestTierLineQualifiesTheServedLatency keeps a column that cannot detect saturation from reading like
// one that can: the simulator observes the latency its scenario DECIDED, not a duration it measured.
func TestTierLineQualifiesTheServedLatency(t *testing.T) {
	line := tierLine(ceiling.Tier{Binds: 20, Status: ceiling.TierCounted})
	if !strings.Contains(line, "configured") {
		t.Errorf("tier line reports a served latency with no reserve:\n%s", line)
	}
}
