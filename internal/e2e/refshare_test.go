package e2e_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// pipelineShare renders what fraction of a message's wall time the MT pipeline accounted for.
//
// It is the subtraction step-201d rests on. The router's consume loop is ONE goroutine, and across the
// measured window its backlog only grows — so it is never waiting for work, and the wall time it spends
// per message is exactly 1/output rate. That budget is spent on
//
//	1/rate = decode + Pipeline.Process + N x (encode + ProduceSync) + amortised offset commit
//
// pipeline_duration_seconds measures the middle term and nothing else (internal/router: the timer opens
// and closes around Pipeline.Process). So the share below reads directly:
//
//   - near 100% — the cost is inside the pipeline; which stage is a question for the per-stage histogram
//     and the pipeline micro-benchmark.
//   - far below — the cost is outside it, and the only serialised thing out there is the synchronous
//     acks=all produce, one record at a time, inside the consume loop.
//
// It refuses to divide rather than print a plausible zero: a window with no pipeline observation means the
// router did not run, which is a fact worth reading, not a 0.0% worth misreading.
func pipelineShare(sumSeconds float64, count uint64, submitted uint64, window time.Duration) string {
	if count == 0 {
		return "no observation — pipeline_duration_seconds was never fed in this window"
	}
	mean := sumSeconds / float64(count)
	out := fmt.Sprintf("%d messages through Pipeline.Process, mean %v",
		count, time.Duration(mean*float64(time.Second)).Round(time.Microsecond))
	if submitted == 0 || window <= 0 {
		return out + " (no output in the window: no budget to compare it against)"
	}
	budget := window.Seconds() / float64(submitted)
	return fmt.Sprintf("%s · budget %v/message at the measured output · pipeline is %.1f%% of it",
		out, time.Duration(budget*float64(time.Second)).Round(time.Microsecond), 100*mean/budget)
}

// cpuShare renders the CPU the Go process burned over the window, in cores.
//
// The run is a single process holding nine components, on a host that is also running Postgres, Redpanda
// and ClickHouse in containers. If this figure approaches runtime.NumCPU, then "the router is the
// bottleneck" is a statement about the laptop and not about the gateway, and no fan-out can be measured
// here at all — which step-201d requires be checked BEFORE any such conclusion is drawn, not after.
//
// It counts this process only, so it is a floor on the host's load and never a total.
func cpuShare(cpuSeconds float64, window time.Duration) string {
	if window <= 0 {
		return "no window to account CPU over"
	}
	cores := cpuSeconds / window.Seconds()
	n := runtime.NumCPU()
	return fmt.Sprintf("the Go process burned %.2f cores of %d over the window (%.0f%%); "+
		"Postgres, Kafka and ClickHouse run in containers beside it and are NOT counted",
		cores, n, 100*cores/float64(n))
}

// TestPipelineShareSplitsTheBudget checks the arithmetic the whole measurement rests on, at the two
// shapes that lead to opposite conclusions — and at the one input that must not produce a number at all.
func TestPipelineShareSplitsTheBudget(t *testing.T) {
	// 892 msg/s over 60s is the 03/08/2026 run. A 1.05ms mean would put the pipeline at ~94% of the
	// 1.12ms budget: the cost is inside it.
	got := pipelineShare(0.00105*53520, 53520, 53520, 60*time.Second)
	if !strings.Contains(got, "93.7%") {
		t.Errorf("a pipeline mean at 1.05ms of a 1.12ms budget should read ~93.7%%, got: %s", got)
	}

	// The same run with a 60µs mean puts ~95% of the budget OUTSIDE the pipeline, which is what names the
	// synchronous produce instead.
	got = pipelineShare(0.00006*53520, 53520, 53520, 60*time.Second)
	if !strings.Contains(got, "5.4%") {
		t.Errorf("a pipeline mean at 60µs of a 1.12ms budget should read ~5.4%%, got: %s", got)
	}

	// No observation must never render as 0%: "the router did nothing" and "the pipeline is free" are
	// opposite findings, and the second is the one a percentage invites.
	if got := pipelineShare(0, 0, 53520, 60*time.Second); strings.Contains(got, "%") {
		t.Errorf("an empty histogram must not be reported as a share: %s", got)
	}
}

// TestCPUShareNamesTheCeiling: the figure only earns its place if it says what it is a share OF.
func TestCPUShareNamesTheCeiling(t *testing.T) {
	got := cpuShare(60*float64(runtime.NumCPU()), 60*time.Second)
	if !strings.Contains(got, "100%") {
		t.Errorf("a process burning every core for the whole window should read 100%%, got: %s", got)
	}
	if !strings.Contains(got, "NOT counted") {
		t.Errorf("the figure must say the containers are excluded, or it reads as a host total: %s", got)
	}
}
