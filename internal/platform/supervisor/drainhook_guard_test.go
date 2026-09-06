package supervisor_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// minSupervisedCommands keeps this guard from passing on nothing. A renamed directory or a wrong
// relative path would otherwise leave the scan empty and satisfy every assertion — a green test that
// guards air. The repo has ten supervised services today, so this floor fires on a broken scan rather
// than on ordinary churn.
const minSupervisedCommands = 10

var (
	// Both idioms, because the guard's whole job is to catch the service someone adds LATER, and that
	// someone may not copy today's uniform style. The composite-literal alternative needs its brace:
	// matching a bare `supervisor.Ordered` would also match the prose in two mains that explain the
	// reverse drain order, and demand OnDrain of a file that merely mentions the type.
	usesSupervisor = regexp.MustCompile(`var \w+ supervisor\.(Group|Ordered)|supervisor\.(Group|Ordered)\{`)
	registersHook  = regexp.MustCompile(`\.OnDrain\(`)
)

// TestEverySupervisedCommandRegistersTheDrainHook is the guard for the layer where this defect
// actually lived. OpsServer.DrainHook can be perfect and the drain still broken for a service whose
// main never calls it — and nothing else would notice, because a missing hook has no symptom until a
// rolling deploy cuts binds in production.
//
// It is deliberately textual rather than behavioural: proving the wiring at runtime would mean booting
// each of the ten services against real containers to observe one line. Two things it therefore cannot
// see — a hook registered on the wrong supervisor, and a delay hardcoded to zero instead of read from
// cfg.DrainDelay. The runtime tests in this package and in internal/observability cover what the hook
// DOES, and TestLoadDefaults pins the delay operators actually get; this one only covers that every
// service asks for it. Tightening the pattern to match the argument too was considered and dropped: a
// textual guard that grows conditions grows exemptions, and a guard with exemptions stops being read.
func TestEverySupervisedCommandRegistersTheDrainHook(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(filepath.Join("..", "..", "..", "cmd"))
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}

	var supervised, missing []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join("..", "..", "..", "cmd", e.Name(), "main.go")
		src, err := os.ReadFile(path)
		if err != nil {
			continue // a command without a main.go is not a service
		}
		if !usesSupervisor.Match(src) {
			continue // a one-shot tool (migrate, kafka-provision…) has nothing to drain
		}
		supervised = append(supervised, e.Name())
		if !registersHook.Match(src) {
			missing = append(missing, e.Name())
		}
	}

	if len(supervised) < minSupervisedCommands {
		t.Fatalf("found %d supervised commands under cmd/, want at least %d — the scan is not reading "+
			"the tree", len(supervised), minSupervisedCommands)
	}
	if len(missing) > 0 {
		t.Errorf("these services run a supervisor but never call OnDrain: %s\n"+
			"Each must register the pre-drain hook (g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))), or "+
			"the pod keeps answering 200 on /readyz while it tears down and the load balancer keeps "+
			"handing it new work — for smpp-server-svc, binds it accepts and then cuts (plan §16).",
			strings.Join(missing, ", "))
	}
}

// passesDrainBudget matches a supervisor Run handed cfg.DrainBudget. It requires TWO arguments before
// it, which is what separates a supervisor's Run(ctx, logger, budget) from the ops server's
// Run(ctx, timeout) — both appear in every one of these mains, and matching the shorter one would let
// a main hand the budget to the wrong Run and still pass. It deliberately does not pin the names of
// ctx and logger: renaming those is not the mistake this guards against.
var passesDrainBudget = regexp.MustCompile(`\.Run\([^),]+,[^),]+,\s*cfg\.DrainBudget\)`)

// TestEverySupervisedCommandPassesTheDrainBudget is a SEPARATE guard from the one above, not another
// condition inside it — that distinction is the point. The OnDrain guard deliberately declined to
// match its own argument, on the grounds that a textual guard growing conditions grows exemptions;
// and it could afford to, because the value a hook receives is pinned elsewhere (TestLoadDefaults
// covers the delay operators actually get).
//
// This argument is pinned NOWHERE. Passing cfg.ShutdownTimeout here type-checks, runs, and is exactly
// the defect that cost step-270 a full review round: the per-component timeout used as the budget of
// the whole sequence, so a component spending the window it was granted trips the ceiling and
// everything registered behind it is abandoned rather than drained — the in-flight submit_sm loss
// step-260 exists to prevent, re-introduced in silence.
//
// It re-walks cmd/ instead of sharing the scan above, for the reason internal/deploy's guard states:
// two guards sharing a helper fail together, and this one must keep working the day someone breaks
// that one.
func TestEverySupervisedCommandPassesTheDrainBudget(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(filepath.Join("..", "..", "..", "cmd"))
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}

	var supervised, wrong []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		src, err := os.ReadFile(filepath.Join("..", "..", "..", "cmd", e.Name(), "main.go"))
		if err != nil || !usesSupervisor.Match(src) {
			continue
		}
		supervised = append(supervised, e.Name())
		if !passesDrainBudget.Match(src) {
			wrong = append(wrong, e.Name())
		}
	}

	if len(supervised) < minSupervisedCommands {
		t.Fatalf("found %d supervised commands under cmd/, want at least %d — the scan is not reading "+
			"the tree", len(supervised), minSupervisedCommands)
	}
	if len(wrong) > 0 {
		t.Errorf("these services run a supervisor but do not hand Run cfg.DrainBudget: %s\n"+
			"The budget bounds the WHOLE drain; cfg.ShutdownTimeout bounds ONE component and is not "+
			"interchangeable with it (config.Validate refuses a budget at or under it). A supervisor "+
			"given the per-component timeout abandons everything behind the first component that "+
			"spends its window.",
			strings.Join(wrong, ", "))
	}
}
