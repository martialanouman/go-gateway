// Command smsc-ceiling measures the submit_sm throughput ceiling of an SMPP test peer (step-201, D3):
// it sweeps the number of concurrent binds, injects submit_sm on all of them for each tier, and reads
// what the peer absorbed from its own Prometheus endpoint.
//
// It is a development tool, not a deployable service. All the logic lives in the importable
// test/load/ceiling package so it can be tested without a peer (step-200 D4); this file parses flags
// and prints the curve.
//
// # It does not start the peer
//
// The SMPP address and the metrics URL are inputs. That keeps testcontainers out of a measurement
// binary, and it is what lets the same command point at a remote simulator for the full-scale campaign
// (step-280). Start the peer yourself — the config below is the HealthyConfig of
// internal/testutil/smscsim, the profile the reference run will use:
//
// The bind password below is a throwaway for a local simulator, and /tmp/smsc-ceiling.yml is
// world-readable. Against anything that is not a disposable container on your own machine, put the
// file somewhere only you can read and take the credential from your own secret store — this template
// gets copied, and the copy is where a real password ends up.
//
//	cat > /tmp/smsc-ceiling.yml <<'YAML'
//	observability:
//	  http_port: 9000
//	virtual_smscs:
//	  - name: carrier
//	    port: 2775
//	    bind_credentials: { system_id: "loadgen", password: "pw" }   # fixture only — never a real one
//	    addr_ton: 1
//	    addr_npi: 1
//	    address_range: ".*"
//	    tls: { enabled: false }
//	    seed: 42
//	    pdu_buffer_size: 10000
//	    scenario:
//	      profile: healthy
//	      latency: { distribution: fixed, params: { ms: 5 } }
//	YAML
//	docker run -d --name smsc-ceiling -p 2775:2775 -p 9000:9000 \
//	  -v /tmp/smsc-ceiling.yml:/etc/smsc/config.yml smsc-simulator:dev
//
// A ceiling measured under a different latency profile bounds nothing: it must be the profile the run
// it is meant to bound will use.
//
// # What it prints
//
// One line per tier — the curve — then the two figures D3 asks to be recorded: the highest rate the
// peer sustained anywhere in the sweep, and the rate at the bind count the reference run will use. The
// second is the one a reference run has to stay under.
//
// The first is printed as a CEILING only if some tier actually saturated the peer. Otherwise it comes
// out as a LOWER BOUND, because that is what it is: the sweep never found the limit, it ran out of
// tiers. Extend -binds until a tier sheds or the curve bends. A window under the 60s floor is branded
// a SMOKE RUN on the same lines, so a figure cannot be lifted out of a run that was only checking the
// instrument works.
//
// It exits non-zero when any tier failed to produce a trustworthy measurement, when no tier counted, or
// when the reference tier is not among the counted ones. A tier the peer SHED on is not a failure: it
// is what a ceiling looks like, and it is reported as disqualified. A sweep that never saturated the
// peer is not a failure either — the reference figure it produces is still valid and still
// conservative — which is why the wording of the line, not the exit code, is what carries the caveat.
//
// Usage:
//
//	smsc-ceiling -addr 127.0.0.1:2775 -metrics http://127.0.0.1:9000
//	smsc-ceiling -binds 10,20,40,80 -reference 20 -measure 60s
//	smsc-ceiling -measure 5s -warmup 1s -settle 1s        a smoke run — NOT a figure to record
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
	"github.com/martialanouman/go-gateway/test/load/ceiling"
	"github.com/martialanouman/go-gateway/test/load/smscmetrics"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("smsc-ceiling: %v", err)
	}
}

func run() error {
	var (
		base    bindgen.Config
		sub     bindgen.SubmitConfig
		cfg     ceiling.Config
		metrics string
		binds   string
	)
	flag.StringVar(&base.Addr, "addr", "127.0.0.1:2775", "SMPP address of the peer (host:port)")
	flag.StringVar(&metrics, "metrics", "http://127.0.0.1:9000", "peer's metrics origin or full /metrics URL")
	flag.StringVar(&base.SystemID, "system-id", "loadgen", "bind system_id, at most 15 characters")
	flag.StringVar(&base.Password, "password", "pw", "bind password, at most 8 characters")
	flag.StringVar(&base.SystemType, "system-type", "", "bind system_type (optional)")
	flag.StringVar(&binds, "binds", "", "comma-separated sweep of bind counts (empty = 10,20,40,80)")
	flag.IntVar(&cfg.Reference, "reference", 0,
		"bind count of the future reference run; the second figure is read there (0 = largest tier of at most 32)")
	flag.DurationVar(&cfg.Measure, "measure", 0, "measurement window per tier (0 = 60s, the floor D3 sets)")
	flag.DurationVar(&cfg.Warmup, "warmup", 0, "head of each injection window left unmeasured (0 = 10s)")
	flag.DurationVar(&cfg.Settle, "settle", 0, "margin between the second reading and the end of the injection (0 = 5s)")
	flag.DurationVar(&cfg.Cooldown, "cooldown", 0, "pause between two tiers (0 = 5s)")
	flag.StringVar(&cfg.VirtualSMSC, "virtual-smsc", "",
		"narrow the readings to one virtual SMSC (empty = every one the peer exposes)")
	flag.IntVar(&sub.Window, "submit-window", 0, "submit_sm in flight at once per session (0 = package default)")
	flag.StringVar(&sub.DestAddr, "submit-dst", "", "submit_sm destination_addr (empty = package default)")
	flag.Parse()

	var err error
	if cfg.Binds, err = parseBinds(binds); err != nil {
		return err
	}
	base.Submit = &sub

	scraper, err := smscmetrics.NewClient(metrics)
	if err != nil {
		return err
	}
	cfg.OnTier = func(t ceiling.Tier) { log.Print(tierLine(t)) }

	sweeper, err := ceiling.New(ceiling.BindgenLoad{Base: base}, scraper, cfg)
	if err != nil {
		return err
	}
	live := sweeper.Config()

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	log.Printf("sweeping %v binds against %s, reading %s", live.Binds, base.Addr, scraper.URL())
	log.Printf("per tier: %s warmup + %s measured + %s settle (%s each, %s cooldown between tiers)",
		live.Warmup, live.Measure, live.Settle, live.Hold(), live.Cooldown)
	if live.Measure < ceiling.MinRecordableMeasure {
		log.Printf("WARNING: the measurement window is %s — under the %s floor, this is a smoke run, not a figure to record",
			live.Measure, ceiling.MinRecordableMeasure)
	}

	res, sweepErr := sweeper.Run(ctx)
	report(res)
	return sweepErr
}

// parseBinds reads the sweep from the flag. An empty value leaves the package default in place.
func parseBinds(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []int
	for _, field := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			return nil, fmt.Errorf("-binds %q: %q is not a bind count", s, field)
		}
		out = append(out, n)
	}
	return out, nil
}

// report prints the curve and the two figures. It runs even when the sweep failed: the tiers that did
// measure are still the useful half of a broken run.
func report(res ceiling.Result) {
	for _, line := range reportLines(res) {
		log.Print(line)
	}
}

// reportLines builds the closing report, one line at a time so what it claims can be tested.
//
// What it must never do is state a ceiling the sweep did not find. A sweep whose every tier scaled
// with the binds it was given measured the largest load it was ASKED to produce, not the largest the
// peer can take, and the two are not the same figure — one of them is a constraint, the other is a
// lower bound that happens to be where somebody stopped typing.
func reportLines(res ceiling.Result) []string {
	lines := []string{
		fmt.Sprintf("--- curve (%d tiers, %s measured each) ---", len(res.Tiers), res.Measure),
	}
	for _, t := range res.Tiers {
		lines = append(lines, tierLine(t))
	}

	if !res.Recordable() {
		lines = append(lines, fmt.Sprintf(
			"SMOKE RUN: %s measured per tier, under the %s floor — these figures show the instrument runs, they are not figures to record",
			res.Measure, ceiling.MinRecordableMeasure))
	}

	if res.CeilingBinds == 0 {
		return append(lines, "no tier counted: this sweep produced no ceiling")
	}

	atLeast := ""
	if res.Saturated {
		lines = append(lines, fmt.Sprintf("CEILING: %.0f submit_sm/s at %d binds — %s",
			res.Ceiling, res.CeilingBinds, res.SaturationReason))
	} else {
		atLeast = "at least "
		lines = append(lines, fmt.Sprintf(
			"LOWER BOUND: the peer absorbed at least %.0f submit_sm/s at %d binds and was NOT saturated — "+
				"no tier shed and the curve never bent, so its real ceiling is above every tier run here. "+
				"Extend -binds until it does.",
			res.Ceiling, res.CeilingBinds))
	}

	if res.ReferenceCeiling == 0 {
		return append(lines, fmt.Sprintf("REFERENCE (%d binds): not measured — that tier did not count",
			res.ReferenceBinds))
	}
	return append(lines, fmt.Sprintf("REFERENCE (%d binds): %s%.0f submit_sm/s — the reference run must stay under this",
		res.ReferenceBinds, atLeast, res.ReferenceCeiling))
}

// tierLine renders one tier: the peer's figures first, since they are the measurement, then the
// injector's own counters, which only say whether it pushed.
//
// The latency is labelled configured because that is what it is: the simulator observes the latency
// its scenario DECIDED, not a duration it measured, so the column reads the same 5 ms whether the peer
// is idle or drowning. It is here to confirm which profile ran, never to show saturation.
func tierLine(t ceiling.Tier) string {
	tp := t.Throughput
	line := fmt.Sprintf(
		"%3d binds | %8.0f submit_sm/s | %7.0f accepted | %7.0f served | latency %6s (configured) | peer binds %3.0f | %s",
		t.Binds, tp.SubmitPerSecond, tp.Submitted, tp.Served, tp.MeanServedLatency.Round(time.Microsecond),
		tp.ActiveBinds, t.Status)
	if t.Reason != "" {
		line += ": " + t.Reason
	}
	rep := t.Report
	// per-session min/max is printed because a tier is refused on it: without it in the output, the
	// next run cannot check the threshold that judged it, and a badly scaled bar stays invisible until
	// it refuses everything. That is how an earlier version of this guard shipped.
	line += fmt.Sprintf(" [injector: bound %d/%d, submitted %d (per session %d..%d), accepted %d, rejected %d, unanswered %d, errors %d]",
		rep.Bound, rep.Requested, rep.Submitted, rep.SubmittedMin, rep.SubmittedMax,
		rep.Accepted, rep.Rejected, rep.Unanswered, rep.SubmitErrors)
	return line
}
