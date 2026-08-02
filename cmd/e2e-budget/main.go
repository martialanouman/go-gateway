// Command e2e-budget checks the end-to-end latency NFR against a running gateway (step-201, D4).
//
// The budget is spec §1.2: submission to SMSC delivery attempt, p99 < 2s. Both ends of that span live
// in one process — the accept time travels with the message and the delivery attempt happens in the
// connector pool — so there is nothing to correlate. The gateway measures the span itself into
// message_e2e_duration_seconds, and this command reads it off the pool's /metrics.
//
//	# take a baseline, run the load, then check what the run added
//	e2e-budget -metrics http://127.0.0.1:9100 -baseline /tmp/before.json
//	make load LOAD_PROFILE=sustained BASE_URL=…
//	e2e-budget -metrics http://127.0.0.1:9100 -baseline /tmp/before.json -check
//
// Without -baseline it scores every observation the process has taken since it started, which after a
// warmup or an earlier run is not the run under test.
//
// # It refuses to guess
//
// A Prometheus histogram gives cumulative buckets, not quantiles. This command reports the two bucket
// edges the quantile falls between and never interpolates: with exponential buckets the edges are a
// factor of two apart, and an interpolated figure would carry that error while reading like a
// measurement.
//
// So there are three outcomes, and only the first exits zero:
//
//   - the quantile's whole bucket is at or below the budget — held, proven;
//   - its whole bucket is at or above — over budget, proven;
//   - the budget falls strictly inside the bucket — the exposition does not resolve it.
//
// The third exits non-zero on purpose. "Not proven to fail" is not "held", and a load run that reports
// a budget it never resolved is worse than one that reports nothing.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/test/load/gatewaymetrics"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	metricsURL := flag.String("metrics", "http://127.0.0.1:9100",
		"gateway ops endpoint; a bare origin gets /metrics appended")
	baseline := flag.String("baseline", "",
		"file holding the reading to subtract; written when -check is absent, read when it is present")
	check := flag.Bool("check", false, "score the run against -baseline instead of recording one")
	quantile := flag.Float64("quantile", 0.99, "quantile to score, strictly between 0 and 1")
	budget := flag.Duration("budget", 2*time.Second, "the budget that quantile must stay under")
	connector := flag.String("connector", "", "restrict to one connector_id; empty means every connector")
	timeout := flag.Duration("timeout", 10*time.Second, "deadline for the scrape")
	flag.Parse()

	// The signal handler is installed inside run, not here: log.Fatalf exits the process, so a defer in
	// this scope would never run.
	if err := run(opts{
		metricsURL: *metricsURL,
		baseline:   *baseline,
		check:      *check,
		quantile:   *quantile,
		budget:     *budget,
		connector:  *connector,
		timeout:    *timeout,
	}); err != nil {
		log.Fatalf("e2e-budget: %v", err)
	}
}

type opts struct {
	metricsURL string
	baseline   string
	check      bool
	quantile   float64
	budget     time.Duration
	connector  string
	timeout    time.Duration
}

func run(o opts) error {
	if o.check && o.baseline == "" {
		return errors.New("-check needs the -baseline it should subtract")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := gatewaymetrics.NewClient(o.metricsURL)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	snap, err := client.Scrape(ctx)
	if err != nil {
		return err
	}
	got := selectHistogram(snap, o.connector)

	if !o.check {
		if o.baseline == "" {
			return errors.New("-baseline names the file to record the reading into")
		}
		if err := writeBaseline(o.baseline, got); err != nil {
			return err
		}
		log.Printf("baseline recorded from %s: %d observations", client.URL(), got.Count)
		return nil
	}

	before, err := readBaseline(o.baseline)
	if err != nil {
		return err
	}
	// Subtract rather than score the whole exposition: the counters are cumulative since the process
	// started, so a warmup or an earlier run would otherwise be folded into the figure under test.
	windowed, err := gatewaymetrics.Sub(before, got)
	if err != nil {
		return err
	}
	if err := checkBudget(windowed, o.quantile, o.budget); err != nil {
		return err
	}
	log.Printf("budget held: %d observations over the run", windowed.Count)
	return nil
}

// selectHistogram narrows the reading to one connector, or totals every one of them.
func selectHistogram(s gatewaymetrics.Snapshot, connectorID string) gatewaymetrics.Histogram {
	if connectorID == "" {
		return s.Total()
	}
	return s.Where(func(k gatewaymetrics.Key) bool { return k.ConnectorID == connectorID })
}

// checkBudget scores the histogram and turns the verdict into an error, or nil when the budget is
// proven held.
func checkBudget(h gatewaymetrics.Histogram, quantile float64, budget time.Duration) error {
	verdict, q, err := h.CheckBudget(quantile, budget)
	if err != nil {
		return err
	}
	log.Printf("p%g of %d observations lies in %s, budget %s → %s",
		100*quantile, h.Count, q, budget, verdict)
	return verdictError(verdict, q, budget)
}

// verdictError maps a verdict to the command's exit rule. Pass is the ONLY success: an unresolved
// budget is reported as a failure, because a run that cannot decide its budget has not held it. An
// unrecognised verdict is a failure too — Verdict is a string type, and a value added later must not
// fall through into success.
func verdictError(v gatewaymetrics.Verdict, q gatewaymetrics.Quantile, budget time.Duration) error {
	switch v {
	case gatewaymetrics.Pass:
		return nil
	case gatewaymetrics.Fail:
		return fmt.Errorf("over budget: the quantile lies in %s, entirely at or above %s", q, budget)
	case gatewaymetrics.Indeterminate:
		return fmt.Errorf(
			"the exposition did not resolve the budget: the quantile lies in %s, which straddles %s — "+
				"a bucket edge at the budget is what makes it decidable", q, budget)
	default:
		return fmt.Errorf("unknown verdict %q: refusing to read it as a held budget", v)
	}
}

func writeBaseline(path string, h gatewaymetrics.Histogram) error {
	b, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("encode baseline: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return fmt.Errorf("write baseline: %w", err)
	}
	return nil
}

func readBaseline(path string) (gatewaymetrics.Histogram, error) {
	//nolint:gosec // G304: the path is this development tool's own -baseline flag, chosen by whoever
	// runs it. There is no untrusted input here and no privilege to escalate to.
	b, err := os.ReadFile(path)
	if err != nil {
		return gatewaymetrics.Histogram{}, fmt.Errorf("read baseline: %w", err)
	}
	var h gatewaymetrics.Histogram
	if err := json.Unmarshal(b, &h); err != nil {
		return gatewaymetrics.Histogram{}, fmt.Errorf("decode baseline: %w", err)
	}
	return h, nil
}
