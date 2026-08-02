// Command smpp-bindgen opens N concurrent SMPP sessions against a peer and reports what happened
// (step-200): the bind half of the load harness, next to the k6 script that covers the REST half.
//
// It is a development tool, not a deployable service: no config file, no ops port, no storage. By
// default it submits nothing — it answers "does this peer accept N simultaneous binds, and how many
// does it drop?". With -submit it also becomes a submit_sm injector (step-201) and pushes traffic on
// every bound session for the whole hold window, which is how the peer's throughput ceiling is
// reached. All the logic lives in the importable test/load/bindgen package so it can be tested from
// the outside (step-200 D4); this file only parses flags and prints the report.
//
// The throughput figure a -submit run exists to produce is NOT the one printed here: it is read from
// the peer's own /metrics, which counts what it really processed. The submitted/accepted counts below
// are a diagnostic — they answer "did the injector actually push?".
//
// It exits non-zero as soon as one session failed to bind, OR was dropped by the peer during the hold
// window — failing to keep what it accepted is the peer's failure too, and it is half the question
// this tool asks — OR the injector was asked to submit and put nothing on the wire, OR a submission
// failed on the wire. Any of them can be asserted on in a script.
//
// What it does NOT fail on is a writer the closing window caught mid-write: an injector pushing until
// the last instant of its window is writing when that instant arrives, so every full-length run ends
// with a few, and they are reported as "cut short" rather than as errors.
//
// Interrupting it (SIGTERM / Ctrl-C) cuts the hold window short and unbinds cleanly. Interrupting it
// during the dial phase is reported as failed binds, since that is what a cancelled dial produces.
//
// Usage:
//
//	smpp-bindgen -addr 127.0.0.1:2775 -binds 200
//	smpp-bindgen -binds 500 -hold 30s              hold the bound sessions open for 30s
//	smpp-bindgen -system-id esme1 -password s3cret
//	smpp-bindgen -binds 20 -hold 60s -submit       inject submit_sm on all 20 binds for 60s
//	smpp-bindgen -binds 20 -hold 60s -submit -submit-window 64
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
)

// maxReportedErrors caps the per-session causes printed after a run. A 5 000-bind run against a dead
// peer produces 5 000 near-identical errors; the count is the signal, the first few are the diagnosis.
const maxReportedErrors = 10

func main() {
	if err := run(); err != nil {
		log.Fatalf("smpp-bindgen: %v", err)
	}
}

func run() error {
	var (
		cfg        bindgen.Config
		sub        bindgen.SubmitConfig
		submit     bool
		submitText string
	)
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:2775", "SMPP peer address (host:port)")
	flag.IntVar(&cfg.Binds, "binds", 1, "number of sessions to open simultaneously")
	flag.StringVar(&cfg.SystemID, "system-id", "loadgen", "bind system_id, at most 15 characters")
	flag.StringVar(&cfg.Password, "password", "", "bind password, at most 8 characters")
	flag.StringVar(&cfg.SystemType, "system-type", "", "bind system_type (optional)")
	flag.DurationVar(&cfg.Hold, "hold", 0, "how long to hold the bound sessions open once every bind has settled")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", 0, "per-session TCP connect timeout (0 = package default)")
	flag.DurationVar(&cfg.RespTimeout, "resp-timeout", 0, "per-session bind_transceiver_resp timeout (0 = package default)")
	flag.BoolVar(&submit, "submit", false, "inject submit_sm on every bound session for the whole hold window (needs -hold)")
	flag.IntVar(&sub.Window, "submit-window", 0, "submit_sm in flight at once per session (0 = package default)")
	flag.IntVar(&sub.Count, "submit-count", 0, "submit_sm to send per session (0 = until the hold window closes)")
	flag.StringVar(&sub.SourceAddr, "submit-src", "", "submit_sm source_addr, at most 20 characters")
	flag.StringVar(&sub.DestAddr, "submit-dst", "", "submit_sm destination_addr, at most 20 characters (empty = package default)")
	flag.StringVar(&submitText, "submit-text", "", "submit_sm body, at most 254 octets (empty = package default filler)")
	flag.Parse()

	if submit {
		sub.ShortMessage = []byte(submitText)
		cfg.Submit = &sub
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	hold := cfg.Hold
	cfg.OnAllBound = func() {
		switch {
		case submit:
			log.Printf("every bind settled, injecting submit_sm for %s (interrupt to stop early)", hold)
		case hold > 0:
			log.Printf("every bind settled, holding for %s (interrupt to stop early)", hold)
		}
	}

	log.Printf("binding %d sessions to %s as %q", cfg.Binds, cfg.Addr, cfg.SystemID)
	rep, err := bindgen.Run(ctx, cfg)
	if err != nil {
		return err
	}
	report(rep)
	return verdict(rep, submit)
}

// verdict turns a finished run into the process's exit status: nil when the run answered the question
// it was asked, an error when it did not. It is kept apart from run so the rule can be asserted on
// without a peer and without flag parsing — the exit code is what a script reads, not the log lines.
func verdict(rep bindgen.Report, submit bool) error {
	switch {
	case rep.Failed > 0:
		return fmt.Errorf("%d of %d binds failed", rep.Failed, rep.Requested)
	// A dropped session is a failure of the peer to HOLD what it accepted — the very question this
	// tool exists to answer. Reporting it as a clean run would say "the peer takes N binds" about a
	// peer that took them and let them go.
	case rep.Dropped > 0:
		return fmt.Errorf("%d of %d bound sessions were dropped by the peer during the hold",
			rep.Dropped, rep.Bound)
	// An injector that pushed nothing produces a peer-side reading of zero, which reads exactly like a
	// peer with no throughput at all. Fail loudly rather than let that be measured.
	case submit && rep.Submitted == 0:
		return fmt.Errorf("the injector put no submit_sm on the wire (%d errors, first: %v)",
			rep.SubmitErrors, rep.SubmitErr)
	// A submission the peer never got is a hole in the traffic the run was supposed to produce, and
	// the peer-side figure is then a rate over a load nobody can state. Report.SubmitCutShort is
	// deliberately NOT here: a writer torn out of a blocked write by the end of its own window is how
	// every full-length saturating run ends, and failing on it would fail on the tool's normal mode.
	case rep.SubmitErrors > 0:
		return fmt.Errorf("%d submissions failed on the wire (first: %v)", rep.SubmitErrors, rep.SubmitErr)
	}
	return nil
}

// report prints the outcome of a run: the counts first, since they are the answer, then a bounded
// sample of the causes.
func report(rep bindgen.Report) {
	log.Printf("requested %d | bound %d | failed %d | dropped %d | elapsed %s",
		rep.Requested, rep.Bound, rep.Failed, rep.Dropped, rep.Elapsed.Round(time.Millisecond))

	if rep.Submitting > 0 {
		log.Printf("submitted %d | accepted %d | rejected %d | unanswered %d | cut short %d | errors %d | "+
			"over %s (~%.0f/s written)",
			rep.Submitted, rep.Accepted, rep.Rejected, rep.Unanswered, rep.SubmitCutShort, rep.SubmitErrors,
			rep.Submitting.Round(time.Millisecond), float64(rep.Submitted)/rep.Submitting.Seconds())
		if rep.SubmitErr != nil {
			log.Printf("  first submit error: %v", rep.SubmitErr)
		}
	}

	for i, err := range rep.Errors {
		if i == maxReportedErrors {
			log.Printf("  ... and %d more", len(rep.Errors)-maxReportedErrors)
			break
		}
		log.Printf("  failed: %v", err)
	}
}
