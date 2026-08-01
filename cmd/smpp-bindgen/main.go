// Command smpp-bindgen opens N concurrent SMPP sessions against a peer and reports what happened
// (step-200): the bind half of the load harness, next to the k6 script that covers the REST half.
//
// It is a development tool, not a deployable service: no config file, no ops port, no storage. It
// submits nothing — it answers "does this peer accept N simultaneous binds, and how many does it
// drop?". All the logic lives in the importable test/load/bindgen package so it can be tested from
// the outside (step-200 D4); this file only parses flags and prints the report.
//
// It exits non-zero as soon as one session failed to bind, so a run can be asserted on in a script.
// Interrupting it (SIGTERM / Ctrl-C) cuts the hold window short and unbinds cleanly.
//
// Usage:
//
//	smpp-bindgen -addr 127.0.0.1:2775 -binds 200
//	smpp-bindgen -binds 500 -hold 30s              hold the bound sessions open for 30s
//	smpp-bindgen -system-id esme1 -password s3cret
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
	var cfg bindgen.Config
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:2775", "SMPP peer address (host:port)")
	flag.IntVar(&cfg.Binds, "binds", 1, "number of sessions to open simultaneously")
	flag.StringVar(&cfg.SystemID, "system-id", "loadgen", "bind system_id, at most 15 characters")
	flag.StringVar(&cfg.Password, "password", "", "bind password, at most 8 characters")
	flag.StringVar(&cfg.SystemType, "system-type", "", "bind system_type (optional)")
	flag.DurationVar(&cfg.Hold, "hold", 0, "how long to hold the bound sessions open once every bind has settled")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", 0, "per-session TCP connect timeout (0 = package default)")
	flag.DurationVar(&cfg.RespTimeout, "resp-timeout", 0, "per-session bind_transceiver_resp timeout (0 = package default)")
	flag.Parse()

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	hold := cfg.Hold
	cfg.OnAllBound = func() {
		if hold > 0 {
			log.Printf("every bind settled, holding for %s (interrupt to stop early)", hold)
		}
	}

	log.Printf("binding %d sessions to %s as %q", cfg.Binds, cfg.Addr, cfg.SystemID)
	rep, err := bindgen.Run(ctx, cfg)
	if err != nil {
		return err
	}
	report(rep)

	if rep.Failed > 0 {
		return fmt.Errorf("%d of %d binds failed", rep.Failed, rep.Requested)
	}
	return nil
}

// report prints the outcome of a run: the counts first, since they are the answer, then a bounded
// sample of the causes.
func report(rep bindgen.Report) {
	log.Printf("requested %d | bound %d | failed %d | elapsed %s",
		rep.Requested, rep.Bound, rep.Failed, rep.Elapsed.Round(time.Millisecond))

	for i, err := range rep.Errors {
		if i == maxReportedErrors {
			log.Printf("  ... and %d more", len(rep.Errors)-maxReportedErrors)
			break
		}
		log.Printf("  failed: %v", err)
	}
}
