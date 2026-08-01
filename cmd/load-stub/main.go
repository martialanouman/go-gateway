// Command load-stub runs the load harness's HTTP stub of the public API (step-200): it answers
// POST /v1/messages like api/openapi-public.yaml says the gateway does, and nothing more.
//
// It is a development tool, not a deployable service: no config file, no ops port, no storage. It
// exists so the k6 load script can be exercised — and, above all, made to FAIL — without a running
// gateway. Run it once fast (the script must pass) and once with -delay above the script's latency
// budget (the script must exit non-zero); a harness that cannot fail proves nothing.
//
// Usage:
//
//	load-stub                     serve on :8099 with no artificial latency
//	load-stub -delay 300ms        serve slowed past a typical latency budget (negative run)
//	load-stub -addr 127.0.0.1:9000 -delay 1s
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

	"github.com/martialanouman/go-gateway/test/load/stub"
)

const shutdownTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("load-stub: %v", err)
	}
}

func run() error {
	addr := flag.String("addr", stub.DefaultAddr, "HTTP listen address")
	delay := flag.Duration("delay", 0, "artificial delay added before every response (e.g. 300ms)")
	flag.Parse()

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	srv, err := stub.Listen(ctx, stub.Config{Addr: *addr, Delay: *delay})
	if err != nil {
		return err
	}
	log.Printf("load-stub listening on %s (POST /v1/messages, delay %s)", srv.Addr(), *delay)

	<-ctx.Done()
	stopSignals()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	log.Print("load-stub stopped")

	return nil
}
