// Command router-svc runs the MT pipeline.
//
// At M0 it processes no message: it is the canonical service skeleton every other cmd/ main is
// modelled on (guide de codage §5). What it establishes is the lifecycle — load and validate
// configuration, install telemetry, serve the ops port, run until SIGTERM, then drain within a
// bounded window.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// serviceName identifies this binary in logs, traces and metrics. It is a constant, not a
// setting: telemetry attributed to a service that can be renamed at runtime is worthless.
const serviceName = "router-svc"

func main() {
	// main holds no defer: log.Fatal exits the process without running them, so the lifecycle —
	// including signal handling — lives in run, where deferred cleanup actually happens.
	if err := run(); err != nil {
		// The only tolerated Fatal is startup (guide de codage §5/§10): the logger may not exist
		// yet, and a service that cannot boot correctly must not boot at all.
		log.Fatalf("%s: %v", serviceName, err)
	}
}

// run holds the whole lifecycle so that every path returns an error instead of exiting, which is
// what makes the sequence testable and the drain reachable.
func run() error {
	// SIGTERM is how Kubernetes asks a pod to stop; Interrupt is how a developer does. Both
	// cancel ctx, which every goroutine below watches.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	shutdownTracing, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	// nolint:contextcheck // Detaching is the point: see drainTracing's comment.
	defer drainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	// Vital dependencies for THIS service (plan §1.5). Kafka only: with Redis down the router
	// fails closed on throttling and messages stay durable in Kafka, so it is still doing its
	// job; with Kafka gone it can neither read work nor durably hand it on, so it must leave the
	// load balancer. Adding a non-vital dependency here would drain healthy pods over a
	// degradation they could absorb.
	//
	// The probe is a TCP dial: M0 has no Kafka client yet, and reachability is the outage that
	// matters. Swap it for a client-level ping when franz-go lands at M2.
	kafkaCheck := observability.AnyTCPDialCheck("kafka", cfg.Kafka.Brokers, cfg.Kafka.Timeout)

	ops, err := observability.NewOpsServer(cfg, logger, kafkaCheck)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg)

	// The ops server is the only long-running component at M0. Later milestones add the Kafka
	// consumer and the pipeline here, each supervised the same way: started under ctx, awaited
	// in the same group, so one failing component brings the service down predictably rather
	// than leaving a half-dead pod behind.
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// Components run under a context run can cancel itself, so a component failing stops the
	// others exactly as a signal does. Cancelling through signal.NotifyContext's stop would work
	// too, but it also unregisters the handler, and a second SIGTERM would then fall back to the
	// default kill.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ops.Run(runCtx, cfg.ShutdownTimeout); err != nil {
			// Buffered and non-blocking: the first failure is the one that matters, and a
			// component must never block on reporting.
			select {
			case errCh <- fmt.Errorf("ops server: %w", err):
			default:
			}
		}
	}()

	// Stop on whichever comes first. Waiting on ctx.Done alone would park a component's startup
	// failure in errCh and keep the pod alive — serving nothing, probed by no one — until an
	// operator noticed and sent SIGTERM.
	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}
	cancel()

	// Every goroutine above exits on runCtx, so this returns within ShutdownTimeout.
	wg.Wait()

	if runErr != nil {
		return runErr
	}

	// A component may still have failed on the way out.
	select {
	case err := <-errCh:
		return err
	default:
	}

	logger.Info("stopped")
	return nil
}

// drainTracing flushes buffered spans on the way out. It runs on a context detached from the
// cancelled service context — one that is already done would abort the flush immediately and
// throw away the spans of the very shutdown an operator is trying to understand.
func drainTracing(shutdown observability.ShutdownFunc, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("flush traces on shutdown", "err", err)
	}
}
