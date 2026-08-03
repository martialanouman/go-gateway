// Command router-svc runs the MT pipeline: it consumes mt.inbound, normalises and routes each
// message, and publishes mt.routed (M2). A message rejected by the pipeline gets a rejected CDR row
// so get-message reflects it. The lifecycle — load and validate config, install telemetry, build the
// service graph (wiring.go), serve the ops port, run the consumer, drain on SIGTERM — follows the
// canonical skeleton (guide de codage §5).
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

// serviceName identifies this binary in logs, traces and metrics, and is the consumer group id.
const serviceName = "router-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres for the startup route snapshot, Kafka for the data plane, ClickHouse for rejected
	// CDR rows. No HTTP: the router has no client-facing listener.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionKafka, config.SectionClickHouse, config.SectionRedis, config.SectionBilling, config.SectionContentKey)
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
	//nolint:contextcheck // Detaching is the point: see DrainTracing's comment.
	defer observability.DrainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	app, err := newRouterApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router pipeline tear down together — neither must outlive the other — so the
	// unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("router", app.router.Run)
	// The accepted-CDR projector is SELF-RESTARTING rather than a plain g.Add: it writes ClickHouse on every
	// message, so a transient ClickHouse fault surfaces as a RunBatch error (uncommitted offset → reprocess).
	// If that error propagated to the unordered supervisor it would tear down the whole process — routing
	// included — over a store the routing happy path never touches (see the readiness rationale in wiring.go).
	// Instead we absorb it: on a fault we back off and resume the consumer (reprocessing from the last commit,
	// so no accepted row is lost), and only ctx cancellation stops it. ClickHouse thus stays non-vital for
	// routing.
	g.Add("accepted cdr", func(c context.Context) error {
		return runResilient(c, "accepted-cdr projector", app.accepted.Run, logger)
	})
	// The outcome-CDR projector (step-201c) is self-restarting for the same reason, and one more: it is now
	// the ONLY writer of the enroute/failed row, so tearing it down on a ClickHouse blip would stop the
	// lifecycle advancing for the whole fleet. Reprocessing from the last commit is safe — the row is
	// idempotent — where crashing the router is not.
	g.Add("outcome cdr", func(c context.Context) error {
		return runResilient(c, "outcome-cdr projector", app.outcome.projector.Run, logger)
	})
	g.Add("snapshot watcher", app.watcher.Run)
	g.Add("metric stream", func(c context.Context) error {
		app.emitter.Run(c, metricStreamInterval)
		return nil
	})
	// Queue depth is polled, not counted: it is the broker's view of how far each group is behind. It runs on
	// its own slower tick because it costs a broker round-trip, and router-svc is the ONE owner of both these
	// topics' depth — two services reporting the same topic would double-count (step-180).
	//
	// mt.outcome joined mt.inbound here in step-201c: the outcome projection is what now decides how long a
	// message reads "accepted" before it reads "enroute", so its backlog is the status lag itself (D13).
	g.Add("queue depth", func(c context.Context) error {
		pollQueueDepth(c, []lagReader{app.consumer, app.outcome.kafka}, app.emitter, app.catalog, logger)
		return nil
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// projectorBackoff is the pause between restarts of a CDR projector after a transient fault (a ClickHouse
// blip), long enough to let the store recover without a tight crash loop, short enough to keep the
// get-message 404 window — and the outcome projector's status lag — from lengthening unduly.
const projectorBackoff = 2 * time.Second

// runResilient runs a supervised loop that survives transient faults: it restarts run after a bounded backoff
// on any non-nil error, and returns only when ctx is cancelled (a clean stop). It lets one component tolerate
// a dependency blip without failing the whole process — used for both CDR projectors so a ClickHouse fault
// reprocesses (at-least-once) instead of crashing router-svc's routing path.
func runResilient(ctx context.Context, name string, run func(context.Context) error, logger *slog.Logger) error {
	for {
		err := run(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		logger.ErrorContext(ctx, "component faulted; restarting after backoff", "component", name, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(projectorBackoff):
		}
	}
}

// metricStreamInterval is how often a snapshot reaches metrics.stream. One second keeps the dashboard well
// inside the < 5 s freshness criterion (§15) while bounding the topic to one record per service per second,
// whatever the traffic.
const metricStreamInterval = time.Second

// queueDepthInterval is slower than the snapshot tick because each poll is a broker round-trip. At 5 s the
// depth is still fresh enough to read a backlog forming, and the cost stays negligible.
const queueDepthInterval = 5 * time.Second

// lagReader is a consumer group whose backlog can be read from the broker. *kafka.Consumer satisfies it;
// declared here, consumer-side, so the polling loop is testable without a broker.
type lagReader interface {
	Lag(ctx context.Context) (map[string]int64, error)
}

// pollQueueDepth publishes the backlog of every group this service consumes, until ctx is cancelled.
//
// Every failure mode is a skipped tick: a broker hiccup must not disturb routing, and a stale depth is
// better than a service that reacts to its own dashboard.
func pollQueueDepth(ctx context.Context, readers []lagReader, e *metricstream.Emitter, cat *metrics.Catalog, logger *slog.Logger) {
	ticker := time.NewTicker(queueDepthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishQueueDepth(ctx, readers, e, cat, logger)
		}
	}
}

// publishQueueDepth reads one tick's worth of backlog from each group and publishes it.
//
// The skipped tick is PER READER (step-201c, D16): a fault reading one group's lag leaves the others'
// depth untouched. Sharing the skip would let an mt.inbound outage erase mt.outcome's depth from the same
// scrape — and mt.outcome's depth is the numerator of the status-lag alert, which would then fall silent
// because of an unrelated neighbour.
func publishQueueDepth(ctx context.Context, readers []lagReader, e *metricstream.Emitter, cat *metrics.Catalog, logger *slog.Logger) {
	for _, r := range readers {
		lags, err := r.Lag(ctx)
		if err != nil {
			logger.DebugContext(ctx, "queue depth unavailable this tick", "err", err)
			continue
		}
		for topic, lag := range lags {
			e.Set("queue_depth_records", metricstream.Labels{"queue": topic}, float64(lag))
			cat.QueueDepth.WithLabelValues(topic).Set(float64(lag))
		}
	}
}
