// Command connector-pool-svc is the outbound SMSC leg (M2): a single SMPP bind that consumes
// mt.routed, submits each message, and records the outcome in the CDR. It follows the canonical
// service lifecycle, adding a Kafka consumer, a ClickHouse connection and the SMPP bind. The service
// graph itself is built by wiring.go.
//
// The bind endpoint is read from the environment here rather than from the connectors control plane:
// the outbound password cannot be recovered from its stored hash, and M2 has no config-sync. This
// env block is the M2 stopgap; M3+ sources connectors from the control plane.
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

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

const serviceName = "connector-pool-svc"

// connectorEnv is the M2 bind configuration. It mirrors the relevant smsc_connectors columns; the
// defaults point at the local fake SMSC.
type connectorEnv struct {
	Addr                 string        `env:"CONNECTOR_ADDR" envDefault:"localhost:2775"`
	SystemID             string        `env:"CONNECTOR_SYSTEM_ID" envDefault:"gateway"`
	Password             string        `env:"CONNECTOR_PASSWORD" envDefault:"gateway"`
	SystemType           string        `env:"CONNECTOR_SYSTEM_TYPE" envDefault:""`
	ID                   uuid.UUID     `env:"CONNECTOR_ID" envDefault:"00000000-0000-0000-0000-000000000001"`
	DialTimeout          time.Duration `env:"CONNECTOR_DIAL_TIMEOUT" envDefault:"5s"`
	ResponseTimeout      time.Duration `env:"CONNECTOR_RESPONSE_TIMEOUT" envDefault:"5s"`
	EnquireLinkInterval  time.Duration `env:"CONNECTOR_ENQUIRE_LINK_INTERVAL" envDefault:"30s"`
	EnquireLinkMaxMissed int           `env:"CONNECTOR_ENQUIRE_LINK_MAX_MISSED" envDefault:"3"`
	WindowSize           int           `env:"CONNECTOR_WINDOW_SIZE" envDefault:"10"`
	// BindPoolSize is the number of parallel SMPP binds (bind_pool_size, 1..32). Sourced from the
	// connectors control plane at M3+; the pool shards mt.routed across the binds (step-124).
	BindPoolSize int `env:"CONNECTOR_BIND_POOL_SIZE" envDefault:"1"`
	// MaxSendRate is the connector's throughput_limit_per_sec, the ceiling for the adaptive throttle
	// (step-086). Zero disables the AIMD pacing. Sourced from the connectors control plane at M3+.
	MaxSendRate float64 `env:"CONNECTOR_MAX_SEND_RATE" envDefault:"0"`
	// Auto-reconnection policy (step-127), mirroring the reconnect_* columns. Disabled by default (opt-in);
	// M3+ sources these from the control plane.
	AutoReconnect         bool          `env:"CONNECTOR_AUTO_RECONNECT" envDefault:"false"`
	ReconnectInitialDelay time.Duration `env:"CONNECTOR_RECONNECT_INITIAL_DELAY" envDefault:"1s"`
	ReconnectMultiplier   float64       `env:"CONNECTOR_RECONNECT_MULTIPLIER" envDefault:"2.0"`
	ReconnectMaxDelay     time.Duration `env:"CONNECTOR_RECONNECT_MAX_DELAY" envDefault:"60s"`
	ReconnectJitterPct    int           `env:"CONNECTOR_RECONNECT_JITTER_PCT" envDefault:"20"`
	ReconnectMaxAttempts  int           `env:"CONNECTOR_RECONNECT_MAX_ATTEMPTS" envDefault:"0"`
	// Dead-letter guards (step-129). RetryWindow dead-letters (retries_exhausted) a message with no
	// fallback chain that keeps hitting a connector-health failure past this window, so a poison record
	// cannot redeliver forever on a dead connector. MaxMessageAge dead-letters (delivery_expired) any
	// message older than this — the gateway's own validity SLA, covering messages that age out in
	// throttle backpressure. Both default off (the pre-step-129 redeliver-forever behaviour); M3+ may
	// source them from the control plane's validity policy.
	RetryWindow   time.Duration `env:"CONNECTOR_RETRY_WINDOW" envDefault:"0"`
	MaxMessageAge time.Duration `env:"CONNECTOR_MAX_MESSAGE_AGE" envDefault:"0"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionKafka, config.SectionClickHouse, config.SectionRedis, config.SectionPostgres, config.SectionBilling)
	if err != nil {
		return err
	}

	var bindEnv connectorEnv
	if err := env.Parse(&bindEnv); err != nil {
		return fmt.Errorf("load connector config: %w", err)
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

	app, err := newPoolApp(ctx, cfg, bindEnv, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg, "connector_addr", bindEnv.Addr)

	// Ops and the connector pool tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.OnDrain(app.ops.DrainHook(cfg.DrainDelay))
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("connector pool", app.pool.Run)
	g.Add("reroute-park drainer", app.drainer.Run)
	g.Add("metric stream", func(c context.Context) error {
		app.emitter.Run(c, metricStreamInterval)
		return nil
	})
	// Queue depth is a LEVEL polled on a slow tick because it costs a broker round-trip, and
	// connector-pool-svc is the ONE owner of mt.routed's depth (step-180). The breaker state is published by
	// the pool's own heartbeat, which runs inside the dial cycle — polling it from here would race the
	// re-dial that reassigns the breakers.
	g.Add("queue depth", func(c context.Context) error {
		pollQueueDepth(c, app.consumer, app.emitter, app.catalog, logger)
		return nil
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// metricStreamInterval is how often a snapshot reaches metrics.stream, well inside the < 5 s dashboard
// freshness criterion (§15) while bounding the topic to one record per service per second.
const metricStreamInterval = time.Second

// queueDepthInterval paces the depth poll. Slower than the snapshot tick because each poll is a broker
// round-trip; still fresh enough to watch a backlog form.
const queueDepthInterval = 5 * time.Second

// pollQueueDepth publishes the consumer group's backlog until ctx is cancelled. Every failure mode is a
// skipped tick: a broker hiccup must not disturb sending.
func pollQueueDepth(ctx context.Context, c *kafka.Consumer, e *metricstream.Emitter, cat *metrics.Catalog, logger *slog.Logger) {
	ticker := time.NewTicker(queueDepthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lags, err := c.Lag(ctx)
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
}
