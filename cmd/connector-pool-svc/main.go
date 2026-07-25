// Command connector-pool-svc is the outbound SMSC leg (M2): a single SMPP bind that consumes
// mt.routed, submits each message, and records the outcome in the CDR. It follows the canonical
// service lifecycle, adding a Kafka consumer, a ClickHouse connection and the SMPP bind.
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

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

const serviceName = "connector-pool-svc"

// connectorEnv is the M2 bind configuration. It mirrors the relevant smsc_connectors columns; the
// defaults point at the local fake SMSC.
type connectorEnv struct {
	Addr                 string        `env:"CONNECTOR_ADDR" envDefault:"localhost:2775"`
	SystemID             string        `env:"CONNECTOR_SYSTEM_ID" envDefault:"gateway"`
	Password             string        `env:"CONNECTOR_PASSWORD" envDefault:"gateway"`
	SystemType           string        `env:"CONNECTOR_SYSTEM_TYPE" envDefault:""`
	DialTimeout          time.Duration `env:"CONNECTOR_DIAL_TIMEOUT" envDefault:"5s"`
	ResponseTimeout      time.Duration `env:"CONNECTOR_RESPONSE_TIMEOUT" envDefault:"5s"`
	EnquireLinkInterval  time.Duration `env:"CONNECTOR_ENQUIRE_LINK_INTERVAL" envDefault:"30s"`
	EnquireLinkMaxMissed int           `env:"CONNECTOR_ENQUIRE_LINK_MAX_MISSED" envDefault:"3"`
	WindowSize           int           `env:"CONNECTOR_WINDOW_SIZE" envDefault:"10"`
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
		config.SectionOTel, config.SectionKafka, config.SectionClickHouse, config.SectionRedis)
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

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTRouted)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// Redis backs the cancel-flag check before each submit_sm: a cancel_sm on smpp-server-svc flags a
	// message here so it is not dispatched. NewClient pings eagerly, so Redis must be reachable at boot
	// (a startup outage crash-loops the pod, as everywhere else). At RUNTIME it is deliberately NOT a
	// readiness dependency and the flag check fails OPEN (connectorpool.handler): a Redis outage lets
	// delivery continue rather than halting all outbound traffic — cancellation is best-effort.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	tracer := observability.Tracer(nil, serviceName)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    consumer,
		CDR:         clickhouse.NewCDRWriter(chConn),
		CancelFlags: cancel.NewRedisFlags(rdb),
		DLRMap:      dlrmap.NewRedisMap(rdb),
		Bind: connectorpool.BindConfig{
			Addr:                 bindEnv.Addr,
			SystemID:             bindEnv.SystemID,
			Password:             bindEnv.Password,
			SystemType:           bindEnv.SystemType,
			DialTimeout:          bindEnv.DialTimeout,
			ResponseTimeout:      bindEnv.ResponseTimeout,
			EnquireLinkInterval:  bindEnv.EnquireLinkInterval,
			EnquireLinkMaxMissed: bindEnv.EnquireLinkMaxMissed,
			WindowSize:           bindEnv.WindowSize,
		},
		Tracer: tracer,
		Logger: logger,
	})

	// Vital dependencies (plan §1.5): Kafka (no work without it), ClickHouse (the outcome is recorded
	// there) and the SMSC bind itself — the pool cannot deliver a single message without a live bind,
	// and an idle-time bind drop would otherwise leave the pod Ready with nothing behind it.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		observability.ReadinessCheck{Name: "smsc-bind", Probe: svc.BindReady},
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}

	logger.InfoContext(ctx, "starting", "config", cfg, "connector_addr", bindEnv.Addr)

	// Ops and the connector pool tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("connector pool", svc.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}
