// Command mo-dlr-router-svc is the return-path router (M4). This step runs the DLR leg: it consumes
// dlr.events, correlates each delivery receipt back to its message through the dlrmap (§1.11), and
// records the final delivery outcome as a versioned CDR row. It follows the canonical service
// lifecycle with a Kafka consumer, a ClickHouse connection and a Redis client. The MO leg (mo.inbound)
// lands in step-045.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

const serviceName = "mo-dlr-router-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName,
		config.SectionOTel, config.SectionKafka, config.SectionClickHouse, config.SectionRedis,
		config.SectionPostgres)
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

	chConn, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	defer func() { _ = chConn.Close() }()

	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicDLREvents)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer consumer.Close()

	// Redis backs the DLR correlation lookup. NewClient pings eagerly, so Redis must be reachable at
	// boot. At RUNTIME it is deliberately NOT a readiness dependency: a receipt whose mapping cannot be
	// read is retried (an infra error) or counted (a genuine miss), so a Redis blip self-heals rather
	// than taking the pod out of rotation.
	rdb, err := redisstore.NewClient(ctx, cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()

	// dlr_unmapped_total counts receipts with no mapping (expired or unknown smsc_msg_id) — the receipt
	// is logged and counted, never dropped silently. No labels: the cardinality-bounded rule forbids a
	// message id / MSISDN / connector id here.
	unmapped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dlr_unmapped_total",
		Help: "Delivery receipts received with no dlrmap entry (expired or unknown smsc_msg_id).",
	})

	svc := modlrrouter.New(modlrrouter.Deps{
		Consumer: consumer,
		Resolver: dlrmap.NewRedisMap(rdb),
		CDR:      clickhouse.NewCDRWriter(chConn),
		Unmapped: unmapped,
		Tracer:   observability.Tracer(nil, serviceName),
		Logger:   logger,
	})

	// --- MO leg (step-045): resolve mo.inbound to an account and publish mo.routed ---
	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	// The routing table is cold-loaded once at boot; a hot reload is a later milestone.
	snapshot, err := modlrrouter.LoadSnapshot(ctx, logger,
		postgres.NewInboundNumberRepo(pool), postgres.NewInboundKeywordRepo(pool), postgres.NewAccountRepo(pool))
	if err != nil {
		return fmt.Errorf("load mo routing snapshot: %w", err)
	}

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	moConsumer, err := kafka.NewConsumer(cfg.Kafka, serviceName+"-mo", kafka.TopicMOInbound)
	if err != nil {
		return fmt.Errorf("kafka mo consumer: %w", err)
	}
	defer moConsumer.Close()

	// mo_unrouted_total{connector_id, reason}: bounded labels (few connectors, three reasons) — never a
	// MSISDN or body, per the cardinality rule.
	unroutedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mo_unrouted_total",
		Help: "Mobile-originated messages that resolved to no account, by connector and reason.",
	}, []string{"connector_id", "reason"})

	moRouter := modlrrouter.NewMORouter(modlrrouter.MODeps{
		Consumer: moConsumer,
		Snapshot: snapshot,
		Producer: producer,
		Unrouted: postgres.NewUnroutedMORepo(pool),
		Metric:   unroutedMetric{vec: unroutedTotal},
		Tracer:   observability.Tracer(nil, serviceName),
		Logger:   logger,
	})

	// Vital dependencies (plan §1.5): Kafka (no work without it) and ClickHouse (the delivery outcome is
	// recorded there). Redis is intentionally absent — see its comment above.
	ops, err := observability.NewOpsServer(cfg, logger,
		consumer.ReadyCheck("kafka", cfg.Kafka.Timeout),
		producer.ReadyCheck("kafka-producer", cfg.Kafka.Timeout),
		chConn.ReadyCheck("clickhouse", cfg.ClickHouse.Timeout),
		postgres.PingCheck("postgres", pool, cfg.Postgres.Timeout),
	)
	if err != nil {
		return fmt.Errorf("init ops server: %w", err)
	}
	ops.Registry().MustRegister(unmapped, unroutedTotal)

	logger.InfoContext(ctx, "starting", "config", cfg)

	// Ops and the router tear down together; the unordered supervisor fits (guide de codage §5).
	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("dlr router", svc.Run)
	g.Add("mo router", moRouter.Run)
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// unroutedMetric adapts a Prometheus CounterVec to modlrrouter.UnroutedMetric.
type unroutedMetric struct{ vec *prometheus.CounterVec }

func (m unroutedMetric) Inc(connectorID, reason string) {
	m.vec.WithLabelValues(connectorID, reason).Inc()
}
