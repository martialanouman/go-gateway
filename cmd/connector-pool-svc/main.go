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
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
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

	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionKafka, config.SectionClickHouse)
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
	//nolint:contextcheck // Detaching is the point: see drainTracing's comment.
	defer drainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

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

	tracer := observability.Tracer(nil, serviceName)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: consumer,
		CDR:      clickhouse.NewCDRWriter(chConn),
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

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ops.Run(runCtx, cfg.ShutdownTimeout); err != nil {
			select {
			case errCh <- fmt.Errorf("ops server: %w", err):
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := svc.Run(runCtx); err != nil {
			select {
			case errCh <- fmt.Errorf("connector pool: %w", err):
			default:
			}
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}
	cancel()
	wg.Wait()

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
	}

	logger.Info("stopped")
	return nil
}

// drainTracing flushes buffered spans on the way out, on a context detached from the cancelled
// service context.
func drainTracing(shutdown observability.ShutdownFunc, timeout time.Duration, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("flush traces on shutdown", "err", err)
	}
}
