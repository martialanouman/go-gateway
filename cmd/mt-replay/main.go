// Command mt-replay re-injects dead-lettered MT messages back onto mt.routed (step-129).
//
// It is a tool, not a deployable service: it has no ops port. It consumes mt.dead-letter from the
// beginning and republishes records verbatim to mt.routed, stamped with a replayed_at header and
// stripped of its dead_letter_reason — so the replay stays correlated (same message_id/trace_id,
// billing idempotent) and the pool's max-age SLA does not instantly re-expire it. It runs until
// interrupted (SIGTERM / Ctrl-C): a replay produces to mt.routed, never back to mt.dead-letter, so it
// cannot feed its own tail — the operator stops it once the parked backlog is drained.
//
// It does not replay everything it reads. A message the customer cancelled before it was parked must
// not go back on the wire (step-240), so the tool reads each message's current CDR status and leaves
// the cancelled ones parked. That is why it needs ClickHouse as well as Kafka, and why an unreadable
// status STOPS the drain rather than skipping a record: the offset stays uncommitted, so a re-run
// resumes exactly where it left off and nothing is lost. The final log line reports all three counts —
// replayed, refused as cancelled, refused for want of a CDR row.
//
// A replay puts real SMS back on the wire, so it leaves a row in control_plane.audit_log before it
// consumes anything, and cannot start without one (step-296). The operator is DECLARED, not
// authenticated: the tool has no principal, and whoever holds the Kafka credentials can produce on
// mt.routed without it. Real accountability needs per-operator credentials (step-310). The run id is
// logged at start too, so a row a kill -9 left without an outcome still finds its logs.
//
// Usage:
//
//	mt-replay -operator <name>   drain mt.dead-letter into mt.routed until interrupted
package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

const serviceName = "mt-replay"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	name := flag.String("operator", "", "who runs this replay; recorded in the audit trail (required)")
	flag.Parse()
	operator, err := declaredOperator(*name)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName, config.SectionKafka, config.SectionClickHouse, config.SectionPostgres)
	if err != nil {
		return err
	}
	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	pg, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pg.Close()

	producer, err := kafka.NewProducer(cfg.Kafka, kafka.ForFailClosedConsumer())
	if err != nil {
		return err
	}
	defer producer.Close()

	// The replay needs to know whether a parked message was cancelled before it was parked, and only the
	// CDR is durable enough to answer: the cancel token expires after 72h, which is exactly the delay
	// past which a replay becomes dangerous (step-240).
	ch, err := clickhouse.NewConn(cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{
		Producer: producer,
		CDR:      clickhouse.NewCDRReader(ch),
		Logger:   logger,
	})
	runID := uuid.NewString()
	err = auditedReplay(ctx, postgres.NewAuditLogRepo(pg), operator, runID, func(ctx context.Context) error {
		// Created only once the row exists: the client joins the group on construction, and a refused run
		// must not rebalance partitions away from a replay already running.
		// AtStart: the parked dead-letters are durable and must all be drained. A dedicated, fixed group so
		// a re-run resumes where a previous drain left off rather than re-replaying the whole topic.
		consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTDeadLetter)
		if err != nil {
			return err
		}
		defer consumer.Close()
		logger.InfoContext(ctx, "replaying mt.dead-letter into mt.routed; interrupt to stop",
			"operator", operator, "run_id", runID)
		return replayer.Run(ctx, consumer)
	})
	// The tool has no ops port, so this line IS the report: the refusals only exist here. An operator who
	// drained a backlog needs to know what did not go back on the wire, and why.
	logger.InfoContext(ctx, "mt-replay stopped",
		"run_id", runID,
		"replayed", replayer.Replayed(),
		"refused_cancelled", replayer.Refused(),
		"refused_absent", replayer.RefusedAbsent())
	return err
}
