// Command mt-replay re-injects dead-lettered MT messages back onto mt.routed (step-129).
//
// It is a tool, not a deployable service: it has no ops port. It consumes mt.dead-letter from the
// beginning and republishes each record verbatim to mt.routed, stamped with a replayed_at header and
// stripped of its dead_letter_reason — so the replay stays correlated (same message_id/trace_id,
// billing idempotent) and the pool's max-age SLA does not instantly re-expire it. It runs until
// interrupted (SIGTERM / Ctrl-C): a replay produces to mt.routed, never back to mt.dead-letter, so it
// cannot feed its own tail — the operator stops it once the parked backlog is drained, and the final
// log line reports how many were replayed.
//
// Usage:
//
//	mt-replay        drain mt.dead-letter into mt.routed until interrupted
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

const serviceName = "mt-replay"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cfg, err := config.Load(serviceName, config.SectionKafka)
	if err != nil {
		return err
	}
	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return err
	}
	defer producer.Close()

	// AtStart: the parked dead-letters are durable and must all be drained. A dedicated, fixed group so
	// a re-run resumes where a previous drain left off rather than re-replaying the whole topic.
	consumer, err := kafka.NewConsumer(cfg.Kafka, serviceName, kafka.TopicMTDeadLetter)
	if err != nil {
		return err
	}
	defer consumer.Close()

	replayer := connectorpool.NewReplayer(connectorpool.ReplayerDeps{Producer: producer, Logger: logger})
	logger.InfoContext(ctx, "replaying mt.dead-letter into mt.routed; interrupt to stop")
	err = replayer.Run(ctx, consumer)
	logger.InfoContext(ctx, "mt-replay stopped", "replayed", replayer.Replayed())
	return err
}
