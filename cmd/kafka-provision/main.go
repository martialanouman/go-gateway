// Command kafka-provision creates the pipeline's Kafka topics and widens them to the configured
// partition count (step-201, D7). It backs `make kafka-topics`.
//
// It is a tool, not a deployable service: it does its work and exits, exactly like `make migrate`.
// Nothing provisions a topic at a service's boot — replicas would race each other, a partial failure
// would happen mid-rollout, and no service would own the result. Provisioning is an operator act.
//
// Without it, KAFKA_TOPIC_PARTITIONS is a dead knob: nothing else in the repository creates a topic
// outside the test harness, and a broker's auto-creation gives ONE partition. A load run against such a
// cluster measures an inter-pod parallelism of 1 and blames the gateway for the ceiling.
//
// # What it does to each topic
//
//	absent          created with the configured partitions and replication factor
//	already right   nothing at all — no request is sent for it
//	too narrow      widened to the configured count
//	too wide        the whole run is REFUSED, before anything is applied
//
// Kafka cannot remove partitions from a topic, so a configuration asking for fewer than a topic has is
// an error, never a silent no-op. The same goes for KAFKA_TOPIC_PARTITIONS_OVERRIDES naming a topic
// that does not exist: config parses the list without knowing the topic registry, so "mt.inbund=48"
// would otherwise be accepted and silently ignored here.
//
// Re-running it is safe. It is also the only way to apply a raised KAFKA_TOPIC_PARTITIONS.
//
// # Run it outside peak hours
//
// Adding partitions to a topic RE-MAPS key → partition: records with the same key produced before and
// after the change can land on different partitions, so their relative order is not preserved across
// the change. For this pipeline that is benign — connector-pool shards binds itself, in the process,
// from rec.Key after the fetch (internal/connectorpool, shardIndex), so no ordering guarantee the
// gateway makes rests on a stable key → partition map. It is still a live change to the data plane:
// do it during a maintenance window, not at peak.
//
// It cannot change an existing topic's replication factor (that needs a partition reassignment). A
// topic whose replication differs from KAFKA_TOPIC_REPLICATION_FACTOR is reported as a warning rather
// than left silently under-replicated.
//
// Usage:
//
//	kafka-provision              apply the plan
//	kafka-provision -dry-run     print the plan, touch nothing
//
// Configuration comes from the environment, like every binary here: KAFKA_BROKERS,
// KAFKA_TOPIC_PARTITIONS, KAFKA_TOPIC_PARTITIONS_OVERRIDES, KAFKA_TOPIC_REPLICATION_FACTOR,
// KAFKA_TIMEOUT.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/storage/kafkaprovision"
)

const serviceName = "kafka-provision"

func main() {
	dryRun := flag.Bool("dry-run", false, "print what would be done and exit without touching the cluster")
	flag.Parse()

	if err := run(*dryRun); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run(dryRun bool) error {
	cfg, err := config.Load(serviceName, config.SectionKafka)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	overrides, err := cfg.Kafka.PartitionOverrides()
	if err != nil {
		return err
	}
	provisionCfg := kafkaprovision.Config{
		Partitions:        cfg.Kafka.TopicPartitions,
		Overrides:         overrides,
		ReplicationFactor: cfg.Kafka.TopicReplicationFactor,
	}

	adm, err := kafkaprovision.NewAdmin(cfg.Kafka.Brokers, cfg.Kafka.Timeout)
	if err != nil {
		return err
	}
	defer adm.Close()

	// A partition change is a live data-plane change: an interrupted run must not leave the operator
	// wondering how far it got, so the context is cancelled and the error says which topic was in flight.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	logger.Info("provisioning kafka topics",
		"brokers", cfg.Kafka.Brokers,
		"partitions", provisionCfg.Partitions,
		"replication_factor", provisionCfg.ReplicationFactor,
		"overrides", cfg.Kafka.TopicPartitionsOverrides,
		"dry_run", dryRun)

	plan, err := apply(ctx, adm, provisionCfg, dryRun)
	report(logger, plan)
	if err != nil {
		return err
	}

	switch {
	case dryRun:
		logger.Info("dry run: nothing was applied", "pending", plan.Pending())
	case plan.Pending() == 0:
		logger.Info("every topic already matched the configuration; nothing was applied")
	default:
		logger.Info("topics provisioned", "changed", plan.Pending())
	}
	return nil
}

func apply(ctx context.Context, adm kafkaprovision.Admin, cfg kafkaprovision.Config, dryRun bool) (kafkaprovision.Plan, error) {
	if dryRun {
		return kafkaprovision.DryRun(ctx, adm, cfg)
	}
	return kafkaprovision.Provision(ctx, adm, cfg)
}

// report prints the plan line by line. It runs even when the run failed: a refused plan is empty, but a
// run that failed halfway is exactly when an operator needs to see what it had decided to do.
func report(logger *slog.Logger, plan kafkaprovision.Plan) {
	for _, change := range plan.Changes {
		fmt.Println(change)
	}
	for _, warning := range plan.Warnings {
		logger.Warn(warning)
	}
}
