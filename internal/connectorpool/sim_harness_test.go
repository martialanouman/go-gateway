package connectorpool_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// simPool is a connector pool wired against real Kafka + ClickHouse and a real (simulator or proxied)
// SMSC bind, for the M8 resilience scenarios (step-130). It exposes the collaborators a test drives: a
// producer to inject mt.routed records, the pool Service (for LinkStatus/BindReady), and a CDR reader to
// assert outcomes. A UNIQUE connector id per pool gives it a disjoint consumer group and Redis namespace,
// so pools never cross-talk even though they share the mt.routed topic (the connector-id filter, option
// B, skips-and-commits foreign records).
type simPool struct {
	svc      *connectorpool.Service
	producer *kafka.Producer
	cdr      *clickhouse.CDRReader
	connID   uuid.UUID
	group    string // the pool's consumer group, for waitGroupStable
}

// simPoolConfig parameterises startSimPool. Only BindAddr/SystemID/Password are required; the rest carry
// resilience-test defaults tuned so the breaker opens fast and the response timeout stays below the
// cooldown (the documented breaker invariant). BindPoolSize 0 means 1.
type simPoolConfig struct {
	BindAddr        string
	SystemID        string
	Password        string
	BindPoolSize    int
	ResponseTimeout time.Duration

	// Resilience wiring (130b), enabled when Redis is non-nil: the per-bind breaker feeds a cross-pod
	// aggregate published every 100ms (kept well below the test cooldown, the documented invariant).
	// ConnID lets several pools share ONE connector (the multi-pod aggregate scenario); zero mints a fresh
	// one. PodID distinguishes replicas in the quorum.
	Redis         *goredis.Client
	ConnID        uuid.UUID
	PodID         string
	BreakerConfig breaker.Config
}

// startSimPool brings up a connector pool against the given bind and returns it. Teardown is ordered
// (cancel → join Run → close Kafka/ClickHouse) via t.Cleanup, so no goroutine outlives the test (P4).
// The mt.routed consumer reads FromLatest (so it never churns through the shared topic's history other
// tests left behind); the caller must waitGroupStable before injecting so no record is missed. The
// connector-id filter discards other pools' records.
func startSimPool(t *testing.T, cfg simPoolConfig) *simPool {
	t.Helper()
	kafkaCfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	chConn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("clickhouse conn: %v", err)
	}
	producer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	connID := cfg.ConnID
	if connID == uuid.Nil {
		connID = uuid.New()
	}
	// A per-pod consumer group name: a distinct connID gives a fresh group; when several pods SHARE a
	// connID (the multi-pod aggregate scenario) the PodID disambiguates so both actually join the group
	// and its partitions split between them, exactly as two real pods do.
	// The consumer group is SHARED by all pods of one connector (same connID) so its partitions split
	// across them, exactly like real pods; the group reads FromLatest so it never churns through the
	// mt.routed history other tests left on the shared topic. A record is injected only after
	// waitGroupStable, so FromLatest loses nothing.
	group := "sim-conn-" + connID.String()
	consumer, err := kafka.NewConsumerFromLatest(kafkaCfg, group, kafka.TopicMTRouted)
	if err != nil {
		t.Fatalf("kafka consumer: %v", err)
	}

	responseTimeout := cfg.ResponseTimeout
	if responseTimeout <= 0 {
		responseTimeout = 500 * time.Millisecond
	}
	deps := connectorpool.Deps{
		Consumer:    consumer,
		CDR:         clickhouse.NewCDRWriter(chConn),
		ConnectorID: connID,
		Bind: connectorpool.BindConfig{
			Addr: cfg.BindAddr, SystemID: cfg.SystemID, Password: cfg.Password,
			DialTimeout: 3 * time.Second, ResponseTimeout: responseTimeout,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
			BindPoolSize: cfg.BindPoolSize,
		},
		Tracer: observability.Tracer(nil, "connector-sim"),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if cfg.Redis != nil {
		podID := cfg.PodID
		if podID == "" {
			podID = "pod-" + uuid.NewString()
		}
		deps.Breaker = breaker.NewAggregator(cfg.Redis, redisstore.NewPubSubPublisher(cfg.Redis), podID)
		deps.BreakerConfig = cfg.BreakerConfig
		deps.BreakerHeartbeat = 100 * time.Millisecond // fast, and below the test cooldown (the invariant)
		deps.PodID = podID
	}
	svc := connectorpool.New(deps)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		producer.Close()
		consumer.Close()
		_ = chConn.Close()
	})

	return &simPool{svc: svc, producer: producer, cdr: clickhouse.NewCDRReader(chConn), connID: connID, group: group}
}

// injectRouted produces one mt.routed record addressed to this pool's connector, carrying the given
// fallback chain, and returns its identity so a test can read the resulting CDR back. The body is a fixed
// short message — this harness never asserts on content, only on routing/outcome.
func (p *simPool) injectRouted(t *testing.T, chain []uuid.UUID) routedIdent {
	t.Helper()
	return p.injectRoutedWithID(t, uuid.New(), chain)
}

// injectRoutedWithID is injectRouted with a caller-chosen message id — the Kafka partition key AND the
// pool's shard key (FNV(message_id) % bind_pool_size), so a bind-pool test can spread a burst evenly
// across the binds instead of leaving the split to a random UUID.
func (p *simPool) injectRoutedWithID(t *testing.T, messageID uuid.UUID, chain []uuid.UUID) routedIdent {
	t.Helper()
	id := routedIdent{messageID: messageID, customerID: uuid.New(), accountID: uuid.New()}
	rec, err := pipeline.EncodeRouted(pipeline.RoutedMT{
		MessageID: id.messageID, TraceID: uuid.New(), AccountID: id.accountID, CustomerID: id.customerID,
		From: "GATEWAY", To: "+2250700000000", Body: msg.NewBodyString("resilience probe"),
		Encoding: "gsm7", ConnectorID: p.connID, FallbackChain: chain,
		SegmentSeq: 1, SegmentCount: 1, SubmittedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	if err := p.producer.Produce(context.Background(), rec); err != nil {
		t.Fatalf("produce mt.routed: %v", err)
	}
	return id
}

// routedIdent is the identity of an injected message, enough to read its CDR row back.
type routedIdent struct {
	messageID  uuid.UUID
	customerID uuid.UUID
	accountID  uuid.UUID
}

// waitCDRStatus polls the CDR until the message reaches want, or fails at the deadline with the last
// status seen. A missing row (not yet written) is retried, not a failure.
func (p *simPool) waitCDRStatus(t *testing.T, id routedIdent, want clickhouse.Status, within time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(within)
	var last clickhouse.Status = "(no row yet)"
	var lastErr error
	for time.Now().Before(deadline) {
		row, ok, err := p.cdr.Current(ctx, id.customerID, id.accountID, id.messageID)
		switch {
		case err != nil:
			lastErr = err
		case ok:
			last = row.Status
			if row.Status == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("message %s never reached CDR status %q (last: %q, last read error: %v)", id.messageID, want, last, lastErr)
}
