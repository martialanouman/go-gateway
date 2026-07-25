package modlrrouter_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestDLRCorrelationUpdatesCDR is the end-to-end DLR path: an enroute row is superseded by a delivered
// row once the correlated receipt flows through Kafka -> Redis lookup -> ClickHouse, and the current
// (highest-version) row is the final status with latency.
func TestDLRCorrelationUpdatesCDR(t *testing.T) {
	kafkaCfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	chConn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = chConn.Close() })
	rdb := redistest.Client(t)
	ctx := context.Background()

	writer := clickhouse.NewCDRWriter(chConn)
	reader := clickhouse.NewCDRReader(chConn)
	store := dlrmap.NewRedisMap(rdb)

	// The submitted message: its enroute row and its dlrmap entry.
	connectorID := uuid.New()
	smscID := "00000000000000ff"
	submittedAt := time.Now().UTC().Add(-3 * time.Second).Truncate(time.Millisecond)
	routed := pipeline.RoutedMT{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		From:         "GATEWAY",
		To:           "+22507123456",
		Body:         msg.NewBodyString("delivered body"),
		Encoding:     "gsm7",
		ConnectorID:  connectorID,
		SegmentCount: 1,
		SubmittedAt:  submittedAt,
	}
	if err := store.Put(ctx, smscID, routed); err != nil {
		t.Fatalf("dlrmap Put: %v", err)
	}
	enroute := clickhouse.CDRRow{
		MessageID: routed.MessageID, TraceID: routed.TraceID, AccountID: routed.AccountID,
		CustomerID: routed.CustomerID, Direction: clickhouse.DirectionMT, SourceAddr: routed.From,
		DestAddr: routed.To, ConnectorID: &connectorID, SubmittedAt: submittedAt,
		Status: clickhouse.StatusEnroute, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}
	if err := writer.Insert(ctx, enroute); err != nil {
		t.Fatalf("write enroute cdr: %v", err)
	}

	// Produce the delivery receipt.
	producer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(producer.Close)
	rec, err := pipeline.EncodeDLR(pipeline.DLREvent{
		ConnectorID:   connectorID,
		SMSCMessageID: smscID,
		State:         smpp.MessageStateDelivered,
		Stat:          "DELIVRD",
		ReceivedAt:    submittedAt.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("encode dlr: %v", err)
	}
	if err := producer.Produce(ctx, rec); err != nil {
		t.Fatalf("produce dlr: %v", err)
	}

	// Run the router on a unique group so it reads from the earliest offset.
	consumer, err := kafka.NewConsumer(kafkaCfg, "test-modlr-"+uuid.NewString(), kafka.TopicDLREvents)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	svc := modlrrouter.New(modlrrouter.Deps{
		Consumer: consumer,
		Resolver: store,
		CDR:      writer,
		Tracer:   observability.Tracer(nil, "test"),
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- svc.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })

	// Poll until the current row is delivered.
	deadline := time.Now().Add(15 * time.Second)
	for {
		row, found, err := reader.Current(ctx, routed.CustomerID, routed.AccountID, routed.MessageID)
		if err != nil {
			t.Fatalf("read cdr: %v", err)
		}
		if found && row.Status == clickhouse.StatusDelivered {
			if row.DeliveredAt == nil {
				t.Error("delivered row missing delivered_at")
			}
			if row.LatencyMs == nil || *row.LatencyMs < 2000 || *row.LatencyMs > 4000 {
				t.Errorf("latency_ms = %v, want ~3000", row.LatencyMs)
			}
			if row.SourceAddr != routed.From || row.DestAddr != routed.To {
				t.Errorf("delivered row blanked the projection: source=%q dest=%q", row.SourceAddr, row.DestAddr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for delivered row (found=%v status=%q)", found, row.Status)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
