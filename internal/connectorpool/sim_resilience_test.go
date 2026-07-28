package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
)

// waitBreakerState polls the connector's cross-pod breaker aggregate (breaker:state:{id}) until it
// reaches want, or fails at the deadline with the last token seen. This is how a test proves "the breaker
// opened within N seconds" rather than sleeping a fixed guess.
func waitBreakerState(t *testing.T, rdb *goredis.Client, connID uuid.UUID, want breaker.State, within time.Duration) {
	t.Helper()
	ctx := context.Background()
	key := "breaker:state:{" + connID.String() + "}"
	deadline := time.Now().Add(within)
	last := "(unset)"
	for time.Now().Before(deadline) {
		v, err := rdb.Get(ctx, key).Result()
		if err == nil {
			last = v
			if st, ok := breaker.ParseState(v); ok && st == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("breaker:state{%s} never reached %v within %s (last: %q)", connID, want, within, last)
}

// injectRoutedTo produces one mt.routed record addressed to connID (not necessarily this pool's own
// connector) carrying chain, for a test that drives a specific connector's inbox directly.
func (p *simPool) injectRoutedTo(t *testing.T, connID uuid.UUID, chain []uuid.UUID) routedIdent {
	t.Helper()
	id := routedIdent{messageID: uuid.New(), customerID: uuid.New(), accountID: uuid.New()}
	rec, err := pipeline.EncodeRouted(pipeline.RoutedMT{
		MessageID: id.messageID, TraceID: uuid.New(), AccountID: id.accountID, CustomerID: id.customerID,
		From: "GATEWAY", To: "+2250700000000", Body: msg.NewBodyString("resilience probe"),
		Encoding: "gsm7", ConnectorID: connID, FallbackChain: chain,
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
