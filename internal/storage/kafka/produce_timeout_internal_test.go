package kafka

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
)

// closedLoopbackPort binds and releases a port, so the address refuses connections; a fixed port such
// as 127.0.0.1:1 could be listened on.
func closedLoopbackPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestProduceFailsWithinTheProduceTimeoutWhenNoBrokerAnswers: with the only broker refusing
// connections, Produce returns within the bound with kgo.ErrRecordTimeout. The caller's context is five
// times longer on purpose: before step-260e the produce ran until the context expired.
func TestProduceFailsWithinTheProduceTimeoutWhenNoBrokerAnswers(t *testing.T) {
	t.Parallel()
	cfg := config.Kafka{
		Brokers:        []string{closedLoopbackPort(t)},
		Timeout:        200 * time.Millisecond,
		ProduceTimeout: time.Second, // the kgo floor
	}
	p, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer() = %v, want nil", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = p.Produce(ctx, Record{Topic: TopicMTInbound, Key: []byte("k"), Value: []byte("v")})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Produce() against a closed port = nil, want an error")
	}
	if !errors.Is(err, kgo.ErrRecordTimeout) {
		t.Errorf("Produce() error = %v, want kgo.ErrRecordTimeout: the record was not bounded by the produce timeout", err)
	}
	if elapsed >= 3*time.Second {
		t.Errorf("Produce() took %s, want about the 1s produce timeout: the bound did not fire", elapsed)
	}
	if ctx.Err() != nil {
		t.Error("the caller's context expired: Produce ran until the context did, not until its own bound")
	}
}
