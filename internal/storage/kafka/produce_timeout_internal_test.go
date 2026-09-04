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

// closedLoopbackPort returns a host:port nothing listens on: a port is bound and released, so the
// address is valid and refuses connections. Never a fixed port such as 127.0.0.1:1 — nothing
// guarantees it is free, and a listener there would turn this test into a hang.
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

// TestProduceFailsWithinTheProduceTimeoutWhenNoBrokerAnswers is the behaviour behind the option, proven
// without a container: with the only broker refusing connections, Produce must return within about
// KAFKA_PRODUCE_TIMEOUT with kgo.ErrRecordTimeout — not run until the caller's context expires, which
// was the pre-step-260e behaviour (franz-go retries a record without limit by default). The caller's
// context is deliberately five times longer than the bound, so a produce that only stops when the
// context does fails this test on both the error and the elapsed time.
func TestProduceFailsWithinTheProduceTimeoutWhenNoBrokerAnswers(t *testing.T) {
	t.Parallel()
	cfg := config.Kafka{
		Brokers:        []string{closedLoopbackPort(t)},
		Timeout:        200 * time.Millisecond,
		ProduceTimeout: time.Second, // franz-go's floor: the suite pays one second, not more
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
