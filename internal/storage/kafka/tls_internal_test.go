package kafka

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// TestEveryKafkaClientDialsOverTLSWhenEnabled: each constructor goes through the shared options, so each one
// completes a handshake with a broker signed by the configured CA.
func TestEveryKafkaClientDialsOverTLSWhenEnabled(t *testing.T) {
	ca := tlstest.NewCA(t)
	clients := map[string]func(config.Kafka) (*kgo.Client, error){
		"producer": func(c config.Kafka) (*kgo.Client, error) {
			p, err := NewProducer(c)
			if err != nil {
				return nil, err
			}
			return p.cl, nil
		},
		"consumer": func(c config.Kafka) (*kgo.Client, error) {
			cons, err := NewConsumer(c, "group", "topic")
			if err != nil {
				return nil, err
			}
			return cons.cl, nil
		},
		"tail reader": func(c config.Kafka) (*kgo.Client, error) {
			cons, err := NewTailReader(c, "topic")
			if err != nil {
				return nil, err
			}
			return cons.cl, nil
		},
		"stream producer": func(c config.Kafka) (*kgo.Client, error) {
			s, err := NewStreamProducer(c)
			if err != nil {
				return nil, err
			}
			return s.cl, nil
		},
	}
	for name, build := range clients {
		t.Run(name, func(t *testing.T) {
			addr, handshakes := ca.HandshakePeer(t)
			cl, err := build(config.Kafka{Brokers: []string{addr}, Timeout: 2 * time.Second, TLSEnabled: true, TLSCAFile: ca.CAFile})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = cl.Ping(ctx)
			select {
			case <-handshakes:
			case <-time.After(3 * time.Second):
				t.Fatal("no TLS handshake reached the broker")
			}
		})
	}
}

// TestAnUnreadableKafkaCAIsABootError: the CA is read when the client is built, not at the first dial.
func TestAnUnreadableKafkaCAIsABootError(t *testing.T) {
	cfg := config.Kafka{Brokers: []string{"localhost:9093"}, TLSEnabled: true, TLSCAFile: filepath.Join(t.TempDir(), "absent.crt")}
	if p, err := NewProducer(cfg); err == nil {
		p.Close()
		t.Error("NewProducer accepted a CA file that does not exist")
	}
}

// TestKafkaRefusesABrokerOfAnotherAuthority: the dial verifies the broker, so a skipped verification on this
// path could not pass the handshake test above unnoticed.
func TestKafkaRefusesABrokerOfAnotherAuthority(t *testing.T) {
	addr, handshakes := tlstest.NewCA(t).HandshakePeer(t)
	p, err := NewProducer(config.Kafka{Brokers: []string{addr}, Timeout: time.Second, TLSEnabled: true, TLSCAFile: tlstest.NewCA(t).CAFile})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.Ping(ctx)
	select {
	case <-handshakes:
		t.Fatal("a broker signed by another authority completed a handshake")
	default:
	}
}
