package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// Producer is a durable, idempotent producer. Every write is acks=all and, because franz-go keeps
// idempotent producing enabled by default whenever acks=all, exactly-once-per-partition on the
// broker side: a retried produce cannot duplicate a record. Produce is synchronous — it returns
// only once the record is durably acknowledged, which is the frontier that earns a REST 202 or an
// SMPP submit_sm_resp OK (§6.7 / §7.3). Never acknowledge a client before Produce returns.
type Producer struct {
	cl *kgo.Client
}

// FailClosedProduceTimeout bounds a produce whose failure redelivers records already sent to an SMSC
// (mt.outcome after submit_sm, mt.routed before the mt.inbound commit): long enough for a leader
// election and the ISR wait, under kgo's 60s RebalanceTimeout past which a blocked handler gets the
// member evicted and every partition redelivered.
const FailClosedProduceTimeout = 30 * time.Second

// ProducerOption is a per-binary policy, set in wiring and never by the environment.
type ProducerOption func(*producerPolicy)

type producerPolicy struct{ bound time.Duration }

// ForFailClosedConsumer replaces KAFKA_PRODUCE_TIMEOUT with FailClosedProduceTimeout (step-260e): for a
// consumer that produces before committing its offset, an expired record is a redelivery, and for
// mt.outcome a redelivery is a duplicate SMS (ADR-0012). The environment cannot lower it.
func ForFailClosedConsumer() ProducerOption {
	return func(p *producerPolicy) { p.bound = FailClosedProduceTimeout }
}

// NewProducer connects a Producer to the configured brokers. It does not block on reachability;
// use ReadyCheck for that.
func NewProducer(cfg config.Kafka, opts ...ProducerOption) (*Producer, error) {
	policy := producerPolicy{bound: cfg.ProduceTimeout}
	for _, o := range opts {
		o(&policy)
	}
	kopts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// AllISRAcks is franz-go's default and the precondition for idempotent producing; naming it
		// makes the durability contract explicit and guards against a future edit weakening it.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(16 << 20),
	}, producerOpts(cfg, policy.bound)...)
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce writes one record and blocks until it is durably acknowledged. On a fault the delivery bound
// (KAFKA_PRODUCE_TIMEOUT, or FailClosedProduceTimeout) fails it with kgo.ErrRecordTimeout — but only a
// record never sent, or whose last attempt got a definite retriable reply: a request in flight is
// bounded by kgo's read deadline instead, and a batch answered REQUEST_TIMED_OUT is retried until a
// definite reply, since failing it could duplicate on the broker. The record MUST carry a key on an
// ordered topic (mt.inbound, mt.routed): producing keyless would scatter a message's segments across
// partitions and lose their order (§7.3).
func (p *Producer) Produce(ctx context.Context, rec Record) error {
	kr := &kgo.Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value}
	for _, h := range rec.Headers {
		kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if err := p.cl.ProduceSync(ctx, kr).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce to %s: %w", rec.Topic, err)
	}
	return nil
}

// Ping reports whether the brokers are reachable.
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

// ReadyCheck adapts the producer to a readiness probe: Kafka is a vital dependency of every service
// that accepts messages, since a message cannot be durably accepted when it is gone (plan §1.5).
func (p *Producer) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, p.cl.Ping)
}

// DeliveryTimeout is the bound the client resolved; wiring tests assert the per-binary policy on it.
func (p *Producer) DeliveryTimeout() time.Duration {
	d, _ := p.cl.OptValue(kgo.RecordDeliveryTimeout).(time.Duration)
	return d
}

// Close flushes any buffered records and releases the client. Because Produce is synchronous there
// is nothing buffered in normal operation; Close is still the orderly way to release broker
// connections on shutdown.
func (p *Producer) Close() { p.cl.Close() }
