package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
)

// levers is a config.Kafka whose every capacity value differs from franz-go's own default, so an
// assertion below cannot pass by accident on an unwired option.
func levers() config.Kafka {
	return config.Kafka{
		Brokers:       []string{"localhost:9092"},
		Timeout:       1500 * time.Millisecond, // franz-go dials for 10s by default
		FetchMinBytes: 64 << 10,                // default 1
		FetchMaxWait:  250 * time.Millisecond,  // default 5s
		FetchMaxBytes: 8 << 20,                 // default 50MiB

		// The duplication bound of ADR-0012: one poll's worth of already-sent messages is what a crash
		// can re-submit, and this is the only knob that caps it.
		FetchMaxPartitionBytes: 256 << 10, // default 1MiB

		ProduceTimeout: 1500 * time.Millisecond, // defaults 0 (unbounded) and 10s
	}
}

// assertOpt reads the value franz-go actually resolved for opt. OptValue is the client's own view of
// its configuration, so an option that was never passed reports the library default and the test
// fails — which is what makes these assertions proof of wiring rather than of intent.
func assertOpt(t *testing.T, cl *kgo.Client, opt any, name string, want any) {
	t.Helper()
	if got := cl.OptValue(opt); got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func assertFetchLevers(t *testing.T, cl *kgo.Client) {
	t.Helper()
	cfg := levers()
	assertOpt(t, cl, kgo.FetchMinBytes, "FetchMinBytes", cfg.FetchMinBytes)
	assertOpt(t, cl, kgo.FetchMaxWait, "FetchMaxWait", cfg.FetchMaxWait)
	assertOpt(t, cl, kgo.FetchMaxBytes, "FetchMaxBytes", cfg.FetchMaxBytes)
	assertOpt(t, cl, kgo.FetchMaxPartitionBytes, "FetchMaxPartitionBytes", cfg.FetchMaxPartitionBytes)
}

// TestConsumerAppliesTheFetchLevers is the step-201 D5/D8 contract: the fetch trio is what sizes a
// ClickHouse CDR insert, since the batch is exactly one poll's records. Unwired, the knobs would be
// dead and the batch size unreachable from configuration.
func TestConsumerAppliesTheFetchLevers(t *testing.T) {
	c, err := NewConsumer(levers(), "group", "topic")
	if err != nil {
		t.Fatalf("NewConsumer() = %v, want nil", err)
	}
	defer c.Close()

	assertFetchLevers(t, c.cl)
}

// TestConsumerFromLatestAppliesTheFetchLevers guards the second constructor: both go through
// newConsumer, and a lever wired in only one of them is a lever the connector pool does not get.
func TestConsumerFromLatestAppliesTheFetchLevers(t *testing.T) {
	c, err := NewConsumerFromLatest(levers(), "group", "topic")
	if err != nil {
		t.Fatalf("NewConsumerFromLatest() = %v, want nil", err)
	}
	defer c.Close()

	assertFetchLevers(t, c.cl)
}

// TestTailReaderAppliesTheFetchLevers covers the groupless reader, which builds its own client and
// would otherwise silently keep franz-go's defaults.
func TestTailReaderAppliesTheFetchLevers(t *testing.T) {
	c, err := NewTailReader(levers(), "topic")
	if err != nil {
		t.Fatalf("NewTailReader() = %v, want nil", err)
	}
	defer c.Close()

	assertFetchLevers(t, c.cl)
}

// TestEveryClientAppliesTheDialTimeout is the step-201 correction: KAFKA_TIMEOUT was read and
// validated but reached no client, so every kgo dial used franz-go's 10s regardless of it.
func TestEveryClientAppliesTheDialTimeout(t *testing.T) {
	cfg := levers()

	consumer, err := NewConsumer(cfg, "group", "topic")
	if err != nil {
		t.Fatalf("NewConsumer() = %v, want nil", err)
	}
	defer consumer.Close()

	tail, err := NewTailReader(cfg, "topic")
	if err != nil {
		t.Fatalf("NewTailReader() = %v, want nil", err)
	}
	defer tail.Close()

	producer, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer() = %v, want nil", err)
	}
	defer producer.Close()

	stream, err := NewStreamProducer(cfg)
	if err != nil {
		t.Fatalf("NewStreamProducer() = %v, want nil", err)
	}
	defer stream.Close()

	for name, cl := range map[string]*kgo.Client{
		"consumer":        consumer.cl,
		"tail reader":     tail.cl,
		"producer":        producer.cl,
		"stream producer": stream.cl,
	} {
		assertOpt(t, cl, kgo.DialTimeout, name+" DialTimeout", cfg.Timeout)
	}
}

// TestProducerAppliesTheProduceTimeout: one variable, both options (step-260e).
func TestProducerAppliesTheProduceTimeout(t *testing.T) {
	cfg := levers()
	producer, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer() = %v, want nil", err)
	}
	defer producer.Close()

	assertOpt(t, producer.cl, kgo.RecordDeliveryTimeout, "RecordDeliveryTimeout", cfg.ProduceTimeout)
	assertOpt(t, producer.cl, kgo.ProduceRequestTimeout, "ProduceRequestTimeout", cfg.ProduceTimeout)
}

// TestAnUnsetLeverKeepsTheLibraryDefault pins the zero-value contract of consumerOpts and dialOpts.
//
// It is not a hypothetical: a config.Kafka built as a struct literal — which is how every integration
// test in the repository builds one — leaves the new fields at zero, and forwarding those zeros makes
// franz-go refuse the client ("max fetch wait 0s is less than allowed 10ms") or, worse for the two it
// does not validate, fetch at most zero bytes per broker and dial without any timeout.
func TestAnUnsetLeverKeepsTheLibraryDefault(t *testing.T) {
	bare := config.Kafka{Brokers: []string{"localhost:9092"}}

	c, err := NewConsumer(bare, "group", "topic")
	if err != nil {
		t.Fatalf("NewConsumer() with an unset config = %v, want nil", err)
	}
	defer c.Close()

	assertOpt(t, c.cl, kgo.FetchMinBytes, "FetchMinBytes", int32(1))
	assertOpt(t, c.cl, kgo.FetchMaxWait, "FetchMaxWait", 5*time.Second)
	assertOpt(t, c.cl, kgo.FetchMaxBytes, "FetchMaxBytes", int32(50<<20))
	assertOpt(t, c.cl, kgo.DialTimeout, "DialTimeout", 10*time.Second)

	p, err := NewProducer(bare)
	if err != nil {
		t.Fatalf("NewProducer() with an unset config = %v, want nil", err)
	}
	defer p.Close()

	assertOpt(t, p.cl, kgo.RecordDeliveryTimeout, "RecordDeliveryTimeout", time.Duration(0))
	assertOpt(t, p.cl, kgo.ProduceRequestTimeout, "ProduceRequestTimeout", 10*time.Second)
}

// TestConsumerRefusesAFetchMaxBytesAboveTheBrokerReadCeiling proves the wiring a second way, and
// independently of OptValue: franz-go validates FetchMaxBytes against BrokerMaxReadBytes (100MiB,
// kgo/config.go:331 and :646) and refuses to build the client. A value the client never received
// could not be refused.
func TestConsumerRefusesAFetchMaxBytesAboveTheBrokerReadCeiling(t *testing.T) {
	cfg := levers()
	cfg.FetchMaxBytes = (100 << 20) + 1

	c, err := NewConsumer(cfg, "group", "topic")
	if err == nil {
		c.Close()
		t.Fatalf("NewConsumer() with FetchMaxBytes above the broker read ceiling = nil, want an error")
	}
	if !strings.Contains(err.Error(), "max fetch bytes") {
		t.Errorf("NewConsumer() error = %v, want it to name max fetch bytes", err)
	}
}

// TestConsumerRefusesAFetchMaxWaitBelowTheFranzGoFloor is the same independent proof for the wait:
// franz-go stores it as int32 milliseconds and refuses anything under 10ms (kgo/config.go:373).
func TestConsumerRefusesAFetchMaxWaitBelowTheFranzGoFloor(t *testing.T) {
	cfg := levers()
	cfg.FetchMaxWait = time.Millisecond

	c, err := NewConsumer(cfg, "group", "topic")
	if err == nil {
		c.Close()
		t.Fatalf("NewConsumer() with a sub-10ms FetchMaxWait = nil, want an error")
	}
	if !strings.Contains(err.Error(), "max fetch wait") {
		t.Errorf("NewConsumer() error = %v, want it to name max fetch wait", err)
	}
}

// TestTheDurabilityInvariantsAreNotConfigurable guards step-201 D6. These are contract frontiers, not
// tuning: acks=all and kgo's idempotent producer are what make a durable ACK durable, and disabled
// autocommit is what makes the consumer at-least-once. A capacity PR is exactly the kind of change
// that would weaken one of them by accident.
func TestTheDurabilityInvariantsAreNotConfigurable(t *testing.T) {
	cfg := levers()

	producer, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer() = %v, want nil", err)
	}
	defer producer.Close()

	assertOpt(t, producer.cl, kgo.RequiredAcks, "producer RequiredAcks", kgo.AllISRAcks())
	assertOpt(t, producer.cl, kgo.DisableIdempotentWrite, "producer DisableIdempotentWrite", false)

	consumer, err := NewConsumer(cfg, "group", "topic")
	if err != nil {
		t.Fatalf("NewConsumer() = %v, want nil", err)
	}
	defer consumer.Close()

	assertOpt(t, consumer.cl, kgo.DisableAutoCommit, "consumer DisableAutoCommit", true)
}
