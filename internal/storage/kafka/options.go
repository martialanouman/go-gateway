package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/config"
)

// A zero-valued field is deliberately left unset here, so franz-go keeps its own default.
// Configuration parsed from the environment never yields a zero — every field has a non-zero default
// and validation refuses out-of-range values — so this only concerns a config.Kafka built as a struct
// literal in code, most often a test. Forwarding the zero would be strictly worse than ignoring it:
// franz-go validates neither FetchMinBytes nor FetchMaxBytes (its bounds table, kgo/config.go:301-386,
// lists every checked limit), so a zero FetchMaxBytes would ask each broker for at most zero bytes per
// fetch, and a zero DialTimeout is net.Dialer{Timeout: 0} — no dial timeout at all (kgo/client.go:510).
// Both are silent stalls, where an unset knob is merely the library default.

// dialOpts are the client options every kgo client in this package shares.
//
// DialTimeout bounds one connection attempt to a broker; franz-go's own default is 10s
// (kgo/config.go:602). Until step-201 KAFKA_TIMEOUT was read and validated but reached no client at
// all, so it governed the readiness probe while every dial behind that probe ignored it. It is a dial
// bound only, never a produce or fetch deadline — those come from the caller's context.
func dialOpts(cfg config.Kafka) []kgo.Opt {
	if cfg.Timeout <= 0 {
		return nil
	}
	return []kgo.Opt{kgo.DialTimeout(cfg.Timeout)}
}

// consumerOpts are the capacity levers shared by every consumer built in this package (step-201, D5).
//
// FetchMinBytes and FetchMaxWait together size a poll: a broker holds a fetch open until it has
// FetchMinBytes to return, or until FetchMaxWait elapses. That pair is therefore the CDR insert-size
// lever (D8) — the ClickHouse batch is exactly what one poll returned, with no client-side buffer
// between them, so poll = insert = commit stays aligned and no offset bookkeeping is needed to know
// what a flush covered. FetchMaxBytes caps one fetch response PER BROKER, so a client can hold up to
// brokers × FetchMaxBytes; franz-go refuses to build a client whose FetchMaxBytes exceeds its
// BrokerMaxReadBytes (100MiB by default, kgo/config.go:331 and :646), and config validation catches
// that before a service boots.
//
// They come from the process-wide KAFKA_ configuration, so every consumer in a service shares them:
// in router-svc, raising FetchMinBytes for the CDR projection also enlarges the MT pipeline's polls,
// and in admin-api-svc it also delays the metrics tail reader's live frames. That is a known and
// accepted limit of the lever — there is deliberately no per-consumer override, which would mean a
// second naming scheme for every consumer in the repository.
func consumerOpts(cfg config.Kafka) []kgo.Opt {
	opts := dialOpts(cfg)
	if cfg.FetchMinBytes > 0 {
		opts = append(opts, kgo.FetchMinBytes(cfg.FetchMinBytes))
	}
	if cfg.FetchMaxWait > 0 {
		opts = append(opts, kgo.FetchMaxWait(cfg.FetchMaxWait))
	}
	if cfg.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(cfg.FetchMaxBytes))
	}
	// The duplication bound of ADR-0012: one poll's records per partition is exactly what a crash
	// between a send and its offset commit can re-submit to the SMSC.
	if cfg.FetchMaxPartitionBytes > 0 {
		opts = append(opts, kgo.FetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes))
	}
	return opts
}
