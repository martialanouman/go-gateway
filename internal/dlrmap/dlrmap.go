// Package dlrmap is the DLR correlation store: when the connector pool submits a message and the
// SMSC returns an assigned smsc_msg_id, it remembers smsc_msg_id -> {message_id, trace_id} in Redis
// so a later deliver_sm (a delivery receipt) can be correlated back to the original message
// (step-044 reads it). The entry is written with a TTL derived from the message's validity_period,
// past which no SMSC will still emit a receipt.
package dlrmap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/pipeline"
)

// The TTL bounds. maxTTL mirrors cancel.flagTTL: 72h covers the maximum SMS validity, past which no
// SMSC emits a receipt, so a longer mapping would only leak keys. minTTL floors the derived value so
// a very short validity_period cannot orphan a receipt that arrives slightly late. ttlMargin is added
// to a derived validity to cover the gap between expiry and the final receipt. The guiding rule: when
// in doubt, expire too late, never too early — an over-long TTL is harmless (the key just lingers), a
// too-short one loses the correlation.
const (
	maxTTL    = 72 * time.Hour
	minTTL    = 1 * time.Hour
	ttlMargin = 1 * time.Hour
)

// RedisMap is the Redis-backed DLR correlation store. Each entry is a single key, so Cluster never
// sees a multi-key op.
type RedisMap struct {
	rdb *redis.Client
}

// NewRedisMap builds a DLR map store over rdb.
func NewRedisMap(rdb *redis.Client) *RedisMap {
	return &RedisMap{rdb: rdb}
}

// mapping is the stored JSON value: the full CDR projection a later delivery receipt needs to write
// its versioned row WITHOUT re-reading ClickHouse. The CDR is a ReplacingMergeTree read with FINAL, so
// a delivered/failed/expired row supersedes the enroute row WHOLE — it must therefore carry every
// column that matters (the ORDER BY key customer_id/account_id/submitted_at/message_id AND the
// descriptive columns source/dest/connector/route/segment/encoding), else the delivered snapshot would
// blank them. It NEVER carries the message body (invariant a): only descriptive metadata.
type mapping struct {
	MessageID    string    `json:"message_id"`
	TraceID      string    `json:"trace_id"`
	AccountID    string    `json:"account_id"`
	CustomerID   string    `json:"customer_id"`
	SourceAddr   string    `json:"source_addr"`
	DestAddr     string    `json:"dest_addr"`
	ConnectorID  string    `json:"connector_id"`
	RouteID      *string   `json:"route_id,omitempty"`
	SegmentCount int       `json:"segment_count"`
	Encoding     string    `json:"encoding"`
	SubmittedAt  time.Time `json:"submitted_at"`
}

// Mapping is the resolved DLR correlation the return-path router reads back (step-044): the full CDR
// projection of the submitted message, so it can write a collapsing delivered/failed/expired row and
// compute latency without a ClickHouse read. It never carries the message body.
type Mapping struct {
	MessageID    uuid.UUID
	TraceID      uuid.UUID
	AccountID    uuid.UUID
	CustomerID   uuid.UUID
	SourceAddr   string
	DestAddr     string
	ConnectorID  uuid.UUID
	RouteID      *uuid.UUID
	SegmentCount int
	Encoding     string
	SubmittedAt  time.Time
}

// key scopes an entry by (connector_id, smsc_msg_id): connector_id disambiguates the same
// smsc_msg_id reused by two different SMSCs. The whole composite is the Redis Cluster hash tag, so
// each mapping distributes across slots (no per-connector hot slot at target throughput) while the
// single-key op stays within one slot. step-044 must rebuild this exact key to look a receipt up.
func key(connectorID uuid.UUID, smscMsgID string) string {
	return "dlrmap:{" + connectorID.String() + ":" + smscMsgID + "}"
}

// Put remembers smsc_msg_id -> the submitted message's CDR projection, with a TTL derived from its
// validity_period. It is called on a successful submit; the caller treats a failure as best-effort
// (the message is already enroute). It takes the routed envelope because that is what the caller holds
// and it carries every projected field; the message body it also carries is deliberately ignored (it
// is never read here, so invariant (a) holds — nothing but metadata reaches Redis).
func (m *RedisMap) Put(ctx context.Context, smscMsgID string, r pipeline.RoutedMT) error {
	var routeID *string
	if r.RouteID != nil {
		s := r.RouteID.String()
		routeID = &s
	}
	value, err := json.Marshal(mapping{
		MessageID:    r.MessageID.String(),
		TraceID:      r.TraceID.String(),
		AccountID:    r.AccountID.String(),
		CustomerID:   r.CustomerID.String(),
		SourceAddr:   r.From,
		DestAddr:     r.To,
		ConnectorID:  r.ConnectorID.String(),
		RouteID:      routeID,
		SegmentCount: r.SegmentCount,
		Encoding:     r.Encoding,
		SubmittedAt:  r.SubmittedAt,
	})
	if err != nil {
		return fmt.Errorf("dlrmap: marshal %s: %w", r.MessageID, err)
	}
	if err := m.rdb.Set(ctx, key(r.ConnectorID, smscMsgID), value, ttlForValidity(r.ValidityPeriod)).Err(); err != nil {
		return fmt.Errorf("dlrmap: put %s/%s: %w", r.ConnectorID, smscMsgID, err)
	}
	return nil
}

// Get resolves the mapping a receipt references. found is false (with a nil error) when no mapping is
// stored — an expired or unknown smsc_msg_id, which the caller counts and logs rather than failing.
// A parse error on a stored value is returned as an error (a corrupt entry, not a normal miss).
func (m *RedisMap) Get(ctx context.Context, connectorID uuid.UUID, smscMsgID string) (Mapping, bool, error) {
	raw, err := m.rdb.Get(ctx, key(connectorID, smscMsgID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Mapping{}, false, nil
	}
	if err != nil {
		return Mapping{}, false, fmt.Errorf("dlrmap: get %s/%s: %w", connectorID, smscMsgID, err)
	}
	var w mapping
	if err := json.Unmarshal(raw, &w); err != nil {
		return Mapping{}, false, fmt.Errorf("dlrmap: unmarshal %s/%s: %w", connectorID, smscMsgID, err)
	}
	out, err := w.resolve()
	if err != nil {
		return Mapping{}, false, fmt.Errorf("dlrmap: parse %s/%s: %w", connectorID, smscMsgID, err)
	}
	return out, true, nil
}

// resolve parses the stored string ids into a typed Mapping.
func (w mapping) resolve() (Mapping, error) {
	messageID, err := uuid.Parse(w.MessageID)
	if err != nil {
		return Mapping{}, fmt.Errorf("message_id: %w", err)
	}
	traceID, err := uuid.Parse(w.TraceID)
	if err != nil {
		return Mapping{}, fmt.Errorf("trace_id: %w", err)
	}
	accountID, err := uuid.Parse(w.AccountID)
	if err != nil {
		return Mapping{}, fmt.Errorf("account_id: %w", err)
	}
	customerID, err := uuid.Parse(w.CustomerID)
	if err != nil {
		return Mapping{}, fmt.Errorf("customer_id: %w", err)
	}
	connectorID, err := uuid.Parse(w.ConnectorID)
	if err != nil {
		return Mapping{}, fmt.Errorf("connector_id: %w", err)
	}
	var routeID *uuid.UUID
	if w.RouteID != nil {
		id, err := uuid.Parse(*w.RouteID)
		if err != nil {
			return Mapping{}, fmt.Errorf("route_id: %w", err)
		}
		routeID = &id
	}
	return Mapping{
		MessageID:    messageID,
		TraceID:      traceID,
		AccountID:    accountID,
		CustomerID:   customerID,
		SourceAddr:   w.SourceAddr,
		DestAddr:     w.DestAddr,
		ConnectorID:  connectorID,
		RouteID:      routeID,
		SegmentCount: w.SegmentCount,
		Encoding:     w.Encoding,
		SubmittedAt:  w.SubmittedAt,
	}, nil
}

// ttlForValidity derives an entry's TTL from the SMPP validity_period. A parsable relative period is
// used (plus a margin, clamped to [minTTL, maxTTL]); everything else — nil, empty, an absolute period,
// or an unparsable value — falls back to maxTTL. Absolute periods are deliberately not parsed: they
// are rare on MT submits and would drag in SMSC-vs-local clock skew, and the fail-long fallback is
// free here (an over-long TTL is harmless).
func ttlForValidity(validityPeriod *string) time.Duration {
	if validityPeriod == nil {
		return maxTTL
	}
	d, ok := parseRelativeValidity(*validityPeriod)
	if !ok {
		return maxTTL
	}
	return clamp(d+ttlMargin, minTTL, maxTTL)
}

// parseRelativeValidity parses a relative SMPP v3.4 validity_period ("YYMMDDhhmmsstnnp" with a final
// 'R') into a duration. Years and months use nominal 365-day / 30-day lengths: any value large enough
// for that approximation to matter is already past maxTTL and clamped away, so only the exact
// day/hour/minute/second components below the cap ever survive. A non-relative, malformed, or
// non-positive value reports ok=false.
func parseRelativeValidity(s string) (time.Duration, bool) {
	if len(s) != 16 || s[15] != 'R' {
		return 0, false
	}
	// Fields at fixed positions: YY MM DD hh mm ss (the tenths and time-zone digits are ignored for a
	// relative period, where they are zero).
	yy, ok1 := twoDigits(s, 0)
	mm, ok2 := twoDigits(s, 2)
	dd, ok3 := twoDigits(s, 4)
	hh, ok4 := twoDigits(s, 6)
	mi, ok5 := twoDigits(s, 8)
	ss, ok6 := twoDigits(s, 10)
	allNumeric := ok1 && ok2 && ok3 && ok4 && ok5 && ok6
	if !allNumeric {
		return 0, false
	}
	d := time.Duration(yy)*365*24*time.Hour +
		time.Duration(mm)*30*24*time.Hour +
		time.Duration(dd)*24*time.Hour +
		time.Duration(hh)*time.Hour +
		time.Duration(mi)*time.Minute +
		time.Duration(ss)*time.Second
	if d <= 0 {
		return 0, false
	}
	return d, true
}

// twoDigits reads a two-digit decimal field at position i, reporting ok=false for a non-digit.
func twoDigits(s string, i int) (int, bool) {
	a, b := s[i], s[i+1]
	if a < '0' || a > '9' || b < '0' || b > '9' {
		return 0, false
	}
	return int(a-'0')*10 + int(b-'0'), true
}

// clamp bounds d to [lo, hi].
func clamp(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}
