// Package status is the shared runtime-health surface of a connector (§6.13/§6.15): the per-bind
// link_status and in_flight the connector pool publishes to Redis (step-128b), and the connector-wide
// breaker aggregate. The Admin API reads it for get-connector-status, keeping link_status and
// breaker_state strictly DISTINCT — a live link can carry an open breaker and vice versa. It also owns
// the reconfigure generation counter the pool polls to pick up a rebind / resize / policy change.
package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
)

// Redis keys (Appendix B). All share the {connector_id} hash tag so a connector's runtime keys land on
// one Cluster slot:
//
//	connector:binds:{id}   HASH  field "pod_id:bind_index" -> JSON {link_status,in_flight}
//	breaker:binds:{id}     HASH  field "pod_id:bind_index" -> "breakerState:heartbeat_ms"  (step-122)
//	breaker:state:{id}     STRING derived breaker aggregate token                          (step-122)
//	connector:cfggen:{id}  STRING monotonically-incremented reconfigure generation         (step-128)

// BindsKey is the per-bind link-status HASH a connector's pool publishes and the Admin API reads.
func BindsKey(connectorID uuid.UUID) string { return "connector:binds:{" + connectorID.String() + "}" }
func genKey(connectorID uuid.UUID) string   { return "connector:cfggen:{" + connectorID.String() + "}" }
func breakerBinds(connectorID uuid.UUID) string {
	return "breaker:binds:{" + connectorID.String() + "}"
}
func breakerState(connectorID uuid.UUID) string {
	return "breaker:state:{" + connectorID.String() + "}"
}

// Link statuses reported per bind. reconnecting is emitted while the reconnect loop is backing off /
// re-dialling; down is a dropped or parked link.
const (
	LinkUp           = "up"
	LinkReconnecting = "reconnecting"
	LinkDown         = "down"
)

// BindEntry is the per-bind runtime value the pool publishes into connector:binds (link + load only;
// the breaker sub-bind state lives in breaker:binds, never conflated).
type BindEntry struct {
	LinkStatus string `json:"link_status"`
	InFlight   int    `json:"in_flight"`
}

// Encode serialises a BindEntry for a connector:binds hash field.
func (e BindEntry) Encode() []byte {
	b, _ := json.Marshal(e) //nolint:errchkjson // a fixed two-field struct never fails to marshal
	return b
}

// Bind is one sub-bind's assembled runtime health, merging its link entry with its breaker state.
type Bind struct {
	PodID        string
	BindIndex    int
	LinkStatus   string
	BreakerState string
	InFlight     int
}

// Connector is the assembled ConnectorStatus: the connector-wide breaker aggregate plus every live
// sub-bind across all pods.
type Connector struct {
	ConnectorID  uuid.UUID
	BreakerState string
	Binds        []Bind
}

// Reader assembles a connector's runtime status from Redis. It is read-only and best-effort: a missing
// key means "nothing published yet" (empty binds, closed breaker), not an error.
type Reader struct{ rdb *goredis.Client }

// NewReader builds a Reader over the shared Redis client.
func NewReader(rdb *goredis.Client) *Reader { return &Reader{rdb: rdb} }

// Read returns the connector's live status: the aggregate breaker_state and one Bind per (pod_id,
// bind_index) seen in either the link hash or the breaker hash. link_status and breaker_state stay
// distinct per bind.
func (r *Reader) Read(ctx context.Context, connectorID uuid.UUID) (Connector, error) {
	linkH, err := r.rdb.HGetAll(ctx, BindsKey(connectorID)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return Connector{}, fmt.Errorf("status: read link binds: %w", err)
	}
	brkH, err := r.rdb.HGetAll(ctx, breakerBinds(connectorID)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return Connector{}, fmt.Errorf("status: read breaker binds: %w", err)
	}
	agg, err := r.rdb.Get(ctx, breakerState(connectorID)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return Connector{}, fmt.Errorf("status: read breaker aggregate: %w", err)
	}
	aggState := breaker.Closed.String()
	if st, ok := breaker.ParseState(agg); ok {
		aggState = st.String()
	}

	// One Bind per field seen in either hash.
	byField := make(map[string]*Bind)
	get := func(field string) *Bind {
		if b, ok := byField[field]; ok {
			return b
		}
		pod, idx := splitField(field)
		b := &Bind{PodID: pod, BindIndex: idx, LinkStatus: LinkDown, BreakerState: breaker.Closed.String()}
		byField[field] = b
		return b
	}
	for field, raw := range linkH {
		b := get(field)
		var e BindEntry
		if json.Unmarshal([]byte(raw), &e) == nil {
			if e.LinkStatus != "" {
				b.LinkStatus = e.LinkStatus
			}
			b.InFlight = e.InFlight
		}
	}
	for field, raw := range brkH {
		b := get(field)
		if tok, _, ok := strings.Cut(raw, ":"); ok {
			if st, ok := breaker.ParseState(tok); ok {
				b.BreakerState = st.String()
			}
		}
	}

	binds := make([]Bind, 0, len(byField))
	for _, b := range byField {
		binds = append(binds, *b)
	}
	return Connector{ConnectorID: connectorID, BreakerState: aggState, Binds: binds}, nil
}

// SignalReconfigure increments the connector's reconfigure generation, signalling every pool pod to
// re-read its config and re-dial (rebind / resize / policy change). It is the only write side of the
// control plane.
func (r *Reader) SignalReconfigure(ctx context.Context, connectorID uuid.UUID) error {
	if err := r.rdb.Incr(ctx, genKey(connectorID)).Err(); err != nil {
		return fmt.Errorf("status: bump cfggen: %w", err)
	}
	return nil
}

// Gen reads the current reconfigure generation (0 when unset). The pool polls it to detect a change.
func (r *Reader) Gen(ctx context.Context, connectorID uuid.UUID) (int64, error) {
	n, err := r.rdb.Get(ctx, genKey(connectorID)).Int64()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("status: read cfggen: %w", err)
	}
	return n, nil
}

// splitField parses a "pod_id:bind_index" hash field. A pod id never contains ':' (k8s hostnames and the
// uuid fallback do not), so the last ':' separates the bind index; a malformed field yields index 0.
func splitField(field string) (podID string, bindIndex int) {
	i := strings.LastIndex(field, ":")
	if i < 0 {
		return field, 0
	}
	idx, _ := strconv.Atoi(field[i+1:])
	return field[:i], idx
}
