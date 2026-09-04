// Package status is the shared runtime-health surface of a connector (§6.13/§6.15): the per-bind
// link_status and in_flight the connector pool publishes to Redis (step-128b), and the connector-wide
// breaker aggregate. The Admin API reads it for get-connector-status, keeping link_status and
// breaker_state strictly DISTINCT — a live link can carry an open breaker and vice versa. It also owns
// the reconfigure generation counter the pool polls to pick up a rebind / resize / policy change.
package status

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// Redis keys (Appendix B). All share the {connector_id} hash tag so a connector's runtime keys land on
// one Cluster slot:
//
//	connector:binds:{id}   HASH  field "pod_id:bind_index" -> JSON {link_status,in_flight,ts}
//	connectorload:{id}     STRING derived in-flight sum of the live binds, for least_loaded (step-260d)
//	breaker:binds:{id}     HASH  field "pod_id:bind_index" -> "breakerState:heartbeat_ms"  (step-122)
//	breaker:state:{id}     STRING derived breaker aggregate token                          (step-122)
//	connector:cfggen:{id}  STRING monotonically-incremented reconfigure generation         (step-128)

//go:embed publish_bind.lua
var publishBindSrc string

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
// the breaker sub-bind state lives in breaker:binds, never conflated). TS is the publish time (unix ms):
// Redis hashes have no per-field TTL, so a field stale beyond bindTTL — a bind removed by a shrink or a
// crashed pod — is dropped by Read rather than lingering forever on the pod-shared key.
type BindEntry struct {
	LinkStatus string `json:"link_status"`
	InFlight   int    `json:"in_flight"`
	TS         int64  `json:"ts,omitempty"`
}

// Encode serialises a BindEntry for a connector:binds hash field.
func (e BindEntry) Encode() []byte {
	b, _ := json.Marshal(e) //nolint:errchkjson // a fixed small struct never fails to marshal
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

// Reader assembles a connector's runtime status from Redis. Reads are best-effort: a missing key means
// "nothing published yet" (empty binds, closed breaker), not an error. It also carries the pool's write
// side (PublishBind, SignalReconfigure).
type Reader struct {
	rdb     *goredis.Client
	publish *redisstore.Script
}

// NewReader builds a Reader over the shared Redis client.
func NewReader(rdb *goredis.Client) *Reader {
	return &Reader{rdb: rdb, publish: redisstore.NewScript(rdb, publishBindSrc)}
}

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
	nowMs := time.Now().UnixMilli()
	for field, raw := range linkH {
		var e BindEntry
		if json.Unmarshal([]byte(raw), &e) != nil {
			continue
		}
		// Drop a per-field entry that has gone stale (a shrunk bind or a crashed pod): the pod-shared key
		// TTL cannot express per-field expiry. A zero TS (unstamped/legacy) is treated as fresh.
		if e.TS != 0 && nowMs-e.TS > bindTTL.Milliseconds() {
			continue
		}
		b := get(field)
		if e.LinkStatus != "" {
			b.LinkStatus = e.LinkStatus
		}
		b.InFlight = e.InFlight
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

// bindTTL is how long a per-bind status field survives without a refresh, so a dead pod's binds fade
// from the status rather than lingering forever. The pool republishes every status heartbeat.
const bindTTL = 30 * time.Second

// PublishBind writes one bind's link_status + in_flight into connector:binds and derives
// connectorload:{id} from the live entries, in one atomic script (publish_bind.lua): the stale entries
// are swept there, the hash and the gauge get the bind TTL. The pool calls it every status heartbeat;
// the Admin API reads the merged hash, the router reads the derived gauge (LoadReader).
func (r *Reader) PublishBind(ctx context.Context, connectorID uuid.UUID, podID string, bindIndex int, linkStatus string, inFlight int) error {
	now := time.Now().UnixMilli()
	field := podID + ":" + strconv.Itoa(bindIndex)
	val := BindEntry{LinkStatus: linkStatus, InFlight: inFlight, TS: now}.Encode()
	err := r.publish.Run(ctx,
		[]string{BindsKey(connectorID), LoadKey(connectorID)},
		field, string(val), now, bindTTL.Milliseconds(),
	).Err()
	if err != nil {
		return fmt.Errorf("status: publish bind: %w", err)
	}
	return nil
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
