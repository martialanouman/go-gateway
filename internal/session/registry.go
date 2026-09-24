// Package session is the authoritative registry of live SMPP ESME binds. It keeps, per account, the
// set of active {pod_id, bind_id} sessions in Redis and enforces the account's max_sessions quota at
// bind time — the socle of invariant (d): a bind beyond max_sessions is refused, and an unbind or a
// lapsed enquire_link (TTL expiry) frees the slot.
//
// The quota check is a single atomic Lua script (never a read-modify-write from Go), so concurrent
// binds across pods can never overshoot the ceiling. The registry knows nothing of PostgreSQL: the
// caller (step-024) reads max_sessions from control_plane.smpp_accounts and passes it in.
package session

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

//go:embed lua/bind.lua
var bindScriptSrc string

//go:embed lua/unbind.lua
var unbindScriptSrc string

//go:embed lua/lookup.lua
var lookupScriptSrc string

// DefaultSessionTTL is the lifetime of a session token without a refresh, and of the owning pod's
// published address with it. A bind is kept alive by re-Binding it (the listener's refreshLoop), which
// pushes the expiry forward without counting twice against the quota. Once the TTL lapses the token is
// swept, the slot is freed, and the pod's address goes with it.
const DefaultSessionTTL = 60 * time.Second

// memberSep joins pod_id and bind_id into a sorted-set member. pod names and bind ids (UUIDs) never
// contain it.
const memberSep = ":"

// Bind identifies one live session: an account, the pod that owns the connection, and the bind id.
//
// Addr is that pod's dialable gRPC address, published by the pod itself at bind time and returned by
// Lookup so the return path dials it without resolving anything (step-302). It is deliberately NOT
// part of member(): the address is where the pod is, the pod_id is who it is, and only the second
// belongs to a session's identity.
type Bind struct {
	AccountID string
	PodID     string
	BindID    string
	Addr      string
}

func (b Bind) member() string { return b.PodID + memberSep + b.BindID }

// Registry is the Redis-backed session registry. Construct it with NewRegistry.
type Registry struct {
	rdb *redis.Client
	ttl time.Duration
	now func() time.Time

	bind   *redis.Script
	unbind *redis.Script
	lookup *redis.Script

	// published only suppresses writes, it is never read as the truth: a missing entry — a fresh
	// replica, a pruned pod — costs one SET more, never an address less.
	pubMu     sync.Mutex
	published map[string]publication
}

type publication struct {
	addr string
	at   time.Time
}

// Option configures a Registry.
type Option func(*Registry)

// WithSessionTTL sets the session lifetime (default DefaultSessionTTL). Non-positive values are ignored.
func WithSessionTTL(d time.Duration) Option {
	return func(r *Registry) {
		if d > 0 {
			r.ttl = d
		}
	}
}

// WithClock overrides the clock, so tests can advance time to exercise TTL expiry without sleeping.
// A nil clock is ignored.
func WithClock(now func() time.Time) Option {
	return func(r *Registry) {
		if now != nil {
			r.now = now
		}
	}
}

// NewRegistry returns a registry backed by rdb. The Lua scripts are prepared once and run via
// EVALSHA (go-redis falls back to EVAL on first use).
func NewRegistry(rdb *redis.Client, opts ...Option) *Registry {
	r := &Registry{
		rdb:    rdb,
		ttl:    DefaultSessionTTL,
		now:    time.Now,
		bind:   redis.NewScript(bindScriptSrc),
		unbind: redis.NewScript(unbindScriptSrc),
		lookup: redis.NewScript(lookupScriptSrc),

		published: make(map[string]publication),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// key is account-scoped so the whole quota lives under one Redis key: a single EVALSHA does the
// count-check-insert atomically. The braces make account_id a Redis Cluster hash tag, keeping every
// operation for an account on one slot.
func key(accountID string) string { return "sess:{" + accountID + "}" }

// podKey holds one pod's dialable gRPC address (step-302). It is written by its own command rather than
// as a second KEYS entry of bind.lua, because bind.lua carries invariant (d) and an address needs none
// of its atomicity — and because the two keys hash-tag to different slots (see podAddrs).
func podKey(podID string) string { return "sess:pod:{" + podID + "}" }

// keyTTLSeconds is the whole-key EXPIRE: the newest member's lifetime plus a second, so an idle
// account key eventually vanishes without ever outliving a live member.
func (r *Registry) keyTTLSeconds() int64 { return int64(r.ttl.Seconds()) + 1 }

// Bind registers b against the account's max_sessions ceiling and returns the account's live session
// count. It returns ErrMaxSessionsExceeded when the quota is reached; a rebind of an already-registered
// session refreshes its TTL and never counts twice.
func (r *Registry) Bind(ctx context.Context, b Bind, maxSessions int) (int, error) {
	now := r.now()
	// Published BEFORE the quota script, so a failure here refuses the bind without having consumed a
	// token. The reverse order would leave the caller told its bind failed while the slot was taken.
	// Publishing for a bind the quota then refuses is harmless: a pod's address is true independently
	// of any one bind, and it expires on its own.
	if b.Addr != "" {
		if err := r.publishAddr(ctx, b, now); err != nil {
			return 0, err
		}
	}
	res, err := r.bind.Run(ctx, r.rdb, []string{key(b.AccountID)},
		b.member(), maxSessions, now.Unix(), now.Add(r.ttl).Unix(), r.keyTTLSeconds()).Result()
	if err != nil {
		return 0, fmt.Errorf("session: bind %s: %w", b.AccountID, err)
	}
	accepted, active, err := parsePair(res)
	if err != nil {
		return 0, fmt.Errorf("session: bind %s: %w", b.AccountID, err)
	}
	if accepted == 0 {
		return active, fmt.Errorf("session: bind %s: %w", b.AccountID, errs.ErrMaxSessionsExceeded)
	}
	return active, nil
}

// publishAddr skips a SET this replica landed less than a quarter TTL ago: every live bind re-Binds
// every half TTL, so the key is never older than a quarter plus a half TTL (plus the call) when a Bind
// rewrites it, well inside its TTL + 1 s. At half a TTL that margin would be a single second. Replicas
// keep separate memories, and the bound holds for each.
func (r *Registry) publishAddr(ctx context.Context, b Bind, now time.Time) error {
	r.pubMu.Lock()
	last, ok := r.published[b.PodID]
	r.pubMu.Unlock()
	if ok && last.addr == b.Addr && now.Sub(last.at) < r.ttl/4 {
		return nil
	}
	if err := r.rdb.Set(ctx, podKey(b.PodID), b.Addr, r.ttl+time.Second).Err(); err != nil {
		return fmt.Errorf("session: publish address of pod %s: %w", b.PodID, err)
	}
	r.pubMu.Lock()
	defer r.pubMu.Unlock()
	for pod, p := range r.published {
		if now.Sub(p.at) > r.ttl {
			delete(r.published, pod)
		}
	}
	r.published[b.PodID] = publication{addr: b.Addr, at: now}
	return nil
}

// Unbind removes b's session token and reports whether a session was present.
func (r *Registry) Unbind(ctx context.Context, b Bind) (bool, error) {
	removed, err := r.unbind.Run(ctx, r.rdb, []string{key(b.AccountID)}, b.member()).Int64()
	if err != nil {
		return false, fmt.Errorf("session: unbind %s: %w", b.AccountID, err)
	}
	return removed > 0, nil
}

// Lookup returns the account's live sessions, having swept any whose TTL has lapsed.
func (r *Registry) Lookup(ctx context.Context, accountID string) ([]Bind, error) {
	members, err := r.lookup.Run(ctx, r.rdb, []string{key(accountID)}, r.now().Unix()).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("session: lookup %s: %w", accountID, err)
	}
	binds := make([]Bind, 0, len(members))
	for _, m := range members {
		pod, bind, ok := strings.Cut(m, memberSep)
		if !ok {
			return nil, fmt.Errorf("session: lookup %s: malformed member %q", accountID, m)
		}
		binds = append(binds, Bind{AccountID: accountID, PodID: pod, BindID: bind})
	}
	addrs, err := r.podAddrs(ctx, binds)
	if err != nil {
		return nil, fmt.Errorf("session: lookup %s: %w", accountID, err)
	}
	for i := range binds {
		binds[i].Addr = addrs[binds[i].PodID]
	}
	return binds, nil
}

// podAddrs reads the dial address of each DISTINCT pod owning one of binds, in a single pipeline.
// An account holds at most max_sessions binds, so this is a handful of GETs, never a scan.
//
// A pod whose address has lapsed yields "" rather than an error: the return path reads that as a bind
// to skip and falls back to the webhook, which is what it already does for a pod that has gone. Any
// OTHER error is returned, and that distinction is the whole point — a partial Redis failure must not
// be indistinguable from the intended degradation, or the return path goes quiet again with nothing
// naming the cause.
//
// Hence the per-command inspection rather than the pipeline's aggregate error: Exec returns the FIRST
// command error (go-redis cmdsFirstErr), so one lapsed pod's redis.Nil would mask a transport error on
// the next pod's GET, and that pod would be read as simply having no address.
//
// A pipeline and not MGET because podKey hash-tags on pod_id, so the keys span slots and a multi-slot
// MGET is refused under Redis Cluster. Note this buys nothing TODAY: r.rdb is a *redis.Client, which
// speaks to one node, and a pipeline would earn its per-slot routing only under a *redis.ClusterClient.
// It is the form that stays correct if this repo's hash tags ever mean what they promise.
func (r *Registry) podAddrs(ctx context.Context, binds []Bind) (map[string]string, error) {
	cmds := make(map[string]*redis.StringCmd, len(binds))
	pipe := r.rdb.Pipeline()
	for _, b := range binds {
		if _, seen := cmds[b.PodID]; seen {
			continue
		}
		cmds[b.PodID] = pipe.Get(ctx, podKey(b.PodID))
	}
	// The aggregate error is ignored on purpose: go-redis sets it on every command, and the loop below
	// reads each one — including the redis.Nil that simply means "this pod published no address".
	_, _ = pipe.Exec(ctx)
	out := make(map[string]string, len(cmds))
	for pod, cmd := range cmds {
		switch v, err := cmd.Result(); {
		case errors.Is(err, redis.Nil): // the pod published no address, or it lapsed
		case err != nil:
			return nil, fmt.Errorf("read address of pod %s: %w", pod, err)
		default:
			out[pod] = v
		}
	}
	return out, nil
}

// parsePair decodes the {accepted, active} reply of bind.lua.
func parsePair(v any) (accepted, active int, err error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != 2 {
		return 0, 0, fmt.Errorf("unexpected script reply %T", v)
	}
	a, ok1 := arr[0].(int64)
	c, ok2 := arr[1].(int64)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("unexpected script reply element types %T,%T", arr[0], arr[1])
	}
	return int(a), int(c), nil
}
