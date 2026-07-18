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
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

//go:embed lua/bind.lua
var bindScriptSrc string

//go:embed lua/unbind.lua
var unbindScriptSrc string

//go:embed lua/touch.lua
var touchScriptSrc string

//go:embed lua/lookup.lua
var lookupScriptSrc string

// DefaultSessionTTL is the lifetime of a session token without a refresh. A bind must be kept alive by
// Touch (driven by enquire_link); once the TTL lapses the token is swept and the slot is freed.
const DefaultSessionTTL = 60 * time.Second

// memberSep joins pod_id and bind_id into a sorted-set member. pod names and bind ids (UUIDs) never
// contain it.
const memberSep = ":"

// Bind identifies one live session: an account, the pod that owns the connection, and the bind id.
type Bind struct {
	AccountID string
	PodID     string
	BindID    string
}

func (b Bind) member() string { return b.PodID + memberSep + b.BindID }

// Registry is the Redis-backed session registry. Construct it with NewRegistry.
type Registry struct {
	rdb *redis.Client
	ttl time.Duration
	now func() time.Time

	bind   *redis.Script
	unbind *redis.Script
	touch  *redis.Script
	lookup *redis.Script
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
		touch:  redis.NewScript(touchScriptSrc),
		lookup: redis.NewScript(lookupScriptSrc),
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

// keyTTLSeconds is the whole-key EXPIRE: the newest member's lifetime plus a second, so an idle
// account key eventually vanishes without ever outliving a live member.
func (r *Registry) keyTTLSeconds() int64 { return int64(r.ttl.Seconds()) + 1 }

// Bind registers b against the account's max_sessions ceiling and returns the account's live session
// count. It returns ErrMaxSessionsExceeded when the quota is reached; a rebind of an already-registered
// session refreshes its TTL and never counts twice.
func (r *Registry) Bind(ctx context.Context, b Bind, maxSessions int) (int, error) {
	now := r.now()
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

// Unbind removes b's session token and reports whether a session was present.
func (r *Registry) Unbind(ctx context.Context, b Bind) (bool, error) {
	removed, err := r.unbind.Run(ctx, r.rdb, []string{key(b.AccountID)}, b.member()).Int64()
	if err != nil {
		return false, fmt.Errorf("session: unbind %s: %w", b.AccountID, err)
	}
	return removed > 0, nil
}

// Touch refreshes b's TTL (called on enquire_link) and reports whether the session was still present.
// A session that already lapsed is not resurrected.
func (r *Registry) Touch(ctx context.Context, b Bind) (bool, error) {
	now := r.now()
	refreshed, err := r.touch.Run(ctx, r.rdb, []string{key(b.AccountID)},
		b.member(), now.Unix(), now.Add(r.ttl).Unix(), r.keyTTLSeconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("session: touch %s: %w", b.AccountID, err)
	}
	return refreshed > 0, nil
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
	return binds, nil
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
