// Package idempotency makes the public POST /messages replay-safe. It keeps, per account and
// client-chosen Idempotency-Key, the original 202 result in Redis for a 24 h window, so a retry of the
// same request returns the original result and publishes the message only once.
//
// The mechanism is two-phase, which preserves the surface's core invariant — a 202 is earned only once
// the message is durably published (§7.3). Reserve claims the slot atomically (state "pending"); the
// winning caller publishes to mt.inbound and then Finalize flips it to "done"; a publish failure calls
// Release so a retry can proceed. A concurrent submit of the same key is told "pending" and Awaits the
// flip to "done" rather than being handed a result for a message that might never be published. The
// reserve-or-read is a single Lua script, so exactly one caller ever wins the slot (never a
// read-modify-write from Go — the store's golden rule).
//
// The entry holds only a SHA-256 of the request body and the id-only 202 response — never the message
// body — so invariant (a) holds: no cleartext body is ever persisted here.
package idempotency

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/reserve.lua
var reserveScriptSrc string

//go:embed lua/finalize.lua
var finalizeScriptSrc string

// DefaultTTL is the idempotency window: a repeat within 24 h replays the original result.
const DefaultTTL = 24 * time.Hour

// defaultPollInterval is how often Await re-reads a pending entry while waiting for the winner to
// confirm its publish. The window between reserve and finalize is a single durable produce (ms), so a
// short tick keeps the wait transparent without hammering Redis.
const defaultPollInterval = 20 * time.Millisecond

// ErrAwaitTimeout is returned by Await when a pending entry is not confirmed within the deadline. The
// REST caller maps it to a retriable 503: the winner is taking unusually long, so retry rather than
// block forever.
var ErrAwaitTimeout = errors.New("idempotency: await timed out")

// Outcome is the verdict of Reserve.
type Outcome int

const (
	// Reserved means the caller won the slot and must publish, then Finalize (or Release on failure).
	Reserved Outcome = iota
	// Replay means the original result is confirmed done and is safe to return without publishing.
	Replay
	// Pending means a concurrent caller holds the slot but has not confirmed its publish; Await the result.
	Pending
	// Conflict means the key is in use with a different body (→ 409 idempotency_conflict).
	Conflict
)

// Result is what Reserve returns. Response carries the stored 202 body for Replay and Pending; it is
// nil for Reserved and Conflict.
type Result struct {
	Outcome  Outcome
	Response []byte
}

const (
	statePending = "pending"
	stateDone    = "done"
)

// Store is the Redis-backed idempotency store. Construct it with New.
type Store struct {
	rdb  *redis.Client
	ttl  time.Duration
	poll time.Duration

	reserve  *redis.Script
	finalize *redis.Script
}

// Option configures a Store.
type Option func(*Store)

// WithTTL sets the idempotency window (default DefaultTTL). Non-positive values are ignored.
func WithTTL(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.ttl = d
		}
	}
}

// WithPollInterval overrides Await's poll cadence (default defaultPollInterval). Non-positive values
// are ignored. Exposed for tests that want to keep waits short.
func WithPollInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.poll = d
		}
	}
}

// New returns a Store backed by rdb. The Lua scripts are prepared once and run via EVALSHA (go-redis
// falls back to EVAL on first use).
func New(rdb *redis.Client, opts ...Option) *Store {
	s := &Store{
		rdb:      rdb,
		ttl:      DefaultTTL,
		poll:     defaultPollInterval,
		reserve:  redis.NewScript(reserveScriptSrc),
		finalize: redis.NewScript(finalizeScriptSrc),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// key scopes the entry to one account and idempotency key. The braces make account_id a Redis Cluster
// hash tag, keeping an account's entries on one slot (mirroring internal/session and internal/bindthrottle).
func key(accountID, idemKey string) string { return "idem:{" + accountID + "}:" + idemKey }

// Reserve atomically claims the slot for (accountID, idemKey) or reads back an existing one. On a first
// reservation it stores bodyHash and response with a 24 h TTL and returns Reserved; the caller must then
// publish and Finalize, or Release on failure. A repeat with the same body returns Replay (done) or
// Pending (in flight); a repeat with a different body returns Conflict.
func (s *Store) Reserve(ctx context.Context, accountID, idemKey, bodyHash string, response []byte) (Result, error) {
	res, err := s.reserve.Run(ctx, s.rdb, []string{key(accountID, idemKey)},
		bodyHash, response, int64(s.ttl.Seconds())).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("idempotency: reserve: %w", err)
	}
	return parseReserve(res)
}

// Finalize marks a reserved entry done once its message is durably published, so subsequent repeats
// replay it. A no-op if the window already lapsed.
func (s *Store) Finalize(ctx context.Context, accountID, idemKey string) error {
	if err := s.finalize.Run(ctx, s.rdb, []string{key(accountID, idemKey)}).Err(); err != nil {
		return fmt.Errorf("idempotency: finalize: %w", err)
	}
	return nil
}

// Release removes a reservation whose publish failed, so a retry can reserve afresh rather than wait out
// the 24 h window on a message that was never queued.
func (s *Store) Release(ctx context.Context, accountID, idemKey string) error {
	if err := s.rdb.Del(ctx, key(accountID, idemKey)).Err(); err != nil {
		return fmt.Errorf("idempotency: release: %w", err)
	}
	return nil
}

// Await blocks until a pending entry is finalized (returning its stored response) or the timeout
// elapses (returning ErrAwaitTimeout). It is how a concurrent submit of the same key waits for the
// winner to confirm its publish instead of being handed a not-yet-durable result.
func (s *Store) Await(ctx context.Context, accountID, idemKey string, timeout time.Duration) ([]byte, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	for {
		state, response, err := s.read(ctx, accountID, idemKey)
		if err != nil {
			return nil, err
		}
		if state == stateDone {
			return response, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, ErrAwaitTimeout
		case <-ticker.C:
		}
	}
}

// read returns the entry's state and stored response. A vanished entry (lapsed window or released
// reservation) reads as empty state.
func (s *Store) read(ctx context.Context, accountID, idemKey string) (state string, response []byte, err error) {
	vals, err := s.rdb.HMGet(ctx, key(accountID, idemKey), "state", "response").Result()
	if err != nil {
		return "", nil, fmt.Errorf("idempotency: read: %w", err)
	}
	if len(vals) != 2 {
		return "", nil, fmt.Errorf("idempotency: read: unexpected reply length %d", len(vals))
	}
	if str, ok := vals[0].(string); ok {
		state = str
	}
	if str, ok := vals[1].(string); ok {
		response = []byte(str)
	}
	return state, response, nil
}

// parseReserve decodes reserve.lua's reply: a one- or two-element array whose first element is the
// verdict tag.
func parseReserve(arr []any) (Result, error) {
	if len(arr) == 0 {
		return Result{}, fmt.Errorf("idempotency: reserve: empty script reply")
	}
	tag, ok := arr[0].(string)
	if !ok {
		return Result{}, fmt.Errorf("idempotency: reserve: unexpected tag type %T", arr[0])
	}
	switch tag {
	case "reserved":
		return Result{Outcome: Reserved}, nil
	case "conflict":
		return Result{Outcome: Conflict}, nil
	case statePending, stateDone:
		outcome := Pending
		if tag == stateDone {
			outcome = Replay
		}
		return Result{Outcome: outcome, Response: replyBytes(arr)}, nil
	default:
		return Result{}, fmt.Errorf("idempotency: reserve: unknown verdict %q", tag)
	}
}

// replyBytes pulls the response element (index 1) out of a {state, response} reply.
func replyBytes(arr []any) []byte {
	if len(arr) < 2 {
		return nil
	}
	if str, ok := arr[1].(string); ok {
		return []byte(str)
	}
	return nil
}
