package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// Script is a Lua script bound to a client, executed atomically server-side. It is the project's one
// mechanism for atomic operational state: the golden rule (CLAUDE.md) forbids a read-modify-write from
// Go for a shared counter, token bucket, credit reserve or breaker — put the whole atomic step in a
// Script instead, so Redis runs it indivisibly.
//
// The SHA-1 is computed once client-side; Run sends EVALSHA and, on NOSCRIPT (the script is not in the
// server cache — after a SCRIPT FLUSH, a restart or a failover), reloads it transparently and retries,
// falling back to EVAL. Callers therefore never manage the script cache themselves.
//
// Over go-redis's own *Script it adds two things: it binds the client so every atomic op reads as
// Run(ctx, keys, args) with no client threaded through, and it is the single seam through which future
// milestones will attach a per-execution OTel span or metric without touching each call site. It is a
// thin wrapper, not a full abstraction boundary — Run still returns go-redis's *Cmd, so callers read
// the result with go-redis's own accessors.
type Script struct {
	client goredis.Scripter
	script *goredis.Script
}

// NewScript prepares src as an atomic script bound to client (a *redis.Client satisfies Scripter).
func NewScript(client goredis.Scripter, src string) *Script {
	return &Script{client: client, script: goredis.NewScript(src)}
}

// Run executes the script with the given KEYS and ARGV, returning the raw command to read the result
// from (.Int(), .Bool(), .Result()…). Keep a script to a single key (or a Cluster hash tag) so every
// operation lands on one slot. The message body never transits here — keys and counters only
// (invariant a).
func (s *Script) Run(ctx context.Context, keys []string, args ...any) *goredis.Cmd {
	return s.script.Run(ctx, s.client, keys, args...)
}

// Hash returns the script's SHA-1, identical to what SCRIPT LOAD returns for the same source.
func (s *Script) Hash() string { return s.script.Hash() }
