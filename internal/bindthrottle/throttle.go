// Package bindthrottle is the anti-brute-force layer in front of the SMPP bind. It keeps a per
// system_id and per source-IP count of recent authentication failures in Redis and, once a subject
// crosses the configured threshold, has the caller refuse the bind with a progressive backoff — before
// the argon2id verification runs, so a flood of guesses cannot make the server pay that CPU cost.
//
// It is defence in depth, not the authentication itself: argon2id (internal/credential) remains the
// real check. The caller (internal/smppserver) treats a Redis failure as fail-open — the throttle
// simply steps aside — so a Redis outage never takes down the SMPP ingress. The counters are updated
// with a single atomic INCR+EXPIRE Lua script (never a read-modify-write from Go), and the EXPIRE makes
// each subject's window slide from its last failure.
//
// The two counters live under distinct keys (a system_id and an IP are different subjects, potentially
// on different Redis Cluster slots), so every operation touches one key at a time — never a multi-key
// script that would break under Cluster.
package bindthrottle

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/incr_expire.lua
var incrExpireSrc string

// Config tunes the throttle. All fields are validated at the configuration layer (config.SMPP), so New
// trusts them; the only defensive floor is a one-second minimum window (see windowSeconds).
type Config struct {
	// MaxFailures is the number of failures a subject may accumulate before its next bind is throttled.
	MaxFailures int
	// Window is how long a failure is remembered. Each new failure slides the window forward, so the
	// lockout lifts only after a full quiet window.
	Window time.Duration
	// BackoffBase is the delay applied to the first throttled bind (at the threshold).
	BackoffBase time.Duration
	// BackoffMax caps the progressive backoff, so the tarpit delay never grows without bound.
	BackoffMax time.Duration
}

// Subject names which counter tripped a block, for the ops metric's bounded label (never the value
// itself, which would be high-cardinality).
const (
	SubjectSystemID = "system_id"
	SubjectIP       = "ip"
)

// Decision is the throttle's verdict for a bind attempt. The zero value (Blocked false) means "let the
// bind proceed"; it is also what the caller uses on a Redis error to fail open.
type Decision struct {
	// Blocked reports whether the bind must be refused without running authentication.
	Blocked bool
	// RetryAfter is the backoff the caller applies before refusing, growing with the failure count up
	// to Config.BackoffMax.
	RetryAfter time.Duration
	// Failures is the count that tripped the block (the larger of the two subjects), for the audit log.
	Failures int
	// Subject is SubjectSystemID or SubjectIP: which counter reached the threshold, for the metric.
	Subject string
}

// Throttle records bind failures and decides when a bind must be refused. Construct it with New.
type Throttle struct {
	rdb        *redis.Client
	cfg        Config
	incrExpire *redis.Script
}

// New returns a Throttle backed by rdb. The Lua script is prepared once and run via EVALSHA (go-redis
// falls back to EVAL on first use).
func New(rdb *redis.Client, cfg Config) *Throttle {
	return &Throttle{
		rdb:        rdb,
		cfg:        cfg,
		incrExpire: redis.NewScript(incrExpireSrc),
	}
}

// systemKey and ipKey scope each counter under its own key. The braces make the subject a Redis Cluster
// hash tag, keeping a subject's operations on one slot (mirroring internal/session's style).
func systemKey(systemID string) string { return "bindfail:{" + systemID + "}" }
func ipKey(ip string) string           { return "bindfail:ip:{" + ip + "}" }

// Check reports whether a bind for systemID from ip must be refused. It reads both counters (pure GETs,
// never a read-modify-write) and blocks when the larger reaches MaxFailures, returning the progressive
// backoff to apply. A missing counter reads as zero.
func (t *Throttle) Check(ctx context.Context, systemID, ip string) (Decision, error) {
	sid, err := t.count(ctx, systemKey(systemID))
	if err != nil {
		return Decision{}, err
	}
	ipc, err := t.count(ctx, ipKey(ip))
	if err != nil {
		return Decision{}, err
	}

	failures := max(sid, ipc)
	if failures < t.cfg.MaxFailures {
		return Decision{}, nil
	}
	// On a tie both subjects are hot; attribute to system_id, the narrower (per-account) signal.
	subject := SubjectIP
	if sid >= ipc {
		subject = SubjectSystemID
	}
	return Decision{Blocked: true, RetryAfter: t.backoff(failures), Failures: failures, Subject: subject}, nil
}

// count reads a counter, treating a missing key as zero.
func (t *Throttle) count(ctx context.Context, key string) (int, error) {
	n, err := t.rdb.Get(ctx, key).Int()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("bindthrottle: read %s: %w", key, err)
	}
	return n, nil
}

// RecordFailure increments both the system_id and the IP counter and refreshes their sliding window.
// It is called only on an authentication/authorisation failure that actually ran — never on an
// already-throttled attempt, so an attacker who varies the (attacker-controlled) system_id cannot mint
// unbounded Redis keys while blocked. The two counters live on distinct keys (distinct Cluster slots),
// so they are two separate atomic scripts; if the second fails the first still counted, a safe bias
// (over-counting the system_id, never under-counting).
func (t *Throttle) RecordFailure(ctx context.Context, systemID, ip string) error {
	win := t.windowSeconds()
	if err := t.incrExpire.Run(ctx, t.rdb, []string{systemKey(systemID)}, win).Err(); err != nil {
		return fmt.Errorf("bindthrottle: record system_id failure: %w", err)
	}
	if err := t.incrExpire.Run(ctx, t.rdb, []string{ipKey(ip)}, win).Err(); err != nil {
		return fmt.Errorf("bindthrottle: record ip failure: %w", err)
	}
	return nil
}

// Reset clears the system_id counter after a successful bind. The IP counter is deliberately left to
// decay on its own window, so one legitimate bind cannot hand an attacker sharing the source IP (behind
// a NAT) a clean slate.
func (t *Throttle) Reset(ctx context.Context, systemID string) error {
	if err := t.rdb.Del(ctx, systemKey(systemID)).Err(); err != nil {
		return fmt.Errorf("bindthrottle: reset %s: %w", systemID, err)
	}
	return nil
}

// backoff returns the delay for a subject at the given failure count: BackoffBase doubled once per
// failure past the threshold, capped at BackoffMax. Doubling stops as soon as the cap is reached, so it
// cannot overflow.
func (t *Throttle) backoff(failures int) time.Duration {
	over := failures - t.cfg.MaxFailures
	d := t.cfg.BackoffBase
	for i := 0; i < over && d < t.cfg.BackoffMax; i++ {
		d *= 2
	}
	// Clamp to the cap; the d <= 0 guard catches a doubling overflow to a negative duration, which a
	// pathological (unvalidated) BackoffMax near math.MaxInt64 could otherwise let through.
	if d > t.cfg.BackoffMax || d <= 0 {
		d = t.cfg.BackoffMax
	}
	return d
}

// windowSeconds is the EXPIRE argument, floored at one second: a sub-second window would truncate to
// zero and make EXPIRE delete the key immediately. Config validation keeps the real value well above
// this floor.
func (t *Throttle) windowSeconds() int64 {
	s := int64(t.cfg.Window.Seconds())
	if s < 1 {
		return 1
	}
	return s
}
