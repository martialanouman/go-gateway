package cancel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// flagTTL bounds how long a cancel intent lives in Redis. It must outlast the longest a message can
// sit queued before dispatch (its validity_period); 72h covers the SMS maximum. Past it the message
// is no longer routable, so a stale flag would gate nothing — the TTL just reclaims the key.
const flagTTL = 72 * time.Hour

// dispatchTTL bounds the connector's token. Unlike a cancel intent it does NOT have to outlive the
// message: it only has to cover the window in which the CDR projection still reads `accepted` for a
// message already on the wire (step-209, DN4). Past that window the second guard takes over — the
// Canceller reads `enroute` off the projection and refuses the cancel before it ever reaches Redis.
//
// INVARIANT: dispatchTTL must comfortably exceed the mt.outcome status-lag alert threshold (30s,
// step-201c). Below it the two guards stop overlapping and the race reopens in exactly the saturation
// the alert exists to catch. 5 minutes leaves 10x of margin.
//
// It is a constant rather than configuration on purpose: it is the safety margin of a correctness
// guard, not a capacity lever, and a deployment free to lower it is free to reopen the bug.
const dispatchTTL = 5 * time.Minute

// Holder names who owns the cancel token of a message. The token is single-winner: the first claim
// takes it and every later claim is told who holds it (step-209, ADR-0013).
type Holder string

const (
	// HolderNone is the zero Holder, returned when the token was free: the caller has just taken it.
	// It is NOT meaningful when Claim also returns an error.
	HolderNone Holder = ""
	// HolderCancel is a cancel_sm that won the token. The message must not be dispatched.
	HolderCancel Holder = "cancel"
	// HolderDispatched is the connector pool claiming a message it is about to put on the SMSC wire.
	// A cancel_sm that reads it has lost the race and must refuse (ESME_RCANCELFAIL) rather than
	// record a cancellation of a message already gone.
	HolderDispatched Holder = "dispatched"
)

// ttl is the lifetime a holder's token gets. It is derived from the holder rather than passed in, so
// no call site can claim with the wrong expiry.
func (h Holder) ttl() time.Duration {
	if h == HolderDispatched {
		return dispatchTTL
	}
	return flagTTL
}

// RedisFlags is the shared cancel-token store: the Canceller and the connector pool arbitrate on it,
// each claiming as itself, and the loser learns who won. The key embeds the message_id as its Redis
// Cluster hash tag, so every operation touches a single slot (never a multi-key op).
type RedisFlags struct {
	rdb *redis.Client
}

// NewRedisFlags builds a flag store over rdb.
func NewRedisFlags(rdb *redis.Client) *RedisFlags {
	return &RedisFlags{rdb: rdb}
}

// key scopes the token under the message_id, which doubles as the Cluster hash tag.
func key(id uuid.UUID) string { return "cancel:{" + id.String() + "}" }

// Claim takes the cancel token of id for as, and reports who actually holds it: HolderNone when the
// token was free and the caller just took it, otherwise the holder already in place — including
// `as` itself, which is how the connector recognises its own token after a Kafka redelivery instead
// of mistaking it for a cancellation.
//
// It is ONE round trip and atomic: `SET key value EX <ttl> NX GET` sets only if absent and returns
// the previous value either way, so two claimants racing on the same message cannot both win. A
// losing claim writes nothing, which is what leaves the winner's expiry untouched.
//
// Claiming as HolderNone is refused. It is the value that MEANS "free", so writing it would leave a
// key every later claimant reads as an unheld token — the connector dispatching and the Canceller
// recording a cancellation, both believing they had won. The arbitration would stop arbitrating with
// no error anywhere, so the misuse is rejected here, where it is still visible.
//
// The returned Holder is meaningless when err is non-nil; callers must branch on the error first.
func (f *RedisFlags) Claim(ctx context.Context, id uuid.UUID, as Holder) (Holder, error) {
	if as == HolderNone {
		return HolderNone, fmt.Errorf("cancel: claim %s: refusing to claim as the free holder", id)
	}
	prev, err := f.rdb.SetArgs(ctx, key(id), string(as), redis.SetArgs{
		Mode: "NX",
		TTL:  as.ttl(),
		Get:  true,
	}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return HolderNone, nil // no previous value: the token was free and is now ours
	case err != nil:
		return HolderNone, fmt.Errorf("cancel: claim %s as %s: %w", id, as, err)
	default:
		return Holder(prev), nil
	}
}
