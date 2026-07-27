package exact

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// RedisGetter is the read surface the resolver needs from Redis. A *goredis.Client satisfies it.
type RedisGetter interface {
	Get(ctx context.Context, key string) *goredis.StringCmd
}

// Resolver is the L0 exact-number lookup: a per-pod Bloom gate in front of the shared Redis map
// exactroute:{msisdn} (Appendix B). A Bloom miss is a definitive "no override" answered without a
// network call; a Bloom hit is confirmed against Redis, so a false positive never routes a message to
// the wrong target.
type Resolver struct {
	bloom *Bloom
	redis RedisGetter
}

// NewResolver builds the L0 resolver over a boot-loaded Bloom and a Redis reader.
func NewResolver(bloom *Bloom, redis RedisGetter) *Resolver {
	return &Resolver{bloom: bloom, redis: redis}
}

// redisKey is the exact-route key for an MSISDN (Appendix B). The hash tag ({msisdn}) pins a number's
// key to one cluster slot, matching how config-sync writes it (step-105).
func redisKey(msisdn string) string { return "exactroute:{" + msisdn + "}" }

// Resolve returns the exact-route target for an MSISDN. ok is false for the common "no override" case
// — a Bloom miss, or a Bloom possible-hit with no Redis entry (a false positive, or not yet synced) —
// and the caller then falls back to normal route resolution. A non-nil error is a transient Redis
// fault; a missing key is not an error.
func (r *Resolver) Resolve(ctx context.Context, msisdn string) (Target, bool, error) {
	if !r.bloom.MightContain(msisdn) {
		return Target{}, false, nil // definitive miss: no Redis read
	}
	val, err := r.redis.Get(ctx, redisKey(msisdn)).Result()
	if errors.Is(err, goredis.Nil) {
		return Target{}, false, nil // Bloom false positive / not yet synced: fall back
	}
	if err != nil {
		return Target{}, false, fmt.Errorf("exact: redis lookup: %w", err)
	}
	target, err := parseTarget(val)
	if err != nil {
		return Target{}, false, err
	}
	return target, true, nil
}

// parseTarget decodes the Redis value "<target_type>:<uuid>" into a Target — the Appendix B encoding
// config-sync writes (step-105). A malformed or unknown value is an error, not a silent miss, so a
// sync bug surfaces rather than mis-routing.
func parseTarget(val string) (Target, error) {
	kind, id, ok := strings.Cut(val, ":")
	if !ok {
		return Target{}, fmt.Errorf("exact: malformed target %q", val)
	}
	tt := TargetType(kind)
	if tt != TargetConnector && tt != TargetRoute {
		return Target{}, fmt.Errorf("exact: unknown target type %q", kind)
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return Target{}, fmt.Errorf("exact: target id %q: %w", id, err)
	}
	return Target{Type: tt, ID: uid}, nil
}

// EncodeTarget renders a Target as the Redis value config-sync writes (step-105). It is the inverse of
// parseTarget and lives here so the encoding has one owner.
func EncodeTarget(t Target) string {
	return string(t.Type) + ":" + t.ID.String()
}
