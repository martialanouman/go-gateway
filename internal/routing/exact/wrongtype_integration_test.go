package exact_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// countingMeter records the outcome of the single resolution each test performs.
type countingMeter struct{ outcomes []string }

func (m *countingMeter) Observe(outcome string) { m.outcomes = append(m.outcomes, outcome) }

// countingCorruption counts the cache values the resolver could not use.
type countingCorruption struct{ n int }

func (c *countingCorruption) Inc() { c.n++ }

// TestResolveHealsAKeyOfTheWrongType covers the third finding the review of step-250e left open.
//
// A WRONGTYPE is neither redis.Nil nor a decoding failure: before this test it was classed as a Redis
// fault, so it was returned, so the message was redelivered — onto the same key, forever, and the TTL
// bounds nothing because such a key does not expire on its own. The whole partition stays wedged until
// somebody runs a manual DEL. An illegible value already has the right treatment (count it, heal from
// the durable table, SET over it), and SET replaces a key of any type; a WRONGTYPE simply joins that
// path.
//
// It runs against a real Redis, deliberately. "WRONGTYPE" is a RESP error code the server emits, not
// a string this repository owns, and a fake that manufactured the error would be testing our own
// literal — the test would stay green even if the real code never matched.
func TestResolveHealsAKeyOfTheWrongType(t *testing.T) {
	ctx := context.Background()
	rdb := redistest.Client(t)

	repo := postgres.NewExactRouteRepo(pgtest.Pool(t))
	ported := fmt.Sprintf("22507%08d", uuid.New().ID()%100_000_000)
	want := exact.Target{Type: exact.TargetConnector, ID: uuid.New()}
	if _, err := repo.Upsert(ctx, exact.Route{
		MSISDN: ported, Target: want, Source: exact.SourceMNPImport,
	}); err != nil {
		t.Fatalf("seed the exact route: %v", err)
	}

	bloom, err := exact.LoadBloom(ctx, repo)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}

	// A list where the resolver expects a string: the shape an operator's stray LPUSH, or a key
	// recycled from another schema, leaves behind.
	cacheKey := "exactroute:{" + ported + "}"
	if err := rdb.Del(ctx, cacheKey).Err(); err != nil {
		t.Fatalf("clear the cache key: %v", err)
	}
	if err := rdb.LPush(ctx, cacheKey, "not a target").Err(); err != nil {
		t.Fatalf("seed a key of the wrong type: %v", err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), cacheKey) })

	meter := &countingMeter{}
	corrupt := &countingCorruption{}
	resolver := exact.NewResolver(bloom, rdb, repo, exact.DefaultCacheTTL,
		exact.WithLookupMeter(meter), exact.WithCorruptionMeter(corrupt))

	got, ok, err := resolver.Resolve(ctx, ported)
	if err != nil {
		t.Fatalf("Resolve() error = %v, want the durable table to answer through the bad key", err)
	}
	if !ok || got != want {
		t.Fatalf("Resolve() = %+v, %v, want %+v, true", got, ok, want)
	}
	if corrupt.n != 1 {
		t.Errorf("corruption counter = %d, want 1: a key of the wrong type is a cache anomaly, and one "+
			"that heals invisibly is one nobody ever fixes", corrupt.n)
	}
	if len(meter.outcomes) != 1 || meter.outcomes[0] != "pg_hit" {
		t.Errorf("outcomes = %v, want exactly [pg_hit]: the durable leg answered, so counting a redis "+
			"fault would blame the wrong dependency", meter.outcomes)
	}

	// The SET of the read-through overwrites whatever type the key held, so the next message pays no
	// second Postgres lookup. Without it the heal is not a heal: every message would take this path.
	if kind, err := rdb.Type(ctx, cacheKey).Result(); err != nil || kind != "string" {
		t.Errorf("key type after Resolve = %q (err=%v), want \"string\": the read-through must have "+
			"replaced the bad key, not merely stepped around it", kind, err)
	}
	// And it carries an expiry. A key of the wrong type has none — that is what made the old behaviour
	// unbounded — so a heal that replaced the value but lost the TTL would trade one permanent key for
	// another.
	if ttl, err := rdb.TTL(ctx, cacheKey).Result(); err != nil || ttl <= 0 {
		t.Errorf("TTL after Resolve = %v (err=%v), want a positive expiry: the replaced key must not "+
			"outlive the cache policy", ttl, err)
	}
}
