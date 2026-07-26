package redis_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"testing"

	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestScriptHashMatchesSHA1 pins the client-side SHA-1: it is exactly the hex sha1 of the source, so
// EVALSHA addresses the same script SCRIPT LOAD would store. No Redis needed.
func TestScriptHashMatchesSHA1(t *testing.T) {
	t.Parallel()
	const src = `return 1`
	s := redisstore.NewScript(nil, src)

	sum := sha1.Sum([]byte(src))
	if want := hex.EncodeToString(sum[:]); s.Hash() != want {
		t.Errorf("Hash() = %q, want %q (client-side sha1 of the source)", s.Hash(), want)
	}
}

// TestScriptRunAndReloadOnNoScript covers the EVALSHA happy path and the NOSCRIPT reload: a script
// runs and returns its result, and after a SCRIPT FLUSH clears the server cache the SAME script runs
// again transparently (Run reloads via EVAL rather than erroring).
func TestScriptRunAndReloadOnNoScript(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()

	// A script exercising KEYS and ARGV so both channels are covered.
	script := redisstore.NewScript(rdb, `redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])`)

	got, err := script.Run(ctx, []string{"script:test:key"}, "hello").Result()
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if got != "hello" {
		t.Errorf("Run result = %v, want hello", got)
	}
	// Confirm the server cached it (EVALSHA path is live).
	if hits, _ := rdb.ScriptExists(ctx, script.Hash()).Result(); len(hits) != 1 || !hits[0] {
		t.Errorf("script should be cached after the first Run, ScriptExists = %v", hits)
	}

	// Flush the server cache: a bare EVALSHA would now fail NOSCRIPT.
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	// Run must reload transparently and still return the result.
	got, err = script.Run(ctx, []string{"script:test:key2"}, "world").Result()
	if err != nil {
		t.Fatalf("Run after flush (NOSCRIPT reload): %v", err)
	}
	if got != "world" {
		t.Errorf("post-flush Run result = %v, want world", got)
	}
}
