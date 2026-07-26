package antispam_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRedisDuplicateChecker proves the SET NX EX semantics against real Redis: the first sighting of a
// fingerprint is new, an immediate repeat is a duplicate, and after the window elapses it is new
// again.
func TestRedisDuplicateChecker(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	checker := antispam.NewRedisDuplicateChecker(rdb)

	const fp = "abc123fingerprint"

	seen, err := checker.Seen(ctx, fp, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("first Seen: %v", err)
	}
	if seen {
		t.Error("first sighting must not be a duplicate")
	}

	seen, err = checker.Seen(ctx, fp, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("second Seen: %v", err)
	}
	if !seen {
		t.Error("an immediate repeat within the window must be a duplicate")
	}

	// After the TTL elapses, the fingerprint is forgotten and a new sighting is not a duplicate.
	time.Sleep(300 * time.Millisecond)
	seen, err = checker.Seen(ctx, fp, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("post-expiry Seen: %v", err)
	}
	if seen {
		t.Error("after the window elapses, the fingerprint must be new again")
	}
}
