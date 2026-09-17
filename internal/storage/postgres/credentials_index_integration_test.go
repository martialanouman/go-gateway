package postgres_test

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestCredentialLookupColumnsAreIndexed pins what three documents assert: that REST authentication
// resolves an account BY the key hash through an index (plan §1.9, the internal/credential package doc,
// internal/storage/postgres/authn.go). Until step-290d that index did not exist, in any of the fifteen
// migrations, and every authenticated request scanned control_plane.credentials end to end — on the
// surface sized for 8 000 requests per second.
//
// It asserts existence, not a plan: on a table of a few rows PostgreSQL rightly prefers a sequential
// scan whatever the indexes, so an EXPLAIN here would prove the opposite of what it looked like.
func TestCredentialLookupColumnsAreIndexed(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.Pool(t)

	rows, err := pool.Query(ctx,
		`SELECT indexname, indexdef FROM pg_indexes
		   WHERE schemaname = 'control_plane' AND tablename = 'credentials'`)
	if err != nil {
		t.Fatalf("read pg_indexes: %v", err)
	}
	defer rows.Close()

	found := map[string]string{}
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			t.Fatalf("scan index definition: %v", err)
		}
		found[name] = def
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_indexes: %v", err)
	}

	// Both, because PrincipalByAPIKeyHash matches an OR — the live hash, or the previous one inside a
	// rotation grace window. Indexing only the first leaves the planner scanning for the second branch.
	//
	// The COLUMN is asserted, not just the name: an index of the right name on the wrong column would
	// leave the lookup exactly as slow while looking fixed here.
	for _, want := range []struct{ name, column string }{
		{"credentials_api_key_hash_idx", "api_key_hash"},
		{"credentials_previous_secret_hash_idx", "previous_secret_hash"},
	} {
		def, ok := found[want.name]
		if !ok {
			t.Errorf("index %q is missing (present: %v): the REST auth lookup is a sequential scan, and "+
				"three documents say otherwise", want.name, slices.Sorted(maps.Keys(found)))
			continue
		}
		if !strings.Contains(def, "("+want.column+")") {
			t.Errorf("index %q is %q, want it on %s: a lookup by key hash cannot use it otherwise",
				want.name, def, want.column)
		}
	}
}
