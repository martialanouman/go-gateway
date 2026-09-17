package postgres_test

import (
	"context"
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
		`SELECT indexname FROM pg_indexes WHERE schemaname = 'control_plane' AND tablename = 'credentials'`)
	if err != nil {
		t.Fatalf("read pg_indexes: %v", err)
	}
	defer rows.Close()

	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_indexes: %v", err)
	}

	// Both, because PrincipalByAPIKeyHash matches an OR — the live hash, or the previous one inside a
	// rotation grace window. Indexing only the first leaves the planner scanning for the second branch.
	for _, want := range []string{"credentials_api_key_hash_idx", "credentials_previous_secret_hash_idx"} {
		if !found[want] {
			t.Errorf("index %q is missing (present: %v): the REST auth lookup is a sequential scan, and "+
				"three documents say otherwise", want, found)
		}
	}
}
