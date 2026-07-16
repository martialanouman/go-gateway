package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestPingCheckReportsAHealthyPool is the readiness contract: against a live, migrated database the
// probe must succeed. It doubles as proof that pgtest brings up a schema the pool can reach.
func TestPingCheckReportsAHealthyPool(t *testing.T) {
	pool := pgtest.Pool(t)

	check := postgres.PingCheck("postgres", pool, 3*time.Second)
	if check.Name != "postgres" {
		t.Errorf("check.Name = %q, want postgres", check.Name)
	}
	if err := check.Probe(context.Background()); err != nil {
		t.Errorf("Probe() on a healthy pool = %v, want nil", err)
	}
}

// TestTheMigratedSchemaHasTheControlPlaneTables proves the shared container really ran
// migrations/0001: the seven M1 tables exist in the control_plane schema.
func TestTheMigratedSchemaHasTheControlPlaneTables(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	for _, table := range []string{
		"customers", "smpp_accounts", "credentials", "smsc_connectors",
		"routes", "route_targets", "sender_ids",
	} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'control_plane' AND table_name = $1)`, table).Scan(&exists)
		if err != nil {
			t.Fatalf("query for table %q: %v", table, err)
		}
		if !exists {
			t.Errorf("control_plane.%s is missing from the migrated schema", table)
		}
	}
}
