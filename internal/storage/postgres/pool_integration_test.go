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

// TestNewPoolPreWarmsMinConns is the step-201 D5/D9 contract for pgxpool: MinConns is the number of
// connections the pool keeps warm, so a pod that has been idle does not answer a traffic peak by
// dialling and authenticating several sessions at once.
//
// It asserts the effect, not the assignment: pgxpool creates max(MinConns, MinIdleConns) resources in
// a background goroutine as soon as the pool is built (pgxpool/pool.go:333-337), so a wired pool grows
// to MinConns with no query ever issued. The MinConns=0 pool is the control — it must stay at the
// single connection the boot ping opened, which is what fails a hardcoded value.
func TestNewPoolPreWarmsMinConns(t *testing.T) {
	const warm = 4

	cfg := pgtest.Config(t)
	cfg.MaxConns = warm
	cfg.MinConns = warm

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool() = %v, want nil", err)
	}
	defer pool.Close()

	if got := pool.Config().MinConns; got != warm {
		t.Errorf("pool.Config().MinConns = %d, want %d", got, warm)
	}

	deadline := time.Now().Add(10 * time.Second)
	for pool.Stat().TotalConns() < warm && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := pool.Stat().TotalConns(); got < warm {
		t.Errorf("warm pool TotalConns() = %d after 10s, want at least %d", got, warm)
	}

	coldCfg := pgtest.Config(t)
	coldCfg.MinConns = 0
	cold, err := postgres.NewPool(ctx, coldCfg)
	if err != nil {
		t.Fatalf("NewPool() with MinConns=0 = %v, want nil", err)
	}
	defer cold.Close()

	time.Sleep(500 * time.Millisecond)
	if got := cold.Stat().TotalConns(); got > 1 {
		t.Errorf("MinConns=0 pool TotalConns() = %d, want at most 1 (the boot ping's own)", got)
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
