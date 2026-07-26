package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestRateLimitRepoList proves the repo loads every configured throughput limit from real Postgres,
// across entity kinds, and maps the individually-nullable columns — the shape the router's rate-limit
// snapshot consumes (step-085).
func TestRateLimitRepoList(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	// rate_limits is a global table read unfiltered; start clean so shared-pool tests are isolated.
	if _, err := pool.Exec(ctx, "DELETE FROM control_plane.rate_limits"); err != nil {
		t.Fatalf("clean rate_limits: %v", err)
	}

	account, connector := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.rate_limits (entity_type, entity_id, max_per_sec, max_per_day, burst_capacity) VALUES
		 ('smpp_account', $1, 100, 50000, 200),
		 ('connector',    $2, 30,   NULL,  30)`, account, connector); err != nil {
		t.Fatalf("seed rate_limits: %v", err)
	}

	entries, err := postgres.NewRateLimitRepo(pool).List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List returned %d entries, want 2", len(entries))
	}

	byEntity := map[string]int{}
	for _, e := range entries {
		byEntity[e.EntityType] = len(byEntity) // presence marker only
		switch e.EntityType {
		case "smpp_account":
			if e.EntityID != account {
				t.Errorf("account entity_id = %s, want %s", e.EntityID, account)
			}
			if e.Limit.MaxPerSec == nil || *e.Limit.MaxPerSec != 100 || e.Limit.BurstCapacity == nil || *e.Limit.BurstCapacity != 200 {
				t.Errorf("account limit = %+v, want per_sec 100 / burst 200", e.Limit)
			}
			if e.Limit.MaxPerDay == nil || *e.Limit.MaxPerDay != 50000 {
				t.Errorf("account max_per_day = %v, want 50000", e.Limit.MaxPerDay)
			}
		case "connector":
			if e.Limit.MaxPerSec == nil || *e.Limit.MaxPerSec != 30 {
				t.Errorf("connector max_per_sec = %v, want 30", e.Limit.MaxPerSec)
			}
			if e.Limit.MaxPerDay != nil {
				t.Errorf("connector max_per_day = %v, want nil (NULL preserved)", e.Limit.MaxPerDay)
			}
		}
	}
	if _, ok := byEntity["smpp_account"]; !ok {
		t.Error("account limit missing from List")
	}
	if _, ok := byEntity["connector"]; !ok {
		t.Error("connector limit missing from List")
	}
}
