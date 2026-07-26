package optout_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestGuardAgainstPostgres proves the snapshot and the exact confirmation load real suppressions: a
// platform-scoped and a customer-scoped number are suppressed, an unlisted number is not, and the
// exact check honours scope isolation — all through the new ListSuppressions / IsSuppressed queries.
func TestGuardAgainstPostgres(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "OptOutCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}

	// Seed suppressions directly (the write side is Admin/MO STOP, step-063/064). msisdn is stored
	// E.164-normalized, the digits-only form the pipeline destination carries.
	const platformNum = "2250700000001"
	const customerNum = "2250700000002"
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.suppressions (scope, scope_id, msisdn, source)
		 VALUES ('platform', NULL, $1, 'admin'), ('customer', $2, $3, 'mo_stop')`,
		platformNum, customer.ID, customerNum); err != nil {
		t.Fatalf("seed suppressions: %v", err)
	}

	repo := postgres.NewSuppressionRepo(pool)
	snap, err := optout.LoadSnapshot(ctx, repo)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	guard := optout.NewGuard(snap, repo)

	cases := []struct {
		name    string
		scope   cp.SuppressionScope
		scopeID *uuid.UUID
		msisdn  string
		want    bool
	}{
		{"platform suppressed", cp.SuppressionScopePlatform, nil, platformNum, true},
		{"customer suppressed", cp.SuppressionScopeCustomer, &customer.ID, customerNum, true},
		{"customer scope, platform number not in customer scope", cp.SuppressionScopeCustomer, &customer.ID, platformNum, false},
		{"unlisted number not suppressed", cp.SuppressionScopePlatform, nil, "2250709999999", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := guard.IsSuppressed(ctx, tc.scope, tc.scopeID, tc.msisdn)
			if err != nil {
				t.Fatalf("IsSuppressed: %v", err)
			}
			if got != tc.want {
				t.Errorf("IsSuppressed(%s, %s) = %t, want %t", tc.scope, tc.msisdn, got, tc.want)
			}
		})
	}
}

// TestSuppressionRejectsNonCanonicalMSISDN locks the canonical-form guarantee (migration 0003): a
// non-normalized MSISDN ("+225…") is refused at write, so no write path can silently create an entry
// the opt-out lookup (which keys on the digits-only form) would never match — a false negative.
func TestSuppressionRejectsNonCanonicalMSISDN(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	for _, bad := range []string{"+2250700000001", "225 0700000001", "0700000001x"} {
		_, err := pool.Exec(ctx,
			`INSERT INTO control_plane.suppressions (scope, scope_id, msisdn, source) VALUES ('platform', NULL, $1, 'admin')`, bad)
		if err == nil {
			t.Errorf("insert of non-canonical msisdn %q succeeded, want the CHECK to reject it", bad)
		}
	}
}
