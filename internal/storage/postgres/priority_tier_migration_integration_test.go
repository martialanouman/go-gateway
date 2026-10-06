package postgres_test

import (
	"context"
	"testing"
)

// TestPriorityTierCheckMigrationClampsExistingRows: priority_tier had no bound until step-292, so a value
// out of 0..2 may exist. The migration clamps it instead of failing and leaving the database dirty: a tier
// above 2 accepted no category, 2 keeps the connector reserved to otp, the closest meaning.
func TestPriorityTierCheckMigrationClampsExistingRows(t *testing.T) {
	ctx := context.Background()
	m, conn := migrateFreshTo(t, 33)
	for label, value := range map[string]int{"above": 5, "below": -1, "inside": 1} {
		if _, err := conn.Exec(ctx, `INSERT INTO control_plane.smsc_connectors
			(name, host, port, bind_type, system_id, password_sealed, password_kms_key_ref, priority_tier)
			VALUES ($1, 'h', 2775, 'trx', 's', '\x01', 'test/v1', $2)`, label, value); err != nil {
			t.Fatalf("seed %s: %v", label, err)
		}
	}

	if err := m.Steps(1); err != nil {
		t.Fatalf("migrate 0034: %v", err)
	}
	for label, want := range map[string]int{"above": 2, "below": 0, "inside": 1} {
		var got int
		if err := conn.QueryRow(ctx, `SELECT priority_tier FROM control_plane.smsc_connectors WHERE name = $1`, label).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", label, err)
		}
		if got != want {
			t.Errorf("%s: priority_tier = %d, want %d", label, got, want)
		}
	}
}
