package postgres_test

import (
	"context"
	"testing"
)

// TestPriorityFlagCheckMigrationClampsExistingRows: before step-294 the column was set only in SQL and
// had no bound, so a value out of the SMPP range may exist. The migration must clamp it to what the pool
// already sent (it clamped on read) instead of failing and leaving the database dirty.
func TestPriorityFlagCheckMigrationClampsExistingRows(t *testing.T) {
	ctx := context.Background()
	m, conn := migrateFreshTo(t, 32)
	for label, value := range map[string]int{"above": 5, "below": -1, "inside": 2} {
		if _, err := conn.Exec(ctx, `INSERT INTO control_plane.smsc_connectors
			(name, host, port, bind_type, system_id, password_sealed, password_kms_key_ref, priority_flag_default)
			VALUES ($1, 'h', 2775, 'trx', 's', '\x01', 'test/v1', $2)`, label, value); err != nil {
			t.Fatalf("seed %s: %v", label, err)
		}
	}

	if err := m.Steps(1); err != nil {
		t.Fatalf("migrate 0033: %v", err)
	}
	for label, want := range map[string]int{"above": 3, "below": 0, "inside": 2} {
		var got int
		if err := conn.QueryRow(ctx, `SELECT priority_flag_default FROM control_plane.smsc_connectors WHERE name = $1`, label).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", label, err)
		}
		if got != want {
			t.Errorf("%s: priority_flag_default = %d, want %d", label, got, want)
		}
	}
}
