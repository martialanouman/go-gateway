package clickhouse

import (
	"testing"
	"time"
)

// TestPartitionExpiredBoundary pins the exact instant a partition becomes droppable. This is the decision
// that destroys data, so it is tested to the second and without a container: a partition must stay while any
// of its rows is still inside the retention window, and go the moment none is.
func TestPartitionExpiredBoundary(t *testing.T) {
	day := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	retention := 90 * 24 * time.Hour
	// The partition covers 2026-05-01 00:00:00 → 2026-05-01 23:59:59, so its last row leaves the window at
	// 2026-05-02 00:00:00 + 90 days.
	expiresAt := day.AddDate(0, 0, 1).Add(retention)

	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"long before", day.Add(time.Hour), false},
		{"one day before expiry", expiresAt.Add(-24 * time.Hour), false},
		{"one second before expiry", expiresAt.Add(-time.Second), false},
		{"exactly at expiry", expiresAt, true},
		{"one second after expiry", expiresAt.Add(time.Second), true},
		{"long after", expiresAt.AddDate(0, 0, 30), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := partitionExpired(day, retention, tc.now); got != tc.want {
				t.Errorf("partitionExpired(day=%s, retention=%s, now=%s) = %v, want %v",
					day.Format(time.RFC3339), retention, tc.now.Format(time.RFC3339), got, tc.want)
			}
		})
	}
}

// TestPartitionExpiredNeverDropsTheCurrentDay: whatever the retention (as long as it is at least a day), the
// partition being written to right now is never expired.
func TestPartitionExpiredNeverDropsTheCurrentDay(t *testing.T) {
	now := time.Date(2026, 7, 31, 15, 4, 5, 0, time.UTC)
	today := now.Truncate(24 * time.Hour)
	for _, retention := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour, 90 * 24 * time.Hour} {
		if partitionExpired(today, retention, now) {
			t.Errorf("today's partition reported expired at retention %s", retention)
		}
	}
}

// TestValidArchivePrefix: the prefix lands inside a SQL string literal, so anything that could break out of
// it — or is simply unexpected — is rejected.
func TestValidArchivePrefix(t *testing.T) {
	valid := []string{"cdr", "cdr-archive", "cold/cdr", "cdr_2026.v1"}
	for _, p := range valid {
		if !ValidArchivePrefix(p) {
			t.Errorf("ValidArchivePrefix(%q) = false, want true", p)
		}
	}
	invalid := []string{"", "cdr'", "cdr','Parquet') SELECT 1 --", "cdr archive", "cdr\n", "é"}
	for _, p := range invalid {
		if ValidArchivePrefix(p) {
			t.Errorf("ValidArchivePrefix(%q) = true, want false", p)
		}
	}
}
