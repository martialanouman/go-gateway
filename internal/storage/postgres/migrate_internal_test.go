package postgres

import (
	"strings"
	"testing"
)

// TestToPgxURL covers the scheme rewrite that lets services and the migrator share one
// POSTGRES_URL instead of two subtly different spellings of it.
func TestToPgxURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "postgres scheme",
			in:   "postgres://gateway:pw@localhost:5432/gateway?sslmode=disable",
			want: "pgx5://gateway:pw@localhost:5432/gateway?sslmode=disable",
		},
		{
			name: "postgresql scheme",
			in:   "postgresql://gateway:pw@localhost:5432/gateway",
			want: "pgx5://gateway:pw@localhost:5432/gateway",
		},
		{
			name: "already pgx5",
			in:   "pgx5://gateway:pw@localhost:5432/gateway",
			want: "pgx5://gateway:pw@localhost:5432/gateway",
		},
		{
			name: "query string preserved",
			in:   "postgres://h/db?sslmode=require&pool_max_conns=10",
			want: "pgx5://h/db?sslmode=require&pool_max_conns=10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toPgxURL(tc.in)
			if err != nil {
				t.Fatalf("toPgxURL(%q) error = %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("toPgxURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestToPgxURLRejectsOtherSchemes: passing a mysql:// URL through would make golang-migrate pick
// a driver this package never registered and fail with an unhelpful error. Reject it here.
func TestToPgxURLRejectsOtherSchemes(t *testing.T) {
	tests := []string{
		"",
		"mysql://u:p@localhost:3306/db",
		"clickhouse://localhost:9000/db",
		"localhost:5432/gateway",
		"gateway",
		"://nope",
	}

	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			if got, err := toPgxURL(in); err == nil {
				t.Errorf("toPgxURL(%q) = %q, want an error", in, got)
			}
		})
	}
}

// TestToPgxURLErrorHidesCredentials: the URL embeds the database password, so a rejection must
// not echo it into a log or a CI transcript (guide de codage §10/§11).
func TestToPgxURLErrorHidesCredentials(t *testing.T) {
	const password = "sup3r-s3cret-canary"

	_, err := toPgxURL("mysql://gateway:" + password + "@localhost:3306/db")
	if err == nil {
		t.Fatal("want an error")
	}
	if strings.Contains(err.Error(), password) {
		t.Errorf("error leaks the database password: %v", err)
	}
}
