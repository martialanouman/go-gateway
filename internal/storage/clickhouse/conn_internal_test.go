package clickhouse

import (
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
)

// TestNewConnAppliesTheConnectionLimits is the step-201 D5 contract for ClickHouse. The values are
// deliberately not the library's own defaults (5 idle, idle+5 open — clickhouse_options.go:412-417),
// so an unwired option reports those instead and the test fails.
//
// Stats reads the resolved pool: MaxOpenConns is cap(ch.open), the semaphore that actually bounds
// concurrent queries, and MaxIdleConns is the idle pool's capacity (clickhouse.go:243-251). Open is
// lazy, so no server is involved.
func TestNewConnAppliesTheConnectionLimits(t *testing.T) {
	cfg := config.ClickHouse{
		Addr:         []string{"localhost:9000"},
		Database:     "gateway",
		Username:     "gateway",
		Password:     "gateway",
		Timeout:      5 * time.Second,
		MaxOpenConns: 7,
		MaxIdleConns: 3,
	}

	conn, err := NewConn(cfg)
	if err != nil {
		t.Fatalf("NewConn() = %v, want nil", err)
	}
	defer func() { _ = conn.Close() }()

	stats := conn.conn.Stats()
	if stats.MaxOpenConns != cfg.MaxOpenConns {
		t.Errorf("Stats().MaxOpenConns = %v, want %v", stats.MaxOpenConns, cfg.MaxOpenConns)
	}
	if stats.MaxIdleConns != cfg.MaxIdleConns {
		t.Errorf("Stats().MaxIdleConns = %v, want %v", stats.MaxIdleConns, cfg.MaxIdleConns)
	}
}
