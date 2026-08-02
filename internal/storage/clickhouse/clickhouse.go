// Package clickhouse is the CDR / analytics sink (plan §1.10): a native-protocol client plus the
// versioned-write helpers the pipeline uses. It owns the connection, the migration runner for the
// separate ClickHouse migration set, and the CDR reader/writer.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// Conn is a ClickHouse connection over the native protocol. The pool is managed internally by the
// driver; the connection is lazily established on first use.
type Conn struct {
	conn driver.Conn
}

// NewConn opens a connection to the configured ClickHouse. It does not block on reachability; use
// ReadyCheck for that.
func NewConn(cfg config.ClickHouse) (*Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Addr,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: cfg.Timeout,
		// Pool sizing (step-201, D5). It matters for admin-api-svc, where search-messages queries and
		// CDR exports contend, far more than for the CDR writer, which is one insert loop. Both are
		// refused as non-positive by config validation, because the library silently substitutes its
		// own defaults for a value <= 0 (clickhouse_options.go:412-417).
		//
		// Deliberately absent: Settings passthrough. It is the back door to async_insert, and
		// async_insert with wait_for_async_insert=0 acknowledges an insert before it is durable —
		// the Kafka offset would then be committed for a CDR that quietly never lands (D6/D8).
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return &Conn{conn: conn}, nil
}

// Ping reports whether ClickHouse is reachable.
func (c *Conn) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }

// Exec runs a statement returning no rows. It is the maintenance escape hatch — partition drops, TTL
// materialisation, archive writes (see Retainer) — for SQL that is DDL and therefore cannot be expressed
// through the typed read/write paths. Data-plane code uses CDRWriter/CDRReader instead.
func (c *Conn) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

// Query runs a query returning rows, for the same maintenance surface as Exec.
func (c *Conn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// QueryRow runs a query returning at most one row, for the same maintenance surface as Exec.
func (c *Conn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

// ReadyCheck adapts the connection to a readiness probe. ClickHouse is vital for get-message (it
// reads the CDR) and for writing the enroute/failed rows; it is NOT on the 202-acceptance path,
// which depends only on the durable Kafka write (§1.10).
func (c *Conn) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, c.conn.Ping)
}

// Close releases the connection pool.
func (c *Conn) Close() error { return c.conn.Close() }
