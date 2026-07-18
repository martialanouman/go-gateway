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
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	return &Conn{conn: conn}, nil
}

// Ping reports whether ClickHouse is reachable.
func (c *Conn) Ping(ctx context.Context) error { return c.conn.Ping(ctx) }

// ReadyCheck adapts the connection to a readiness probe. ClickHouse is vital for get-message (it
// reads the CDR) and for writing the enroute/failed rows; it is NOT on the 202-acceptance path,
// which depends only on the durable Kafka write (§1.10).
func (c *Conn) ReadyCheck(name string, timeout time.Duration) observability.ReadinessCheck {
	return observability.ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return c.conn.Ping(ctx)
		},
	}
}

// Close releases the connection pool.
func (c *Conn) Close() error { return c.conn.Close() }
