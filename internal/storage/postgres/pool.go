package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

// Pool lifetime tuning. These are conservative defaults for the control plane, which is a
// low-QPS provisioning surface, not a hot path; a milestone that puts traffic through here should
// revisit them.
const (
	poolMaxConnLifetime = 30 * time.Minute
	poolMaxConnIdleTime = 5 * time.Minute
	poolHealthCheck     = 1 * time.Minute
)

// NewPool opens the control-plane connection pool from cfg. It returns a ready pool: one connection
// is established eagerly, so a bad URL or an unreachable database fails the boot rather than the
// first request (guide de codage §10). Close the pool when done.
func NewPool(ctx context.Context, cfg config.Postgres) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		// ParseConfig echoes the DSN, password and all, in its error. Never surface it.
		return nil, fmt.Errorf("parse postgres url: invalid connection string")
	}

	pc.MaxConns = cfg.MaxConns
	pc.ConnConfig.ConnectTimeout = cfg.Timeout
	pc.MaxConnLifetime = poolMaxConnLifetime
	pc.MaxConnIdleTime = poolMaxConnIdleTime
	pc.HealthCheckPeriod = poolHealthCheck

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("reach postgres on boot: %w", err)
	}
	return pool, nil
}

// PingCheck reports the pool's health for /readyz. It probes the POOL, not an address: a pool that
// has exhausted its connections, or is pointed at a database in recovery, answers a TCP dial
// happily and every query with an error. This is the client-level ping the router's own TCP-dial
// check anticipated swapping to once a client existed. It honours ctx and bounds itself at timeout,
// because the kubelet calls it every few seconds.
func PingCheck(name string, pool *pgxpool.Pool, timeout time.Duration) observability.ReadinessCheck {
	return observability.PingCheck(name, timeout, pool.Ping)
}
