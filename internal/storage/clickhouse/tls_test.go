package clickhouse_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

func tlsClickHouse(addr, caFile string) config.ClickHouse {
	return config.ClickHouse{
		Addr: []string{addr}, Database: "default", Username: "default", Timeout: 2 * time.Second,
		MaxOpenConns: 1, MaxIdleConns: 1, TLSEnabled: true, TLSCAFile: caFile,
	}
}

// TestClickHouseDialsOverTLSWhenEnabled: the CDR carries MSISDNs and sealed bodies, so with
// CLICKHOUSE_TLS_ENABLED the connection completes a handshake with a server signed by the configured CA.
func TestClickHouseDialsOverTLSWhenEnabled(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr, handshakes := ca.HandshakePeer(t)
	conn, err := clickhouse.NewConn(tlsClickHouse(addr, ca.CAFile))
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = conn.Ping(ctx)
	select {
	case <-handshakes:
	case <-time.After(3 * time.Second):
		t.Fatal("no TLS handshake reached the server")
	}
}

// TestAnUnreadableClickHouseCAIsABootError: the CA is read when the connection is built, not at the first dial.
func TestAnUnreadableClickHouseCAIsABootError(t *testing.T) {
	if conn, err := clickhouse.NewConn(tlsClickHouse("localhost:9440", filepath.Join(t.TempDir(), "absent.crt"))); err == nil {
		_ = conn.Close()
		t.Error("NewConn accepted a CA file that does not exist")
	}
}

// TestTheClickHouseMigratorDialsOverTLSWhenEnabled: the migration Job reaches the same server as the services,
// with the password on the wire, so it follows the same CLICKHOUSE_TLS_ settings.
func TestTheClickHouseMigratorDialsOverTLSWhenEnabled(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr, handshakes := ca.HandshakePeer(t)
	if m, err := clickhouse.NewMigrator(tlsClickHouse(addr, ca.CAFile), t.TempDir(), slog.New(slog.DiscardHandler)); err == nil {
		_ = m.Close()
	}
	select {
	case <-handshakes:
	case <-time.After(3 * time.Second):
		t.Fatal("no TLS handshake reached the server")
	}
}

// TestClickHouseRefusesAServerOfAnotherAuthority: the dial verifies the server, so a skipped verification on
// this path could not pass the handshake tests above unnoticed.
func TestClickHouseRefusesAServerOfAnotherAuthority(t *testing.T) {
	addr, handshakes := tlstest.NewCA(t).HandshakePeer(t)
	conn, err := clickhouse.NewConn(tlsClickHouse(addr, tlstest.NewCA(t).CAFile))
	if err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Ping(ctx)
	select {
	case <-handshakes:
		t.Fatal("a server signed by another authority completed a handshake")
	default:
	}
}
