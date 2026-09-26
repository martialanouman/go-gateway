package clickhouse_test

import (
	"context"
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
