package main

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestConfigSourceLoadsThePriorityFlagDefault: buildSubmit sends the connector's priority_flag_default
// for a message of effective priority 0 (ADR-0020 §3); it reaches the pool only through this load.
func TestConfigSourceLoadsThePriorityFlagDefault(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()
	conn, err := repo.Create(ctx, cp.NewConnector{
		Name: "smsc-priority", Host: "smsc.example", Port: 2775, BindType: cp.BindTRX, SystemID: "sys",
		Password: cp.SealedSecret{Sealed: []byte{1}, KMSKeyRef: "test/v1"},
	})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE control_plane.smsc_connectors SET priority_flag_default = 7 WHERE id = $1`, conn.ID); err != nil {
		t.Fatalf("set priority_flag_default: %v", err)
	}

	live, err := connectorConfigSource{repo: repo}.Load(ctx, conn.ID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The column has no CHECK: an out-of-range value set in SQL is clamped to the SMPP range, not wrapped.
	if live.PriorityFlagDefault != 3 {
		t.Errorf("PriorityFlagDefault = %d, want 3", live.PriorityFlagDefault)
	}
}
