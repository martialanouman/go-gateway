package main

import (
	"context"
	"testing"

	"github.com/google/uuid"

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
	two := 2
	conn, err := repo.Create(ctx, cp.NewConnector{
		Name: "smsc-priority-" + uuid.NewString(), Host: "smsc.example", Port: 2775, BindType: cp.BindTRX, SystemID: "sys",
		Password: cp.SealedSecret{Sealed: []byte{1}, KMSKeyRef: "test/v1"}, PriorityFlagDefault: &two,
	})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}

	live, err := connectorConfigSource{repo: repo}.Load(ctx, conn.ID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if live.PriorityFlagDefault != 2 {
		t.Errorf("PriorityFlagDefault = %d, want 2", live.PriorityFlagDefault)
	}
}
