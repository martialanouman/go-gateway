package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestConnectorRepoRoundTripAcrossTypeGaps is the real proof that the smallint / numeric / jsonb
// conversions survive a write-then-read against PostgreSQL: the defaults come back as int/float64,
// and a tls_config_json object round-trips as a map.
func TestConnectorRepoRoundTripAcrossTypeGaps(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()

	created, err := repo.Create(ctx, cp.NewConnector{
		Name:          "smsc-int-test",
		Host:          "smsc.example",
		Port:          2775,
		BindType:      cp.BindTRX,
		SystemID:      "sys",
		PasswordHash:  "hash",
		TLSConfigJSON: map[string]any{"verify": true, "min_version": "1.2"},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// The DDL smallint defaults arrive as int, not int16.
	if created.InterfaceVersion != 52 {
		t.Errorf("interface_version = %d, want 52 (the smallint default)", created.InterfaceVersion)
	}
	if created.SourceAddrTON != 5 {
		t.Errorf("source_addr_ton = %d, want 5 (the smallint default)", created.SourceAddrTON)
	}
	// numeric(4,2) default arrives as float64.
	if created.ReconnectMultiplier != 2.0 {
		t.Errorf("reconnect_multiplier = %v, want 2.0 (the numeric default)", created.ReconnectMultiplier)
	}

	got, err := repo.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	// jsonb round-trips as a map.
	if v, _ := got.TLSConfigJSON["verify"].(bool); !v {
		t.Errorf("tls_config_json = %v, want the verify:true entry to survive", got.TLSConfigJSON)
	}
}

// TestConnectorRepoDuplicateNameConflicts: the inline UNIQUE(name) becomes a conflict (409).
func TestConnectorRepoDuplicateNameConflicts(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()

	base := cp.NewConnector{Name: "smsc-dup", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s", PasswordHash: "hash"}
	if _, err := repo.Create(ctx, base); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := repo.Create(ctx, base)
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("duplicate-name Create code = %q, want conflict", code)
	}
}
