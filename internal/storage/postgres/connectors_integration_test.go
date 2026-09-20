package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

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
		Password:      cp.SealedSecret{Sealed: []byte{0x01, 0x00, 0xff, 0x7f, 0x00}, KMSKeyRef: "test/v1"},
		TLSConfigJSON: map[string]any{"verify": true, "min_version": "1.2"},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// step-295: the column used to hold an argon2id hash, which no bind_transceiver PDU can carry. What
	// goes in now must come back byte for byte — embedded NULs and high bytes included, since a bytea
	// round trip that mangled one byte would leave a ciphertext that no longer opens.
	want := cp.SealedSecret{Sealed: []byte{0x01, 0x00, 0xff, 0x7f, 0x00}, KMSKeyRef: "test/v1"}
	if !bytes.Equal(created.Password.Sealed, want.Sealed) {
		t.Errorf("password_sealed came back %x, want %x", created.Password.Sealed, want.Sealed)
	}
	if created.Password.KMSKeyRef != want.KMSKeyRef {
		t.Errorf("password_kms_key_ref = %q, want %q", created.Password.KMSKeyRef, want.KMSKeyRef)
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

	base := cp.NewConnector{Name: "smsc-dup", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s", Password: cp.SealedSecret{Sealed: []byte("hash"), KMSKeyRef: "test/v1"}}
	if _, err := repo.Create(ctx, base); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := repo.Create(ctx, base)
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("duplicate-name Create code = %q, want conflict", code)
	}
}

// TestConnectorRepoUpdateReconnectPolicyAndBindPool: the step-128 dedicated updates persist the
// reconnect knobs (incl. the numeric multiplier) and bind_pool_size, leaving other fields untouched.
func TestConnectorRepoUpdateReconnectPolicyAndBindPool(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()

	c, err := repo.Create(ctx, cp.NewConnector{
		Name: "smsc-reconf", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s", Password: cp.SealedSecret{Sealed: []byte("hash"), KMSKeyRef: "test/v1"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	delay, mult, attempts := 2500, 1.75, 9
	got, err := repo.UpdateReconnectPolicy(ctx, c.ID, cp.ReconnectPolicy{
		AutoReconnectEnabled: true, InitialDelayMs: &delay, Multiplier: &mult, MaxAttempts: &attempts,
	})
	if err != nil {
		t.Fatalf("UpdateReconnectPolicy: %v", err)
	}
	if !got.AutoReconnectEnabled || got.ReconnectInitialDelayMs != 2500 || got.ReconnectMultiplier != 1.75 || got.ReconnectMaxAttempts != 9 {
		t.Errorf("reconnect policy = %+v, want enabled / delay 2500 / multiplier 1.75 / attempts 9", got)
	}
	// An unspecified knob keeps its stored value (jitter default 20).
	if got.ReconnectJitterPct != 20 {
		t.Errorf("reconnect_jitter_pct = %d, want the untouched default 20", got.ReconnectJitterPct)
	}

	got, err = repo.UpdateBindPool(ctx, c.ID, 4)
	if err != nil {
		t.Fatalf("UpdateBindPool: %v", err)
	}
	if got.BindPoolSize != 4 {
		t.Errorf("bind_pool_size = %d, want 4", got.BindPoolSize)
	}
	// The reconnect policy set above survived the bind-pool update.
	if !got.AutoReconnectEnabled || got.ReconnectMaxAttempts != 9 {
		t.Errorf("bind-pool update clobbered the reconnect policy: %+v", got)
	}
}

// TestConnectorRepoPilotingUpdatesUnknownAreNotFound: both dedicated updates report ErrNotFound for an
// unknown connector.
func TestConnectorRepoPilotingUpdatesUnknownAreNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()
	if _, err := repo.UpdateBindPool(ctx, uuid.New(), 2); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("UpdateBindPool(unknown) error = %v, want ErrNotFound", err)
	}
	if _, err := repo.UpdateReconnectPolicy(ctx, uuid.New(), cp.ReconnectPolicy{AutoReconnectEnabled: true}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("UpdateReconnectPolicy(unknown) error = %v, want ErrNotFound", err)
	}
}

// A rotation must move the ciphertext and its key reference TOGETHER. The two columns are written through
// separate COALESCE arguments, so nothing in the SQL couples them: leave one behind and the row holds a
// ciphertext from one master key beside a reference naming another — it still opens today, and a future
// KEK rotation skips it, which is the failure that never surfaces until the key it needs is gone.
func TestConnectorRepoRotatesTheSealedPasswordAndItsKeyRefTogether(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewConnectorRepo(pool)
	ctx := context.Background()

	c, err := repo.Create(ctx, cp.NewConnector{
		Name: "smsc-rotate", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s",
		Password: cp.SealedSecret{Sealed: []byte{0xde, 0xad}, KMSKeyRef: "kek-before"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	rotated := cp.SealedSecret{Sealed: []byte{0xbe, 0xef, 0x00, 0xff}, KMSKeyRef: "kek-after"}
	got, err := repo.Update(ctx, c.ID, cp.ConnectorPatch{Password: &rotated})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !bytes.Equal(got.Password.Sealed, rotated.Sealed) {
		t.Errorf("password_sealed = %x, want %x", got.Password.Sealed, rotated.Sealed)
	}
	if got.Password.KMSKeyRef != rotated.KMSKeyRef {
		t.Errorf("password_kms_key_ref = %q, want %q — the ciphertext moved without its key reference",
			got.Password.KMSKeyRef, rotated.KMSKeyRef)
	}

	// A patch that does not carry a password leaves BOTH columns alone.
	name := "smsc-rotate-renamed"
	got, err = repo.Update(ctx, c.ID, cp.ConnectorPatch{Name: &name})
	if err != nil {
		t.Fatalf("Update (name only): %v", err)
	}
	if !bytes.Equal(got.Password.Sealed, rotated.Sealed) || got.Password.KMSKeyRef != rotated.KMSKeyRef {
		t.Errorf("an unrelated patch disturbed the sealed password: %x / %q", got.Password.Sealed, got.Password.KMSKeyRef)
	}
}
