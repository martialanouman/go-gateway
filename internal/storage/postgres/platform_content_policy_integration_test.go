package postgres_test

import (
	"context"
	"errors"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestPlatformContentPolicyIsOffAfterMigrationAndNeverPlaintext: the migration must not change what an
// inherit customer stores (off, as the constant it replaces), and the database itself refuses a platform
// default in clear — the API's 422 only words it.
func TestPlatformContentPolicyIsOffAfterMigrationAndNeverPlaintext(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()
	t.Cleanup(func() { _, _ = repo.SetPlatformContentStorage(context.Background(), cp.ContentOff) })

	got, err := repo.PlatformContentStorage(ctx)
	if err != nil || got != cp.ContentOff {
		t.Fatalf("platform default after migration = %q, err=%v; want off", got, err)
	}

	if got, err := repo.SetPlatformContentStorage(ctx, cp.ContentStoredEncrypted); err != nil || got != cp.ContentStoredEncrypted {
		t.Fatalf("set stored_encrypted = %q, err=%v", got, err)
	}
	if got, _ := repo.PlatformContentStorage(ctx); got != cp.ContentStoredEncrypted {
		t.Fatalf("read back = %q, want stored_encrypted", got)
	}

	for _, refused := range []cp.ContentStorage{cp.ContentStoredPlaintext, cp.ContentInherit} {
		if _, err := repo.SetPlatformContentStorage(ctx, refused); !errors.Is(err, errs.ErrValidation) {
			t.Errorf("set %q: err=%v, want ErrValidation from the table's CHECK", refused, err)
		}
		if got, _ := repo.PlatformContentStorage(ctx); got != cp.ContentStoredEncrypted {
			t.Fatalf("after refusing %q the default is %q, want stored_encrypted unchanged", refused, got)
		}
	}
}

// TestSettingThePlatformDefaultRestoresAMissingRow: nothing but a constraint-free DELETE can empty the
// table, and when it happens the operator's PATCH must repair it rather than answer an undeclared 404.
// Not parallel: the row is global to the package's database.
func TestSettingThePlatformDefaultRestoresAMissingRow(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()
	t.Cleanup(func() { _, _ = repo.SetPlatformContentStorage(context.Background(), cp.ContentOff) })
	if _, err := pool.Exec(ctx, `DELETE FROM control_plane.platform_content_policy WHERE true`); err != nil {
		t.Fatalf("empty the table: %v", err)
	}

	if got, err := repo.SetPlatformContentStorage(ctx, cp.ContentOff); err != nil || got != cp.ContentOff {
		t.Fatalf("set on an empty table = %q, err=%v; want off", got, err)
	}
	if got, err := repo.PlatformContentStorage(ctx); err != nil || got != cp.ContentOff {
		t.Fatalf("read back = %q, err=%v; want off", got, err)
	}
}
