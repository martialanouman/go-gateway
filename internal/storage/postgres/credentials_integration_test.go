package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestCredentialCardinalityConflicts is the M1 acceptance criterion: a second credential of the
// same type on one account is refused with a conflict (409), enforced by the schema, not by prose.
func TestCredentialCardinalityConflicts(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "CredCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	keyHash := "hash-1"
	if _, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash,
	}); err != nil {
		t.Fatalf("first api_key Create: %v", err)
	}

	// Second api_key on the same account -> credentials_one_per_type_uq -> conflict.
	keyHash2 := "hash-2"
	_, err = creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash2,
	})
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("second api_key code = %q, want conflict", code)
	}
}

// TestRevokeKeepsTheRowAndBlocksRecreation pins decision 2: revoke flips the status and keeps the
// row, so re-creating that type still conflicts. The path to a new secret is rotate, not re-create.
func TestRevokeKeepsTheRowAndBlocksRecreation(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, _ := customers.Create(ctx, cp.NewCustomer{Name: "RevokeCo"})
	account, _ := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})

	keyHash := "hash-1"
	cred, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	revoked, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialRevoked)
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked.Status != cp.CredentialRevoked {
		t.Errorf("status = %q, want revoked", revoked.Status)
	}

	// The revoked row still occupies the type slot -> re-creating that type conflicts.
	keyHash2 := "hash-2"
	_, err = creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash2,
	})
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("re-create after revoke code = %q, want conflict (the row is kept)", code)
	}
}

// TestRotateWithGraceKeepsThePreviousHash: rotating with a grace window records previous_secret_hash
// and grace_expires_at, so the old secret keeps working in parallel.
func TestRotateWithGraceKeepsThePreviousHash(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, _ := customers.Create(ctx, cp.NewCustomer{Name: "RotateCo"})
	account, _ := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})

	oldHash := "old-hash"
	cred, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &oldHash,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	grace := time.Minute * 10
	rotated, err := creds.Rotate(ctx, account.ID, cred.ID, cp.CredentialRotation{NewHash: "new-hash", Grace: &grace})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.GraceExpiresAt == nil {
		t.Error("grace_expires_at is nil after a grace rotation")
	}
	if rotated.RotatedAt == nil {
		t.Error("rotated_at is nil after a rotation")
	}

	// Confirm the previous hash was preserved by reading it directly.
	var prev *string
	err = pool.QueryRow(ctx,
		`SELECT previous_secret_hash FROM control_plane.credentials WHERE id = $1`, cred.ID).Scan(&prev)
	if err != nil {
		t.Fatalf("read previous_secret_hash: %v", err)
	}
	if prev == nil || *prev != oldHash {
		t.Errorf("previous_secret_hash = %v, want the old hash %q", prev, oldHash)
	}
}

// TestBindLookupCarriesTheRotationGraceColumns closes the loop step-027 opens: rotating an smpp_bind
// with a grace window must surface both grace columns on the SMPP authentication read path, or the
// bind has nothing to fall back on and a rotation silently severs every live ESME. It asserts the
// mapping, not the deadline — the cut-off itself is proven in internal/smppserver against an injected
// clock, which no Postgres now() would let us fast-forward.
func TestBindLookupCarriesTheRotationGraceColumns(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)
	binds := postgres.NewBindRepo(pool)

	customer, _ := customers.Create(ctx, cp.NewCustomer{Name: "GraceBindCo"})
	account, _ := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})

	systemID := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	oldHash := "old-bind-hash"
	cred, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialSMPPBind, SystemID: &systemID, PasswordHash: &oldHash,
	})
	if err != nil {
		t.Fatalf("create bind credential: %v", err)
	}

	// Before any rotation the window is absent, so nothing can fall back.
	got, found, err := binds.BindCredentialBySystemID(ctx, systemID)
	if err != nil || !found {
		t.Fatalf("lookup before rotation: found=%v err=%v", found, err)
	}
	if got.PreviousSecretHash != nil || got.GraceExpiresAt != nil {
		t.Errorf("grace columns set before any rotation: prev=%v expiry=%v",
			got.PreviousSecretHash, got.GraceExpiresAt)
	}

	grace := 10 * time.Minute
	const newHash = "new-bind-hash"
	if _, err := creds.Rotate(ctx, account.ID, cred.ID,
		cp.CredentialRotation{NewHash: newHash, Grace: &grace}); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	got, found, err = binds.BindCredentialBySystemID(ctx, systemID)
	if err != nil || !found {
		t.Fatalf("lookup after rotation: found=%v err=%v", found, err)
	}
	if got.PasswordHash != newHash {
		t.Errorf("password_hash = %q, want the new hash %q", got.PasswordHash, newHash)
	}
	if got.PreviousSecretHash == nil || *got.PreviousSecretHash != oldHash {
		t.Errorf("previous_secret_hash = %v, want the old hash %q", got.PreviousSecretHash, oldHash)
	}
	if got.GraceExpiresAt == nil {
		t.Fatal("grace_expires_at is nil on the bind read path after a grace rotation")
	}
	if !got.GraceExpiresAt.After(time.Now()) {
		t.Errorf("grace_expires_at = %v, want a future instant", *got.GraceExpiresAt)
	}

	// An immediate cutover (nil grace) must clear the window, so the superseded secret dies at once.
	if _, err := creds.Rotate(ctx, account.ID, cred.ID,
		cp.CredentialRotation{NewHash: "newer-bind-hash"}); err != nil {
		t.Fatalf("rotate without grace: %v", err)
	}
	got, _, err = binds.BindCredentialBySystemID(ctx, systemID)
	if err != nil {
		t.Fatalf("lookup after cutover: %v", err)
	}
	if got.PreviousSecretHash != nil || got.GraceExpiresAt != nil {
		t.Errorf("grace columns survived an immediate cutover: prev=%v expiry=%v",
			got.PreviousSecretHash, got.GraceExpiresAt)
	}
}
