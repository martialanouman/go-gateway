package postgres_test

import (
	"context"
	"testing"
	"time"

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
