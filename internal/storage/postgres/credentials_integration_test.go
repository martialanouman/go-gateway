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

// TestRotateRevokedCredentialReactivatesItWithoutAGraceWindow: rotating a revoked credential brings it
// back to active with the new secret only. Even when a grace window reaches the store (a revocation
// racing a planned rotation), the revoked secret must not come back for the window's length.
func TestRotateRevokedCredentialReactivatesItWithoutAGraceWindow(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)
	apikeys := postgres.NewAPIKeyRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "ReactivateCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	oldHash, newHash := "old-"+uuid.NewString(), "new-"+uuid.NewString()
	cred, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &oldHash,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialRevoked); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	grace := 10 * time.Minute
	rotated, err := creds.Rotate(ctx, account.ID, cred.ID, cp.CredentialRotation{NewHash: newHash, Grace: &grace})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.Status != cp.CredentialActive {
		t.Errorf("status = %q, want active", rotated.Status)
	}
	if rotated.GraceExpiresAt != nil {
		t.Errorf("grace_expires_at = %v, want nil on a reactivation", *rotated.GraceExpiresAt)
	}
	var prev *string
	if err := pool.QueryRow(ctx,
		`SELECT previous_secret_hash FROM control_plane.credentials WHERE id = $1`, cred.ID).Scan(&prev); err != nil {
		t.Fatalf("read previous_secret_hash: %v", err)
	}
	if prev != nil {
		t.Errorf("previous_secret_hash = %q, want nil: the revoked secret must not survive", *prev)
	}
	if _, found, err := apikeys.PrincipalByAPIKeyHash(ctx, newHash); err != nil || !found {
		t.Errorf("new secret: found=%v err=%v, want it to authenticate", found, err)
	}
	if _, found, err := apikeys.PrincipalByAPIKeyHash(ctx, oldHash); err != nil || found {
		t.Errorf("revoked secret: found=%v err=%v, want it refused", found, err)
	}
}

// TestRevokedCredentialLeavesRevokedOnlyThroughRotate: setting a revoked credential back to active (or
// to disabled, then active) would revive its old, possibly leaked, secret. Only a rotation brings it
// back, with a new one; revoking again stays idempotent.
func TestRevokedCredentialLeavesRevokedOnlyThroughRotate(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "StuckCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	keyHash := "key-" + uuid.NewString()
	cred, err := creds.Create(ctx, cp.NewCredential{AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialRevoked); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	for _, status := range []cp.CredentialStatus{cp.CredentialActive, cp.CredentialDisabled} {
		_, err := creds.SetStatus(ctx, account.ID, cred.ID, status)
		if code, _ := errs.CodeOf(err); code != errs.ErrNotFound {
			t.Errorf("revoked -> %s: code = %q (err=%v), want not_found (no row left revoked)", status, code, err)
		}
	}
	got, err := creds.Get(ctx, account.ID, cred.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != cp.CredentialRevoked {
		t.Errorf("status = %q, want revoked", got.Status)
	}
	if _, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialRevoked); err != nil {
		t.Errorf("revoking twice: %v, want idempotent", err)
	}
}

// TestRotateDisabledCredentialKeepsItDisabled: only a revocation is undone by a rotation; a disabled
// credential is an operator's pause, and a new secret does not lift it.
func TestRotateDisabledCredentialKeepsItDisabled(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "DisabledCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	keyHash := "key-" + uuid.NewString()
	cred, err := creds.Create(ctx, cp.NewCredential{AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &keyHash})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialDisabled); err != nil {
		t.Fatalf("disable: %v", err)
	}

	rotated, err := creds.Rotate(ctx, account.ID, cred.ID, cp.CredentialRotation{NewHash: "new-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated.Status != cp.CredentialDisabled {
		t.Errorf("status = %q, want disabled", rotated.Status)
	}
}

// TestReactivatingABindWhoseSystemIDWasTakenConflicts: credentials_system_id_uq only covers live binds,
// so another account may take a revoked bind's system_id. Reactivating the revoked one then violates the
// index, and that is the client's conflict (409), not a server fault.
func TestReactivatingABindWhoseSystemIDWasTakenConflicts(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "TakenCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	first, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "first"})
	if err != nil {
		t.Fatalf("create account first: %v", err)
	}
	second, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "second"})
	if err != nil {
		t.Fatalf("create account second: %v", err)
	}

	systemID := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	hash := "bind-hash"
	revoked, err := creds.Create(ctx, cp.NewCredential{
		AccountID: first.ID, Type: cp.CredentialSMPPBind, SystemID: &systemID, PasswordHash: &hash,
	})
	if err != nil {
		t.Fatalf("create first bind: %v", err)
	}
	if _, err := creds.SetStatus(ctx, first.ID, revoked.ID, cp.CredentialRevoked); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := creds.Create(ctx, cp.NewCredential{
		AccountID: second.ID, Type: cp.CredentialSMPPBind, SystemID: &systemID, PasswordHash: &hash,
	}); err != nil {
		t.Fatalf("second account takes the system_id: %v", err)
	}

	_, err = creds.Rotate(ctx, first.ID, revoked.ID, cp.CredentialRotation{NewHash: "new-bind-hash"})
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("reactivation code = %q (err=%v), want conflict", code, err)
	}
}
