package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// CredentialRepo is the credentials repository. It satisfies adminapi.CredentialStore structurally.
// It never returns a secret: the domain Credential has no secret field, so a read cannot leak one.
type CredentialRepo struct {
	q *sqlcgen.Queries
}

// NewCredentialRepo returns the credentials repository backed by pool.
func NewCredentialRepo(pool *pgxpool.Pool) *CredentialRepo {
	return &CredentialRepo{q: sqlcgen.New(pool)}
}

// Create inserts a credential whose secret has already been hashed by the caller. A second
// credential of the same type on the account violates credentials_one_per_type_uq, which
// translate() reports as a conflict (409).
func (r *CredentialRepo) Create(ctx context.Context, in cp.NewCredential) (cp.Credential, error) {
	row, err := r.q.CreateCredential(ctx, sqlcgen.CreateCredentialParams{
		AccountID:    in.AccountID,
		Type:         string(in.Type),
		SystemID:     in.SystemID,
		PasswordHash: in.PasswordHash,
		ApiKeyHash:   in.APIKeyHash,
	})
	if err != nil {
		return cp.Credential{}, translate("create credential", err)
	}
	return credentialFromRow(row), nil
}

// ListByAccount returns an account's credentials (at most one bind and one key).
func (r *CredentialRepo) ListByAccount(ctx context.Context, accountID uuid.UUID) ([]cp.Credential, error) {
	rows, err := r.q.ListCredentialsByAccount(ctx, accountID)
	if err != nil {
		return nil, translate("list credentials", err)
	}
	out := make([]cp.Credential, 0, len(rows))
	for _, row := range rows {
		out = append(out, credentialFromRow(row))
	}
	return out, nil
}

// Get returns one credential of an account, or ErrNotFound.
func (r *CredentialRepo) Get(ctx context.Context, accountID, credID uuid.UUID) (cp.Credential, error) {
	row, err := r.q.GetCredential(ctx, sqlcgen.GetCredentialParams{AccountID: accountID, ID: credID})
	if err != nil {
		return cp.Credential{}, translate("get credential", err)
	}
	return credentialFromRow(row), nil
}

// SetStatus changes a credential's status. Revoking flips the status to revoked and keeps the row:
// the slot stays occupied (credentials_one_per_type_uq), so re-creating that type conflicts and the
// normal path to a new secret is Rotate.
func (r *CredentialRepo) SetStatus(ctx context.Context, accountID, credID uuid.UUID, s cp.CredentialStatus) (cp.Credential, error) {
	row, err := r.q.SetCredentialStatus(ctx, sqlcgen.SetCredentialStatusParams{
		AccountID: accountID,
		ID:        credID,
		Status:    string(s),
	})
	if err != nil {
		return cp.Credential{}, translate("set credential status", err)
	}
	return credentialFromRow(row), nil
}

// Rotate writes a new hashed secret into the column matching the credential's type, optionally
// keeping the previous hash valid for a grace window.
func (r *CredentialRepo) Rotate(ctx context.Context, accountID, credID uuid.UUID, rot cp.CredentialRotation) (cp.Credential, error) {
	var graceSeconds *int32
	if rot.Grace != nil {
		secs := int32(rot.Grace.Seconds()) //nolint:gosec // G115: grace is a small operator-set window.
		graceSeconds = &secs
	}
	newHash := rot.NewHash
	row, err := r.q.RotateCredential(ctx, sqlcgen.RotateCredentialParams{
		AccountID:    accountID,
		ID:           credID,
		NewHash:      &newHash,
		GraceSeconds: graceSeconds,
	})
	if err != nil {
		return cp.Credential{}, translate("rotate credential", err)
	}
	return credentialFromRow(row), nil
}

func credentialFromRow(row sqlcgen.ControlPlaneCredential) cp.Credential {
	return cp.Credential{
		ID:             row.ID,
		AccountID:      row.AccountID,
		Type:           cp.CredentialType(row.Type),
		SystemID:       row.SystemID,
		Status:         cp.CredentialStatus(row.Status),
		LastUsedAt:     tsPtr(row.LastUsedAt),
		GraceExpiresAt: tsPtr(row.GraceExpiresAt),
		CreatedAt:      tsVal(row.CreatedAt),
		RotatedAt:      tsPtr(row.RotatedAt),
	}
}
