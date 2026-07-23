package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// BindRepo resolves a presented SMPP system_id to its bind credential. It is the read side of SMPP
// bind authentication (§1.9): the system_id is looked up on the partial unique index over live bind
// credentials, and the argon2id password is verified by the caller (internal/credential), not in SQL.
type BindRepo struct {
	q *sqlcgen.Queries
}

// NewBindRepo returns the SMPP bind-credential repository backed by pool.
func NewBindRepo(pool *pgxpool.Pool) *BindRepo {
	return &BindRepo{q: sqlcgen.New(pool)}
}

// BindCredentialBySystemID returns the bind credential behind a system_id. found is false when no
// live bind credential matches — an unknown or revoked system_id — which the caller maps to
// ESME_RINVPASWD (deliberately not distinguishing an unknown system_id from a wrong password, so a
// bind cannot enumerate valid system_ids). A row is returned even when the channel is disabled or the
// account is suspended, so the caller can answer ESME_RBINDFAIL for those rather than a blanket miss.
// A credential whose password_hash is unexpectedly null (a shape the DDL forbids for smpp_bind) is
// treated as not found rather than trusted. A genuine database error is returned as-is.
//
// The rotation grace columns travel raw (previous_secret_hash, grace_expires_at): an argon2id hash
// cannot be compared in SQL, so the deadline is enforced by the caller against its own clock rather
// than by the query's now() — which is what makes the post-grace cut-off testable without Postgres.
func (r *BindRepo) BindCredentialBySystemID(ctx context.Context, systemID string) (cp.BindCredential, bool, error) {
	row, err := r.q.GetBindPrincipal(ctx, &systemID)
	if errors.Is(err, pgx.ErrNoRows) {
		return cp.BindCredential{}, false, nil
	}
	if err != nil {
		return cp.BindCredential{}, false, fmt.Errorf("lookup bind principal: %w", err)
	}
	if row.PasswordHash == nil {
		return cp.BindCredential{}, false, nil
	}
	return cp.BindCredential{
		AccountID:          row.AccountID,
		CustomerID:         row.CustomerID,
		PasswordHash:       *row.PasswordHash,
		PreviousSecretHash: row.PreviousSecretHash,
		GraceExpiresAt:     tsPtr(row.GraceExpiresAt),
		CredentialStatus:   cp.CredentialStatus(row.CredentialStatus),
		SMPPEnabled:        row.SmppEnabled,
		AllowedBindType:    cp.BindType(row.AllowedBindTypes),
		MaxSessions:        row.MaxSessions,
		QuerySMEnabled:     row.QuerySmEnabled,
		CancelSMEnabled:    row.CancelSmEnabled,
		AccountStatus:      cp.AccountStatus(row.AccountStatus),
		CustomerStatus:     cp.CustomerStatus(row.CustomerStatus),
	}, true, nil
}
