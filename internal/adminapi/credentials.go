package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// CredentialDTO is the masked wire form of a credential (contract schema Credential). It carries no
// secret, by construction. It is exported because credentialWithSecretDTO embeds it, and Huma's
// $schema transformer drops an embedded UNexported type at runtime.
type CredentialDTO struct {
	ID             string     `json:"id" format:"uuid"`
	AccountID      string     `json:"account_id" format:"uuid"`
	Type           string     `json:"type" enum:"smpp_bind,api_key"`
	SystemID       *string    `json:"system_id,omitempty" nullable:"true"`
	Status         string     `json:"status" enum:"active,disabled,revoked"`
	LastUsedAt     *time.Time `json:"last_used_at,omitempty" format:"date-time" nullable:"true"`
	GraceExpiresAt *time.Time `json:"grace_expires_at,omitempty" format:"date-time" nullable:"true"`
	CreatedAt      time.Time  `json:"created_at" format:"date-time"`
	RotatedAt      *time.Time `json:"rotated_at,omitempty" format:"date-time" nullable:"true"`
}

func toCredentialDTO(c cp.Credential) CredentialDTO {
	return CredentialDTO{
		ID:             idString(c.ID),
		AccountID:      idString(c.AccountID),
		Type:           string(c.Type),
		SystemID:       c.SystemID,
		Status:         string(c.Status),
		LastUsedAt:     c.LastUsedAt,
		GraceExpiresAt: c.GraceExpiresAt,
		CreatedAt:      c.CreatedAt,
		RotatedAt:      c.RotatedAt,
	}
}

// credentialWithSecretDTO is CredentialWithSecret: the masked view plus the one-time secret. It is
// returned at creation and rotation only; the secret is never stored and cannot be retrieved again.
type credentialWithSecretDTO struct {
	CredentialDTO
	Secret string `json:"secret" doc:"Returned once, at creation and rotation. Not stored and not retrievable again."`
}

type credentialHandlers struct {
	creds    CredentialStore
	accounts AccountStore
}

func registerCredentials(api huma.API, creds CredentialStore, accounts AccountStore) {
	h := &credentialHandlers{creds: creds, accounts: accounts}

	register(api, huma.Operation{
		OperationID: "list-credentials", Method: http.MethodGet, Path: "/admin/smpp-accounts/{id}/credentials",
		Summary: "List an account's credentials", Tags: []string{"Credentials"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-credential", Method: http.MethodPost, Path: "/admin/smpp-accounts/{id}/credentials",
		DefaultStatus: http.StatusCreated,
		Summary:       "Issue a credential (secret returned once)", Tags: []string{"Credentials"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-credential-status", Method: http.MethodPatch,
		Path:    "/admin/smpp-accounts/{id}/credentials/{credId}",
		Summary: "Update a credential's status", Tags: []string{"Credentials"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.updateStatus)

	register(api, huma.Operation{
		OperationID: "revoke-credential", Method: http.MethodDelete,
		Path:          "/admin/smpp-accounts/{id}/credentials/{credId}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Revoke a credential (row is kept, status set to revoked)", Tags: []string{"Credentials"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.revoke)

	register(api, huma.Operation{
		OperationID: "rotate-credential", Method: http.MethodPost,
		Path:    "/admin/smpp-accounts/{id}/credentials/{credId}/rotate",
		Summary: "Rotate a credential (new secret returned once)", Tags: []string{"Credentials"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.rotate)
}

type listCredentialsInput struct {
	ID string `path:"id" format:"uuid"`
}
type listCredentialsOutput struct {
	Body []CredentialDTO
}

func (h *credentialHandlers) list(ctx context.Context, in *listCredentialsInput) (*listCredentialsOutput, error) {
	accountID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	// list has no credId to miss, so a 404 means the account itself is unknown.
	if _, err := h.accounts.Get(ctx, accountID); err != nil {
		return nil, humaerr.FromError(err)
	}
	creds, err := h.creds.ListByAccount(ctx, accountID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listCredentialsOutput{Body: make([]CredentialDTO, 0, len(creds))}
	for _, c := range creds {
		out.Body = append(out.Body, toCredentialDTO(c))
	}
	return out, nil
}

type createCredentialBody struct {
	Type     string  `json:"type" enum:"smpp_bind,api_key"`
	SystemID *string `json:"system_id,omitempty" nullable:"true" doc:"Required for smpp_bind."`
}
type createCredentialInput struct {
	ID   string `path:"id" format:"uuid"`
	Body createCredentialBody
}
type credentialWithSecretOutput struct {
	Body credentialWithSecretDTO
}

func (h *credentialHandlers) create(ctx context.Context, in *createCredentialInput) (*credentialWithSecretOutput, error) {
	accountID, err := uuid.Parse(in.ID)
	if err != nil {
		// An unknown account surfaces as a foreign-key validation (422), matching the contract.
		return nil, humaerr.Fail(errs.ErrValidation, "invalid account id")
	}

	newCred, secret, err := buildCredential(accountID, cp.CredentialType(in.Body.Type), in.Body.SystemID)
	if err != nil {
		return nil, err
	}

	created, err := h.creds.Create(ctx, newCred)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &credentialWithSecretOutput{Body: credentialWithSecretDTO{
		CredentialDTO: toCredentialDTO(created),
		Secret:        secret,
	}}, nil
}

// buildCredential generates and hashes the secret for a new credential of the given type, returning
// the NewCredential to persist and the plaintext secret to return once.
func buildCredential(accountID uuid.UUID, credType cp.CredentialType, systemID *string) (cp.NewCredential, string, error) {
	switch credType {
	case cp.CredentialSMPPBind:
		if systemID == nil || *systemID == "" {
			return cp.NewCredential{}, "", humaerr.FailValidation("system_id is required for an smpp_bind credential",
				humaerr.FieldError{Field: "system_id", Message: "required when type is smpp_bind"})
		}
		password, hash, err := credential.GenerateBindPassword()
		if err != nil {
			return cp.NewCredential{}, "", humaerr.Fail(errs.ErrInternal, "generate bind password")
		}
		return cp.NewCredential{
			AccountID:    accountID,
			Type:         cp.CredentialSMPPBind,
			SystemID:     systemID,
			PasswordHash: &hash,
		}, password, nil
	case cp.CredentialAPIKey:
		key, hash, err := credential.GenerateAPIKey()
		if err != nil {
			return cp.NewCredential{}, "", humaerr.Fail(errs.ErrInternal, "generate api key")
		}
		return cp.NewCredential{
			AccountID:  accountID,
			Type:       cp.CredentialAPIKey,
			APIKeyHash: &hash,
		}, key, nil
	default:
		return cp.NewCredential{}, "", humaerr.FailValidation("unknown credential type",
			humaerr.FieldError{Field: "type", Message: "must be smpp_bind or api_key"})
	}
}

type updateCredentialStatusBody struct {
	Status string `json:"status" enum:"active,disabled,revoked"`
}
type updateCredentialStatusInput struct {
	ID     string `path:"id" format:"uuid"`
	CredID string `path:"credId" format:"uuid"`
	Body   updateCredentialStatusBody
}
type credentialOutput struct {
	Body CredentialDTO
}

func (h *credentialHandlers) updateStatus(ctx context.Context, in *updateCredentialStatusInput) (*credentialOutput, error) {
	accountID, credID, err := parseAccountAndCred(in.ID, in.CredID)
	if err != nil {
		return nil, err
	}
	c, err := h.creds.SetStatus(ctx, accountID, credID, cp.CredentialStatus(in.Body.Status))
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &credentialOutput{Body: toCredentialDTO(c)}, nil
}

type credentialIDInput struct {
	ID     string `path:"id" format:"uuid"`
	CredID string `path:"credId" format:"uuid"`
}

func (h *credentialHandlers) revoke(ctx context.Context, in *credentialIDInput) (*deleteOutput, error) {
	accountID, credID, err := parseAccountAndCred(in.ID, in.CredID)
	if err != nil {
		return nil, err
	}
	// Revoke keeps the row and flips the status; the slot stays occupied.
	if _, err := h.creds.SetStatus(ctx, accountID, credID, cp.CredentialRevoked); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

type rotateCredentialBody struct {
	GracePeriodSec *int `json:"grace_period_sec,omitempty" minimum:"0" nullable:"true" doc:"Old secret stays valid this long in parallel."`
}
type rotateCredentialInput struct {
	ID     string `path:"id" format:"uuid"`
	CredID string `path:"credId" format:"uuid"`
	Body   *rotateCredentialBody
}

func (h *credentialHandlers) rotate(ctx context.Context, in *rotateCredentialInput) (*credentialWithSecretOutput, error) {
	accountID, credID, err := parseAccountAndCred(in.ID, in.CredID)
	if err != nil {
		return nil, err
	}

	// The type decides which kind of secret to mint, so read the credential first.
	existing, err := h.creds.Get(ctx, accountID, credID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	minted, secret, err := buildCredential(accountID, existing.Type, existing.SystemID)
	if err != nil {
		return nil, err
	}

	rot := cp.CredentialRotation{NewHash: hashOf(minted)}
	if in.Body != nil && in.Body.GracePeriodSec != nil {
		grace := time.Duration(*in.Body.GracePeriodSec) * time.Second
		rot.Grace = &grace
	}

	rotated, err := h.creds.Rotate(ctx, accountID, credID, rot)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &credentialWithSecretOutput{Body: credentialWithSecretDTO{
		CredentialDTO: toCredentialDTO(rotated),
		Secret:        secret,
	}}, nil
}

// hashOf returns the storable hash a freshly built credential carries, whichever column applies.
func hashOf(nc cp.NewCredential) string {
	if nc.APIKeyHash != nil {
		return *nc.APIKeyHash
	}
	if nc.PasswordHash != nil {
		return *nc.PasswordHash
	}
	return ""
}

func parseAccountAndCred(accountIDStr, credIDStr string) (uuid.UUID, uuid.UUID, error) {
	accountID, err := uuid.Parse(accountIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("credential")
	}
	credID, err := uuid.Parse(credIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("credential")
	}
	return accountID, credID, nil
}
