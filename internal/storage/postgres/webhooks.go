package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// WebhookRepo is the webhooks repository: the delivery paths read one webhook by event type, the Admin
// surface administers them by account and id.
type WebhookRepo struct {
	q *sqlcgen.Queries
}

// NewWebhookRepo returns the webhook repository backed by pool.
func NewWebhookRepo(pool *pgxpool.Pool) *WebhookRepo {
	return &WebhookRepo{q: sqlcgen.New(pool)}
}

// Get returns the account's webhook for an event type, disabled ones included — the caller decides what
// an inactive target means for it. found is false (with a nil error) when the account has no webhook for
// that event: a normal absence, not a failure.
func (r *WebhookRepo) Get(ctx context.Context, accountID uuid.UUID, eventType cp.WebhookEventType) (cp.Webhook, bool, error) {
	row, err := r.q.GetWebhook(ctx, sqlcgen.GetWebhookParams{
		AccountID: accountID,
		EventType: string(eventType),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return cp.Webhook{}, false, nil
		}
		return cp.Webhook{}, false, translate("get webhook", err)
	}
	return webhookFromRow(row), true, nil
}

// List returns the account's webhooks, disabled ones included, ordered by event type.
func (r *WebhookRepo) List(ctx context.Context, accountID uuid.UUID) ([]cp.Webhook, error) {
	rows, err := r.q.ListWebhooksByAccount(ctx, accountID)
	if err != nil {
		return nil, translate("list webhooks", err)
	}
	out := make([]cp.Webhook, 0, len(rows))
	for _, row := range rows {
		out = append(out, webhookFromRow(row))
	}
	return out, nil
}

// Create inserts a webhook. A second one for an event type the account already subscribes to hits
// webhooks_uq and comes back as ErrConflict.
func (r *WebhookRepo) Create(ctx context.Context, in cp.NewWebhook) (cp.Webhook, error) {
	row, err := r.q.CreateWebhook(ctx, sqlcgen.CreateWebhookParams{
		AccountID:       in.AccountID,
		EventType:       string(in.EventType),
		Url:             in.URL,
		SecretSealed:    in.Secret.Sealed,
		SecretKmsKeyRef: in.Secret.KMSKeyRef,
		RetryPolicyJson: in.RetryPolicyJSON,
	})
	if err != nil {
		return cp.Webhook{}, translate("create webhook", err)
	}
	return webhookFromRow(row), nil
}

// Update applies a partial change and returns the updated webhook, or ErrNotFound.
func (r *WebhookRepo) Update(ctx context.Context, accountID, id uuid.UUID, p cp.WebhookPatch) (cp.Webhook, error) {
	sealed, keyRef := sealedPair(p.Secret)
	row, err := r.q.UpdateWebhook(ctx, sqlcgen.UpdateWebhookParams{
		ID:              id,
		AccountID:       accountID,
		Url:             p.URL,
		SecretSealed:    sealed,
		SecretKmsKeyRef: keyRef,
		RetryPolicyJson: p.RetryPolicyJSON,
		Status:          strPtr(p.Status),
	})
	if err != nil {
		return cp.Webhook{}, translate("update webhook", err)
	}
	return webhookFromRow(row), nil
}

// Delete removes a webhook, or reports ErrNotFound when nothing matched.
func (r *WebhookRepo) Delete(ctx context.Context, accountID, id uuid.UUID) error {
	n, err := r.q.DeleteWebhook(ctx, sqlcgen.DeleteWebhookParams{ID: id, AccountID: accountID})
	if err != nil {
		return translate("delete webhook", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func webhookFromRow(row sqlcgen.ControlPlaneWebhook) cp.Webhook {
	return cp.Webhook{
		ID:              row.ID,
		AccountID:       row.AccountID,
		EventType:       cp.WebhookEventType(row.EventType),
		URL:             row.Url,
		Secret:          cp.SealedSecret{Sealed: row.SecretSealed, KMSKeyRef: row.SecretKmsKeyRef},
		RetryPolicyJSON: json.RawMessage(row.RetryPolicyJson),
		Status:          cp.WebhookStatus(row.Status),
		CreatedAt:       tsVal(row.CreatedAt),
		UpdatedAt:       tsVal(row.UpdatedAt),
	}
}
