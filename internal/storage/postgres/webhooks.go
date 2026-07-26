package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// WebhookRepo reads the return-path webhook configuration. It satisfies the webhook sender's resolver
// structurally.
type WebhookRepo struct {
	q *sqlcgen.Queries
}

// NewWebhookRepo returns the webhook repository backed by pool.
func NewWebhookRepo(pool *pgxpool.Pool) *WebhookRepo {
	return &WebhookRepo{q: sqlcgen.New(pool)}
}

// Get returns the account's webhook for an event type. found is false (with a nil error) when the
// account has no webhook for that event — a normal absence, not a failure.
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

func webhookFromRow(row sqlcgen.ControlPlaneWebhook) cp.Webhook {
	return cp.Webhook{
		ID:              row.ID,
		AccountID:       row.AccountID,
		EventType:       cp.WebhookEventType(row.EventType),
		URL:             row.Url,
		Secret:          row.Secret,
		RetryPolicyJSON: json.RawMessage(row.RetryPolicyJson),
		Status:          cp.WebhookStatus(row.Status),
	}
}
