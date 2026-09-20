-- name: GetActiveWebhook :one
-- The webhook a MO or DLR should be delivered to. One row per (account_id, event_type) — the unique
-- key — so this returns at most one; no rows means the account has no webhook for that event, or has
-- switched it off.
--
-- The status filter lives HERE rather than in the callers: both delivery paths ask this same question
-- and one of them had forgotten to, so a disabled webhook kept being retried off the retry topic for
-- as long as its attempt budget allowed. The Admin CRUD below reads by account and by id instead, and
-- does see disabled rows — which is the whole point of being able to switch one back on.
SELECT * FROM control_plane.webhooks
WHERE account_id = @account_id AND event_type = @event_type AND status = 'active';

-- name: ListWebhooksByAccount :many
-- Not paginated: at most two rows per account (one per event type), bounded by webhooks_uq.
SELECT * FROM control_plane.webhooks
WHERE account_id = @account_id
ORDER BY event_type;

-- name: GetWebhookByID :one
-- Scoped by account as well as by id: the path carries both, and a webhookId belonging to another
-- account must read as absent rather than as someone else's row.
SELECT * FROM control_plane.webhooks
WHERE id = @id AND account_id = @account_id;

-- name: CreateWebhook :one
-- status falls back to the DDL default ('active'), retry_policy_json to '{}'. A second webhook for an
-- event type the account already subscribes to hits webhooks_uq and comes back as a conflict.
INSERT INTO control_plane.webhooks (account_id, event_type, url, secret, retry_policy_json)
VALUES (@account_id, @event_type, @url, @secret, COALESCE(sqlc.narg('retry_policy_json')::jsonb, '{}'::jsonb))
RETURNING *;

-- name: UpdateWebhook :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE). event_type is absent on
-- purpose — it is the identity of the subscription, not a setting. updated_at is set by the
-- webhooks_touch trigger. Consequence of COALESCE: retry_policy_json cannot be reset to '{}' through
-- this path (debts/patch-null-ne-peut-pas-effacer-un-champ.md).
UPDATE control_plane.webhooks SET
    url               = COALESCE(sqlc.narg('url'), url),
    secret            = COALESCE(sqlc.narg('secret'), secret),
    retry_policy_json = COALESCE(sqlc.narg('retry_policy_json'), retry_policy_json),
    status            = COALESCE(sqlc.narg('status'), status)
WHERE id = @id AND account_id = @account_id
RETURNING *;

-- name: DeleteWebhook :execrows
-- Deleting the configuration stops future deliveries; it never touches the events already delivered
-- or parked. 0 rows means the id is unknown to this account.
DELETE FROM control_plane.webhooks WHERE id = @id AND account_id = @account_id;
