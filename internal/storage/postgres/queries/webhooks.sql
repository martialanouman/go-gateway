-- name: GetWebhook :one
-- The account's webhook for an event type (mo|dlr). One row per (account_id, event_type) — the unique
-- key — so this returns at most one; no rows means the account has no webhook for that event.
--
-- Disabled rows are returned, NOT filtered out here: the two delivery paths both have to know the
-- difference. A first delivery dead-letters either way, but the deferred retry runner drops a deleted
-- webhook's event and parks a disabled one's — switching a webhook off is a pause, not an order to
-- destroy the backlog, and a query that hid the status would take that decision away from it.
SELECT * FROM control_plane.webhooks
WHERE account_id = @account_id AND event_type = @event_type;

-- name: ListWebhooksByAccount :many
-- Not paginated: at most two rows per account (one per event type), bounded by webhooks_uq.
SELECT * FROM control_plane.webhooks
WHERE account_id = @account_id
ORDER BY event_type;

-- name: CreateWebhook :one
-- status falls back to the DDL default ('active'), retry_policy_json to '{}'. A second webhook for an
-- event type the account already subscribes to hits webhooks_uq and comes back as a conflict.
INSERT INTO control_plane.webhooks (account_id, event_type, url, secret_sealed, secret_kms_key_ref, retry_policy_json)
VALUES (@account_id, @event_type, @url, @secret_sealed, @secret_kms_key_ref,
        COALESCE(sqlc.narg('retry_policy_json')::jsonb, '{}'::jsonb))
RETURNING *;

-- name: UpdateWebhook :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE). event_type is absent on
-- purpose — it is the identity of the subscription, not a setting. updated_at is set by the
-- webhooks_touch trigger. retry_policy_json IS resettable here, unlike the nullable columns of
-- debts/patch-null-ne-peut-pas-effacer-un-champ.md: an omitted field arrives as a nil RawMessage (SQL
-- NULL, COALESCE keeps the column) and a supplied {} arrives as two non-nil bytes, which COALESCE takes.
--
-- The two halves of the sealed secret are COALESCE'd on the SAME argument being present or absent, so a
-- rotation cannot write the ciphertext while keeping the reference of the key that sealed the previous one
-- — a row that opens today and stops opening at the first master-key rotation.
UPDATE control_plane.webhooks SET
    url                = COALESCE(sqlc.narg('url'), url),
    secret_sealed      = COALESCE(sqlc.narg('secret_sealed'), secret_sealed),
    secret_kms_key_ref = COALESCE(sqlc.narg('secret_kms_key_ref'), secret_kms_key_ref),
    retry_policy_json  = COALESCE(sqlc.narg('retry_policy_json'), retry_policy_json),
    status             = COALESCE(sqlc.narg('status'), status)
WHERE id = @id AND account_id = @account_id
RETURNING *;

-- name: DeleteWebhook :execrows
-- Deleting the configuration stops future deliveries; it never touches the events already delivered
-- or parked. 0 rows means the id is unknown to this account.
DELETE FROM control_plane.webhooks WHERE id = @id AND account_id = @account_id;
