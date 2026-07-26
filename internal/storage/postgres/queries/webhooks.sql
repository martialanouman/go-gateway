-- name: GetWebhook :one
-- The account's webhook for an event type (mo|dlr). One row per (account_id, event_type) — the unique
-- key — so this returns at most one; no rows means the account has no webhook for that event.
SELECT * FROM control_plane.webhooks
WHERE account_id = @account_id AND event_type = @event_type;
