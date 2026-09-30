-- name: GetRateLimit :one
-- Throughput limits for one entity (smpp_account/connector). A missing row means "no explicit
-- limit configured" -> the caller treats it as absent, not an error. rate_limits_uq guarantees :one.
SELECT max_per_sec, max_per_day, burst_capacity
FROM control_plane.rate_limits
WHERE entity_type = @entity_type AND entity_id = @entity_id;

-- name: ListRateLimits :many
-- Every configured throughput limit, for the cold-loaded snapshot (step-283): admission and the send
-- resolve an account/connector limit by (entity_type, entity_id) without a per-message read.
SELECT entity_type, entity_id, max_per_sec, max_per_day, burst_capacity
FROM control_plane.rate_limits
ORDER BY entity_type, entity_id;
