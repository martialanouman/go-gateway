-- name: GetRateLimit :one
-- Throughput limits for one entity (smpp_account/connector/route). A missing row means "no explicit
-- limit configured" -> the caller treats it as absent, not an error. rate_limits_uq guarantees :one.
SELECT max_per_sec, max_per_day, burst_capacity
FROM control_plane.rate_limits
WHERE entity_type = @entity_type AND entity_id = @entity_id;
