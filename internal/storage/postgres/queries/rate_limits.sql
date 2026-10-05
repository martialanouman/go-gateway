-- name: GetRateLimit :one
-- Throughput limits for one entity (smpp_account/connector). A missing row means "no explicit
-- limit configured" -> the caller treats it as absent, not an error. rate_limits_uq guarantees :one.
SELECT max_per_sec, max_per_day, burst_capacity
FROM control_plane.rate_limits
WHERE entity_type = @entity_type AND entity_id = @entity_id;

-- name: ListRateLimits :many
-- Every configured throughput limit, for the cold-loaded snapshot (step-283): admission and the send
-- resolve an account/connector limit by (entity_type, entity_id) without a per-message read. A sender ID's
-- row also carries the (customer, address) admission keys it by, since a submission names no sender id.
SELECT rl.entity_type, rl.entity_id, rl.max_per_sec, rl.max_per_day, rl.burst_capacity,
       s.customer_id AS sender_customer_id, s.address AS sender_address
FROM control_plane.rate_limits rl
LEFT JOIN control_plane.sender_ids s ON rl.entity_type = 'sender_id' AND s.id = rl.entity_id
ORDER BY rl.entity_type, rl.entity_id;
