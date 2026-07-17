-- name: CreateConnector :one
-- Only the columns ConnectorCreate settles are set here; the SMPP wire-parameter block and the
-- reconnect tuning knobs take their DDL defaults. A duplicate name violates the inline UNIQUE on
-- name -> 409.
INSERT INTO control_plane.smsc_connectors (
    name, host, port, bind_type, system_id, password_hash, vendor_profile,
    interface_version, data_coding_default, window_size, bind_pool_size,
    throughput_limit_per_sec, tls_enabled, tls_config_json, priority_tier, auto_reconnect_enabled
) VALUES (
    @name, @host, @port, @bind_type, @system_id, @password_hash, sqlc.narg('vendor_profile'),
    COALESCE(sqlc.narg('interface_version')::smallint, 52),
    sqlc.narg('data_coding_default'),
    COALESCE(sqlc.narg('window_size')::integer, 10),
    COALESCE(sqlc.narg('bind_pool_size')::integer, 1),
    sqlc.narg('throughput_limit_per_sec'),
    COALESCE(sqlc.narg('tls_enabled')::boolean, false),
    sqlc.narg('tls_config_json'),
    COALESCE(sqlc.narg('priority_tier')::integer, 0),
    COALESCE(sqlc.narg('auto_reconnect_enabled')::boolean, false)
)
RETURNING *;

-- name: GetConnector :one
SELECT * FROM control_plane.smsc_connectors WHERE id = @id;

-- name: ListConnectors :many
SELECT * FROM control_plane.smsc_connectors ORDER BY name;

-- name: UpdateConnector :one
UPDATE control_plane.smsc_connectors SET
    name                     = COALESCE(sqlc.narg('name'), name),
    host                     = COALESCE(sqlc.narg('host'), host),
    port                     = COALESCE(sqlc.narg('port'), port),
    bind_type                = COALESCE(sqlc.narg('bind_type'), bind_type),
    system_id                = COALESCE(sqlc.narg('system_id'), system_id),
    password_hash            = COALESCE(sqlc.narg('password_hash'), password_hash),
    vendor_profile           = COALESCE(sqlc.narg('vendor_profile'), vendor_profile),
    data_coding_default      = COALESCE(sqlc.narg('data_coding_default'), data_coding_default),
    window_size              = COALESCE(sqlc.narg('window_size'), window_size),
    throughput_limit_per_sec = COALESCE(sqlc.narg('throughput_limit_per_sec'), throughput_limit_per_sec),
    tls_enabled              = COALESCE(sqlc.narg('tls_enabled'), tls_enabled),
    tls_config_json          = COALESCE(sqlc.narg('tls_config_json'), tls_config_json),
    priority_tier            = COALESCE(sqlc.narg('priority_tier'), priority_tier),
    status                   = COALESCE(sqlc.narg('status'), status)
WHERE id = @id
RETURNING *;

-- name: DeleteConnector :execrows
DELETE FROM control_plane.smsc_connectors WHERE id = @id;
