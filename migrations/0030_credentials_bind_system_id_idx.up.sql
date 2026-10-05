-- step-286b: the bind lookup also resolves a revoked system_id, to attribute its refused binds.
CREATE INDEX credentials_bind_system_id_idx
  ON control_plane.credentials(system_id) WHERE type = 'smpp_bind';
