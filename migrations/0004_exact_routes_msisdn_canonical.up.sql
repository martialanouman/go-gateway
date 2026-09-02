-- Enforce the canonical E.164-minus-"+" form (digits only, country code 1-9) on exact-route MSISDNs
-- (§6.1). The L0 Bloom snapshot and the exactroute:{msisdn} confirmation key are built on this exact
-- form, and the router queries them with e164.Normalize output (digits only). A write path (admin,
-- MNP/carrier import) storing a non-normalized value ("+225…", padded) would silently defeat every
-- override — a routing false negative. This CHECK makes such a write fail loudly instead.
ALTER TABLE control_plane.exact_routes
  ADD CONSTRAINT exact_routes_msisdn_canonical_ck CHECK (msisdn ~ '^[1-9][0-9]+$');
