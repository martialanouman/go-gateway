-- Enforce the canonical E.164-minus-"+" form (digits only, country code 1-9) on suppression MSISDNs
-- (§6.20). The opt-out Bloom snapshot and the exact confirmation key on this exact form; a write path
-- (admin, import, carrier, MO STOP) storing a non-normalized value ("+225…", padded) would silently
-- defeat suppression — a regulatory false negative. This CHECK makes such a write fail loudly instead.
ALTER TABLE control_plane.suppressions
  ADD CONSTRAINT suppressions_msisdn_canonical_ck CHECK (msisdn ~ '^[1-9][0-9]+$');
