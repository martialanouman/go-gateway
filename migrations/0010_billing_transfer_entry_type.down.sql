ALTER TABLE control_plane.billing_idempotency
  DROP CONSTRAINT billing_idempotency_entry_type_check,
  ADD CONSTRAINT billing_idempotency_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','mo_charge'));

ALTER TABLE control_plane.billing_ledger
  DROP CONSTRAINT billing_ledger_entry_type_check,
  ADD CONSTRAINT billing_ledger_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','topup','adjustment','mo_charge'));
