-- Add the 'mo_charge' ledger entry type for the MO meter (§6.9, step-143). MO (mobile-originated, the
-- return path) is a postpaid counter, distinct from the MT lifecycle: each MO is a single debit
-- (credits < 0) on the MO balance, never a reserve/capture/release cycle. It is message-scoped and
-- idempotent by message_id, so it must also enter the partition-free billing_idempotency guard.
ALTER TABLE control_plane.billing_ledger
  DROP CONSTRAINT billing_ledger_entry_type_check,
  ADD CONSTRAINT billing_ledger_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','topup','adjustment','mo_charge'));

ALTER TABLE control_plane.billing_idempotency
  DROP CONSTRAINT billing_idempotency_entry_type_check,
  ADD CONSTRAINT billing_idempotency_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','mo_charge'));
