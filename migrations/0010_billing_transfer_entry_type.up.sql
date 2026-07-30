-- Add the 'transfer' ledger entry type for admin balance transfers (§6.9, step-148). A transfer moves MT
-- credit between two owners of the SAME customer as TWO signed entries (debit source / credit destination)
-- summing to zero, sharing one correlation reference. It is admin-initiated and made idempotent by a
-- client key claimed in billing_idempotency, so both tables gain 'transfer'. This also brings
-- billing_idempotency's set up to billing_ledger's, so any message-scoped admin entry (topup, adjustment)
-- can be claimed for retry-safety.
ALTER TABLE control_plane.billing_ledger
  DROP CONSTRAINT billing_ledger_entry_type_check,
  ADD CONSTRAINT billing_ledger_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','topup','adjustment','mo_charge','transfer'));

ALTER TABLE control_plane.billing_idempotency
  DROP CONSTRAINT billing_idempotency_entry_type_check,
  ADD CONSTRAINT billing_idempotency_entry_type_check
    CHECK (entry_type IN ('reserve','capture','release','refund','topup','adjustment','mo_charge','transfer'));
