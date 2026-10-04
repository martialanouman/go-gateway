-- step-400: transitions waiting for the billing.events topic, written in the mo_charge transaction.
CREATE TABLE control_plane.billing_events_outbox (
  id            uuid NOT NULL DEFAULT uuidv7() PRIMARY KEY,
  owner_type    text NOT NULL CHECK (owner_type IN ('customer','smpp_account')),
  owner_id      uuid NOT NULL,
  customer_id   uuid NOT NULL,
  balance_after integer NOT NULL,
  floor         integer NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
