-- Unrouted MO (§6.21): mobile-originated messages that resolved to no account. A configuration
-- anomaly kept for the operator (list-unrouted-mo), never a billable CDR — a distinct table that
-- never stores the message body (invariant a).
CREATE TABLE control_plane.unrouted_mo (
  id                uuid PRIMARY KEY DEFAULT uuidv7(),
  received_at       timestamptz NOT NULL DEFAULT now(),
  connector_id      uuid,
  inbound_number_id uuid REFERENCES control_plane.inbound_numbers(id) ON DELETE SET NULL,
  source_addr       text NOT NULL,
  dest_addr         text NOT NULL,
  segment_count     integer NOT NULL DEFAULT 1,
  encoding          text NOT NULL,
  reason            text NOT NULL CHECK (reason IN ('unknown_number','number_disabled','no_keyword_match'))
);

CREATE INDEX unrouted_mo_page_idx ON control_plane.unrouted_mo(received_at DESC, id DESC);
