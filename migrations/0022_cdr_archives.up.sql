CREATE TABLE control_plane.cdr_archives (
  day         date PRIMARY KEY,
  object      text NOT NULL,
  row_count   bigint NOT NULL CHECK (row_count >= 0),
  archived_at timestamptz NOT NULL DEFAULT now()
);
