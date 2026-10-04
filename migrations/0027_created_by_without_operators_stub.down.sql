-- NOT VALID: the created_by written since the up reference operators the stub never held, and a
-- validated key would make this rollback impossible. New rows are still checked: roll the binary back
-- first, or every creation under a BFF token fails on the empty stub.
CREATE SCHEMA dashboard;
CREATE TABLE dashboard.operators (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  email       text NOT NULL UNIQUE,
  display_name text,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE dashboard.operators IS
  'STUB — canonical definition lives in the Admin Dashboard spec. Present only to satisfy created_by FKs.';
ALTER TABLE control_plane.customer_groups ADD CONSTRAINT customer_groups_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES dashboard.operators(id) NOT VALID;
ALTER TABLE control_plane.sender_ids ADD CONSTRAINT sender_ids_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES dashboard.operators(id) NOT VALID;
ALTER TABLE control_plane.routing_scripts ADD CONSTRAINT routing_scripts_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES dashboard.operators(id) NOT VALID;
ALTER TABLE control_plane.sender_id_rewrite_rules ADD CONSTRAINT sender_id_rewrite_rules_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES dashboard.operators(id) NOT VALID;
