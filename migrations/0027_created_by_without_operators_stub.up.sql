-- created_by is the BFF's operator id (ADR-0019), and the BFF keeps its operators in its own database:
-- no real operator could ever satisfy these keys. RESTRICT on the schema: a dashboard schema holding
-- anything but the stub stops the migration rather than losing it.
ALTER TABLE control_plane.customer_groups DROP CONSTRAINT customer_groups_created_by_fkey;
ALTER TABLE control_plane.sender_ids DROP CONSTRAINT sender_ids_created_by_fkey;
ALTER TABLE control_plane.routing_scripts DROP CONSTRAINT routing_scripts_created_by_fkey;
ALTER TABLE control_plane.sender_id_rewrite_rules DROP CONSTRAINT sender_id_rewrite_rules_created_by_fkey;
DROP TABLE dashboard.operators;
DROP SCHEMA dashboard RESTRICT;
