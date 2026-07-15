-- 0001_init.down.sql — full teardown of the control-plane schema.
--
-- This is the FIRST migration, so its down reverses the entire schema. CASCADE drops every table,
-- index, trigger, partition and the touch_updated_at() function in control_plane, plus the
-- dashboard.operators stub this migration created. If a later migration set makes `dashboard` a
-- shared schema owned elsewhere, revisit this down.

DROP SCHEMA IF EXISTS control_plane CASCADE;
DROP SCHEMA IF EXISTS dashboard CASCADE;
