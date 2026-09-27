-- name: RecordCDRArchive :one
-- First writer wins and the row is never rewritten: returns the object of whichever line now holds the
-- day. The outer SELECT cannot see the CTE's own insert, nor a concurrent one committed while the insert
-- waited on it: no row then, and the caller runs the statement again.
WITH inserted AS (
  INSERT INTO control_plane.cdr_archives (day, object, row_count)
  VALUES (@day, @object, @row_count)
  ON CONFLICT (day) DO NOTHING
  RETURNING object
)
SELECT object FROM inserted
UNION ALL
SELECT object FROM control_plane.cdr_archives WHERE day = @day
LIMIT 1;
