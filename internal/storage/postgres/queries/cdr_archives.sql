-- name: RecordCDRArchive :one
-- First writer wins and the row is never rewritten: returns the row_count of whichever line now holds the
-- day. The outer SELECT cannot see the CTE's own insert, so exactly one branch yields a row.
WITH inserted AS (
  INSERT INTO control_plane.cdr_archives (day, object, row_count)
  VALUES (@day, @object, @row_count)
  ON CONFLICT (day) DO NOTHING
  RETURNING row_count
)
SELECT row_count FROM inserted
UNION ALL
SELECT row_count FROM control_plane.cdr_archives WHERE day = @day
LIMIT 1;
