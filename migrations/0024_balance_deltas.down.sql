-- Fold every pending delta before removing the table, or the credits it holds are lost.
WITH moved AS (
  DELETE FROM control_plane.balance_deltas RETURNING owner_type, owner_id, direction, credits
)
INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
SELECT owner_type, owner_id, direction, sum(credits)::int FROM moved
GROUP BY owner_type, owner_id, direction
ON CONFLICT (owner_type, owner_id, direction)
DO UPDATE SET credits = control_plane.balances.credits + excluded.credits, updated_at = now();
DROP TABLE control_plane.balance_deltas;
