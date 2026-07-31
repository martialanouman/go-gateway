-- Remove the per-column body TTL. The content columns then live as long as the row (the table TTL).
ALTER TABLE cdr MODIFY COLUMN content_ciphertext REMOVE TTL, MODIFY COLUMN content_key_id REMOVE TTL;
