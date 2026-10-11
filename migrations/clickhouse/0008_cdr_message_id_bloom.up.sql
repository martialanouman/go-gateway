-- step-287n. Current filters on the sort-key prefix (customer_id, account_id), which keeps a whole account: a
-- large one is hundreds of millions of rows a day, read on every reaper check and every GET /messages/{id}.
-- The bloom filter lets a message_id lookup skip every granule that does not hold it.
--
-- No MATERIALIZE INDEX, for the reason 0003 gives about its TTL: indexing the existing parts rereads the whole
-- retained CDR in the background while the deploy reports success. New and merged parts carry the index; the
-- existing ones are an operator action, partition by partition, off-peak:
--   ALTER TABLE cdr MATERIALIZE INDEX message_id_bloom IN PARTITION 'YYYY-MM-DD'
ALTER TABLE cdr ADD INDEX message_id_bloom message_id TYPE bloom_filter(0.01) GRANULARITY 1
