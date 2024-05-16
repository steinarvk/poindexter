ALTER TABLE records ADD COLUMN record_is_superseded_by_id UUID;

UPDATE records
SET record_is_superseded_by_id = t2.record_id
FROM records AS t2
WHERE records.record_supersedes_id = t2.record_id
AND   records.namespace_id = t2.namespace_id;