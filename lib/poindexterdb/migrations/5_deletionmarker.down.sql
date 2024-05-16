DROP INDEX IF EXISTS idx_partial_namespace_id_uuid;
DROP INDEX IF EXISTS idx_partial_namespace_id_timestamp;
DROP INDEX IF EXISTS idx_partial_namespace_id_shape_hash;

ALTER TABLE records DROP COLUMN record_is_deletion_marker;
