-- ALTER TABLE records ADD COLUMN record_is_superseded BOOLEAN NOT NULL DEFAULT FALSE;
-- CREATE INDEX idx_with_markers_namespace_id_uuid ON records (namespace_id, record_is_deletion_marker, record_is_superseded, record_id);

DROP INDEX IF EXISTS idx_with_markers_namespace_id_uuid;
ALTER TABLE records DROP COLUMN record_is_superseded;
