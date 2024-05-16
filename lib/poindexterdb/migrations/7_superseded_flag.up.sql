ALTER TABLE records ADD COLUMN record_is_superseded BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE records SET record_is_superseded = TRUE WHERE record_is_superseded_by_id IS NOT NULL;

CREATE INDEX idx_with_markers_namespace_id_uuid ON records (namespace_id, record_is_deletion_marker, record_is_superseded, record_id);