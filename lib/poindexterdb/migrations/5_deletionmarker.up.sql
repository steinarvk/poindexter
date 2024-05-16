ALTER TABLE records ADD COLUMN record_is_deletion_marker BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX idx_partial_namespace_id_uuid ON records (namespace_id, record_id) WHERE NOT record_is_deletion_marker;
CREATE INDEX idx_partial_namespace_id_timestamp ON records (namespace_id, record_timestamp) WHERE NOT record_is_deletion_marker;
CREATE INDEX idx_partial_namespace_id_shape_hash ON records (namespace_id, record_shape_hash) WHERE NOT record_is_deletion_marker;