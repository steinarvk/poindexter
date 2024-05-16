ALTER TABLE records ADD COLUMN entity_id UUID NULL;

UPDATE records SET entity_id = record_id;

ALTER TABLE records ALTER COLUMN entity_id SET NOT NULL;

CREATE INDEX idx_entityid ON records (namespace_id, record_is_deletion_marker, record_is_superseded, entity_id);