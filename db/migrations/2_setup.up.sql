CREATE TABLE namespaces (
    namespace_id SERIAL PRIMARY KEY,
    namespace_name TEXT UNIQUE NOT NULL
);

CREATE TABLE records (
    record_id UUID PRIMARY KEY,
    record_namespace INTEGER NOT NULL,
    record_timestamp TIMESTAMP WITH TIME ZONE,
    record_hash TEXT NOT NULL,
    record_data TEXT,
    CONSTRAINT fk_namespace FOREIGN KEY (record_namespace) REFERENCES namespaces (namespace_id),
    CONSTRAINT uniq_namespace_hash UNIQUE (record_namespace, record_hash)
);

CREATE INDEX idx_record_namespace_uuid ON records (record_namespace, record_id);
CREATE INDEX idx_record_namespace_timestamp ON records (record_namespace, record_timestamp);

CREATE TABLE indexing_keys (
    key_id SERIAL PRIMARY KEY,
    namespace_id INTEGER NOT NULL,
    key_name TEXT NOT NULL,
    CONSTRAINT fk_indexing_keys_namespace FOREIGN KEY (namespace_id) REFERENCES namespaces (namespace_id),
    CONSTRAINT uniq_namespace_keyname UNIQUE (namespace_id, key_name)
);

CREATE TABLE indexing_data (
    key_id INTEGER NOT NULL,
    namespace_id INTEGER NOT NULL,
    record_id UUID NOT NULL,
    value TEXT NULL,
    CONSTRAINT fk_indexing_data_key FOREIGN KEY (key_id)
        REFERENCES indexing_keys (key_id),
    CONSTRAINT fk_indexing_data_namespace FOREIGN KEY (namespace_id)
        REFERENCES namespaces (namespace_id),
    CONSTRAINT fk_indexing_data_record FOREIGN KEY (record_id)
        REFERENCES records (record_id)
);

CREATE INDEX idx_indexing_data ON indexing_data (namespace_id, key_id, value);

