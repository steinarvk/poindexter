-- Create the "namespaces" table
CREATE TABLE namespaces (
    namespace_id SERIAL PRIMARY KEY,
    namespace_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create the "records" table
CREATE TABLE records (
    record_id SERIAL PRIMARY KEY,
    record_namespace INTEGER NOT NULL,
    record_uuid UUID NOT NULL,
    record_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    record_hash BYTEA NOT NULL,
    record_data TEXT,
    CONSTRAINT fk_namespace FOREIGN KEY (record_namespace) REFERENCES namespaces (namespace_id),
    CONSTRAINT uniq_namespace_uuid UNIQUE (record_namespace, record_uuid),
    CONSTRAINT uniq_namespace_hash UNIQUE (record_namespace, record_hash)
);

CREATE INDEX idx_record_namespace_uuid ON records (record_namespace, record_uuid);
CREATE INDEX idx_record_namespace_timestamp ON records (record_namespace, record_timestamp);

CREATE TABLE indexing_keys (
    key_id SERIAL PRIMARY KEY,
    namespace_id INTEGER NOT NULL,
    key_name VARCHAR(255) NOT NULL,
    CONSTRAINT fk_indexing_keys_namespace FOREIGN KEY (namespace_id) REFERENCES namespaces (namespace_id),
    CONSTRAINT uniq_namespace_keyname UNIQUE (namespace_id, key_name)
);

-- Create the "indexing_data" table
CREATE TABLE indexing_data (
    key_id INTEGER NOT NULL,
    namespace_id INTEGER NOT NULL,
    record_id INTEGER NOT NULL,
    value VARCHAR(1024) NULL,
    CONSTRAINT fk_indexing_data_key FOREIGN KEY (key_id)
        REFERENCES indexing_keys (key_id),
    CONSTRAINT fk_indexing_data_namespace FOREIGN KEY (namespace_id)
        REFERENCES namespaces (namespace_id),
    CONSTRAINT fk_indexing_data_record FOREIGN KEY (record_id)
        REFERENCES records (record_id)
);

-- Create index for (namespace_id, key_id, value) in "indexing_data"
CREATE INDEX idx_indexing_data ON indexing_data (namespace_id, key_id, value);

