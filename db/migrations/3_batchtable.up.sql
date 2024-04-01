CREATE TABLE processed_batches (
    namespace_id INTEGER NOT NULL,
    batch_timestamp TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    batch_data_hash TEXT NOT NULL,
    CONSTRAINT fk_namespace FOREIGN KEY (namespace_id) REFERENCES namespaces (namespace_id)
);
