CREATE TABLE IF NOT EXISTS standalone_table (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;
