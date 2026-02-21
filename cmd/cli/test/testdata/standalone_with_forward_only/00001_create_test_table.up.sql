CREATE TABLE IF NOT EXISTS forward_only_table (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;
