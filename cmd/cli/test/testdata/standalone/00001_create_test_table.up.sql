CREATE TABLE IF NOT EXISTS test_standalone_migration (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;
