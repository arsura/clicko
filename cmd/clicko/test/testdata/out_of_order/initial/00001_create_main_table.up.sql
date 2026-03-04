CREATE TABLE IF NOT EXISTS oot_main (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id;
