CREATE TABLE IF NOT EXISTS oot_audit (
    id UInt64,
    action String
) ENGINE = MergeTree()
ORDER BY id;
