CREATE TABLE IF NOT EXISTS test_cli_migration ON CLUSTER dev (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_cli_migration', '{replica}')
ORDER BY id;
