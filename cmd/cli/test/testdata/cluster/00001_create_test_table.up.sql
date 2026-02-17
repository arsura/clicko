CREATE TABLE IF NOT EXISTS test_cluster_migration ON CLUSTER dev (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_cluster_migration', '{replica}')
ORDER BY id;
