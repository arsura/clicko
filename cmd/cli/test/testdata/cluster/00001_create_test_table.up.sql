CREATE TABLE IF NOT EXISTS cluster_table ON CLUSTER dev (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/cluster_table', '{replica}')
ORDER BY id;
