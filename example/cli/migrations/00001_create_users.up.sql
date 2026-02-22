CREATE TABLE IF NOT EXISTS users ON CLUSTER dev (
    id          UInt64,
    name        String,
    email       String,
    created_at  DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
ORDER BY id;
