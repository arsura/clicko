CREATE TABLE IF NOT EXISTS orders ON CLUSTER dev (
    id          UInt64,
    user_id     UInt64,
    amount      Decimal64(2),
    status      LowCardinality(String),
    created_at  DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree
ORDER BY (user_id, id);
