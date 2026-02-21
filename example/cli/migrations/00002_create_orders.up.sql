CREATE TABLE IF NOT EXISTS orders (
    id          UInt64,
    user_id     UInt64,
    amount      Decimal64(2),
    status      LowCardinality(String),
    created_at  DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, id);
