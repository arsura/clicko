CREATE TABLE IF NOT EXISTS users (
    id          UInt64,
    name        String,
    email       String,
    created_at  DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
