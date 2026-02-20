CREATE TABLE orders (id UInt64, user_id UInt64) ENGINE = MergeTree() ORDER BY id;
