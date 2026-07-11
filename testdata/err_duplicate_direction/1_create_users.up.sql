CREATE TABLE users (id UInt64, email String) ENGINE = MergeTree() ORDER BY id;
