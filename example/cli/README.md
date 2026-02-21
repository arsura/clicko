# CLI Migrations Example

This example shows how to use **clicko** as a standalone CLI tool with plain `.sql` migration files — no Go code required.

## Project Structure

```
cli-migrations/
└── migrations/
    ├── 00001_create_users.up.sql
    ├── 00001_create_users.down.sql
    ├── 00002_create_orders.up.sql
    ├── 00002_create_orders.down.sql
    ├── 00003_add_users_age_column.up.sql
    └── 00003_add_users_age_column.down.sql
```

## How It Works

1. Each migration version requires an `.up.sql` file. A `.down.sql` file is optional (for rollback support).
2. Files must follow the naming convention: `<version>_<description>.<up|down>.sql`
3. The CLI scans the `--dir` directory, loads all matching files, and runs the requested command.

## Installation

```bash
go install github.com/arsura/clicko/cmd/cli@latest
```

Or build from source (from the repository root):

```bash
go build -o clicko ./cmd/cli
```

## Running

```bash
# Make sure ClickHouse is running on localhost:9000

# Apply all pending migrations
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       up

# Check migration status
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       status

# Rollback the last migration
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       down

# Apply migrations up to version 2 (inclusive)
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       up-to 2

# Rollback down to (but not including) version 1
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       down-to 1

# Rollback all migrations
clicko --uri "clickhouse://default:@localhost:9000/default" \
       --dir ./migrations \
       reset
```

## Cluster Mode

For cluster deployments, pass additional flags:

```bash
clicko \
  --uri "clickhouse://default:@localhost:29000/default" \
  --dir ./migrations \
  --cluster "migration" \
  --engine "ReplicatedMergeTree('/clickhouse/migration/tables/all/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  up
```

> **Note:** When using cluster mode, your `.up.sql` and `.down.sql` files should also include `ON CLUSTER <cluster_name>` in the DDL statements.

## Writing a New Migration

Create a pair of files (or just an `.up.sql` if no rollback is needed):

```sql
-- 00004_add_products_table.up.sql
CREATE TABLE IF NOT EXISTS products (
    id    UInt64,
    name  String,
    price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY id;
```

```sql
-- 00004_add_products_table.down.sql
DROP TABLE IF EXISTS products;
```
