# CLI Example

This example demonstrates how to use **clicko** via the CLI to manage ClickHouse migrations defined as SQL files.

## Structure

```
example/cli/
└── migrations/
    ├── 00001_create_users.up.sql
    ├── 00001_create_users.down.sql
    ├── 00002_create_orders.up.sql
    ├── 00002_create_orders.down.sql
    ├── 00003_add_users_age_column.up.sql
    └── 00003_add_users_age_column.down.sql
```

Migration files follow the naming convention: `{version}_{description}.{up|down}.sql`


## Usage Examples

Apply all pending migrations:

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir example/clicko/migrations up
```

Check current migration status:

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir example/clicko/migrations status
```

Apply migrations up to version 2:

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir example/clicko/migrations up-to 2
```

Rollback the last applied migration:

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir example/clicko/migrations down
```

Rollback all migrations:

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir example/clicko/migrations reset
```

### Cluster Mode

For ClickHouse clusters, provide `--cluster` and optionally a custom `--engine`:

```bash
clicko \
  --uri "clickhouse://default:@localhost:9000/default" \
  --dir example/clicko/migrations \
  --cluster migration \
  --engine "ReplicatedMergeTree('/clickhouse/migration/table/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  up
```
