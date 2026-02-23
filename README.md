<p align="center">
  <img src="assets/clicko_no_smoking.png" alt="clicko no smoking" width="500" />
</p>

A ClickHouse migration tool friendly for self-hosted sharded clusters, inspired by [pressly/goose](https://github.com/pressly/goose). Works with ClickHouse Cloud too. Supports engine selection, insert quorum, `ON CLUSTER` DDL, and both SQL file and Go function migrations.

[![Test](https://github.com/arsura/clicko/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/arsura/clicko/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/arsura/clicko)](https://goreportcard.com/report/github.com/arsura/clicko)

## Features

- **Engine selection** — Choose any ClickHouse table engine for the migration tracking table. Standalone mode defaults to `MergeTree()`; cluster mode defaults to `ReplicatedMergeTree(...)`. Override with `--engine` to control the ZooKeeper path and replication topology.
- **Insert quorum** — Set `--insert-quorum` (a number or `"auto"`) to ensure migration records are replicated to N nodes before being considered applied. Prevents inconsistent migration state across replicas.
- **Cluster and sharding support** — First-class `ON CLUSTER` support via `--cluster`. DDL propagates across all nodes; DELETE mutations use `mutations_sync=2` for consistency.
- **SQL and Go migrations** — Write migrations as plain `.sql` files or as Go functions.

## Installation

```bash
go install github.com/arsura/clicko/cmd/clicko@latest
```

## Migration files

SQL migration files follow the naming convention:

```
{version}_{description}.{up|down}.sql
```

Example directory:

```
migrations/
├── 00001_create_users.up.sql
├── 00001_create_users.down.sql
├── 00002_create_orders.up.sql
├── 00002_create_orders.down.sql
└── 00003_add_users_age_column.up.sql
```

## Recommendations

### Forward-only migrations

clicko supports `down` migrations, but for ClickHouse in production you may want to avoid writing them altogether.

ClickHouse tables tend to be large, and many DDL operations (like `DROP COLUMN` or `ALTER TABLE ... DELETE`) are executed as background mutations that can take a long time to complete and cannot be easily interrupted. Rolling back a migration by running a `down` file does not instantly undo the change — it queues another mutation on top, which can leave the cluster in an inconsistent state during the window between the two operations.

A **forward-only** approach means every change is expressed as a new `up` migration. Instead of rolling back, you write a follow-up migration that corrects or reverts the intent. This keeps the migration history append-only, predictable, and safe to apply in automated pipelines.

This is a recommendation, not a requirement. If your use case is well-suited to rollbacks (e.g. a small local cluster or a development environment), the `down` commands work fine.

### Idempotent migrations

ClickHouse has no transactional DDL. If a migration with multiple statements fails halfway, the already-applied statements won't roll back. Re-running the migration will hit errors like "table already exists."

Write every statement in an idempotent form so re-runs are safe:

- `CREATE TABLE IF NOT EXISTS` instead of `CREATE TABLE`
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` instead of `ALTER TABLE ... ADD COLUMN`
- `DROP TABLE IF EXISTS` instead of `DROP TABLE`

### One statement per migration

When possible, put a single DDL statement in each migration file. Since ClickHouse has no transactional DDL, a file with multiple statements can fail halfway — making it harder to tell what succeeded and what didn't. One statement per file keeps failures obvious and pairs well with idempotent writes.

Instead of one file with two statements:

```sql
-- 00002_create_orders.up.sql
CREATE TABLE IF NOT EXISTS orders (...) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE IF NOT EXISTS order_items (...) ENGINE = MergeTree() ORDER BY id;
```

Split into two migration files:

```sql
-- 00002_create_orders.up.sql
CREATE TABLE IF NOT EXISTS orders (...) ENGINE = MergeTree() ORDER BY id;
```

```sql
-- 00003_create_order_items.up.sql
CREATE TABLE IF NOT EXISTS order_items (...) ENGINE = MergeTree() ORDER BY id;
```

### Run migrations from CI/CD, not on application boot

ClickHouse has no advisory locks or distributed mutual exclusion. If you run migrations at application startup and deploy multiple instances at once, they will race — causing duplicate tracking rows, conflicting DDL, or partial failures.

Instead, run migrations as a **dedicated, single-process step** in your CI/CD pipeline (e.g. a Kubernetes `Job` or a CI stage) before deploying the new application version. This guarantees only one process touches the migration state at a time, sidestepping the locking problem entirely.

If you use the Go library, run it from a CI job — not as part of your application code.

## CLI usage

```
clicko --uri <uri> [flags] <command>
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--uri` | *(required)* | ClickHouse connection URI (e.g. `clickhouse://user:pass@host:9000/db`) |
| `--dir` | `migrations` | Directory containing migration files |
| `--table` | `migration_versions` | Migration tracking table name |
| `--cluster` |   | ClickHouse cluster name (enables `ON CLUSTER`) |
| `--engine` |   | Custom table engine for the tracking table |
| `--insert-quorum` |   | Write quorum for cluster inserts (number or `"auto"`) |
| `--help` |   | Show help |

### Commands

| Command | Description |
|---|---|
| `up` | Apply all pending migrations |
| `up-to <version>` | Apply migrations up to a specific version |
| `down` | Rollback the last applied migration |
| `down-to <version>` | Rollback migrations down to a specific version |
| `reset` | Rollback all applied migrations |
| `status` | Show migration status |

### Example

```bash
clicko --uri "clickhouse://default:@localhost:9000/default" --dir migrations up
```

### Cluster mode

```bash
clicko \
  --uri "clickhouse://default:@localhost:9000/default" \
  --dir migrations \
  --cluster migration \
  --engine "ReplicatedMergeTree('/clickhouse/migration/table/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  up
```

## Go library

Besides the CLI, clicko can be embedded as a Go library. This lets you run migrations as part of your CI pipeline, write integration tests against a local cluster, and programmatically target different environments — no risky manual access to the cluster required.

```go
package main

import (
    "context"
    "log"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/arsura/clicko"

    _ "your/app/migrations" // blank import to register Go migrations via init()
)

func main() {
    ctx := context.Background()

    opts, err := clickhouse.ParseDSN("clickhouse://default:@localhost:9000/default")
    if err != nil {
        log.Fatal(err)
    }
    conn, err := clickhouse.Open(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    migrator, err := clicko.New(conn, clicko.StoreConfig{
        TableName:    "migration_versions",
        Cluster:      "migration",
        CustomEngine: "ReplicatedMergeTree('/clickhouse/migration/table/{database}/{table}', '{replica}')",
        InsertQuorum: "4",
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := migrator.Up(ctx); err != nil {
        log.Fatal(err)
    }
}
```

See [Go integration example](example/go/README.md) for the full walkthrough including Go function migrations with `clicko.RegisterMigration`.

## Migrations on a sharded cluster

When your ClickHouse cluster has multiple shards, the data cluster (e.g. `dev`) splits nodes into separate shards — each shard only replicates within its own group. This is great for data but problematic for migration tracking: if you run `ON CLUSTER dev`, the migration table gets sharded too, and each shard may end up with an independent copy of migration state.

The solution is to define a **logical cluster** dedicated to migrations. This cluster puts **all replicas from every shard into a single shard**, so the migration tracking table replicates uniformly across the entire cluster.

For example, the [dev/cluster](https://github.com/arsura/clicko/tree/main/dev/cluster) setup defines two clusters:

- `dev` — the data cluster with 2 shards x 2 replicas:

```
dev
├── shard 1: ch-1-1, ch-1-2
└── shard 2: ch-2-1, ch-2-2
```

- `migration` — a logical cluster with a single shard containing all 4 nodes:

```
migration
└── shard 1: ch-1-1, ch-1-2, ch-2-1, ch-2-2
```

When you run clicko with `--cluster migration`, the migration tracking table is created `ON CLUSTER migration` and every node sees the same replicated migration state. Pair this with a custom engine whose ZooKeeper path does **not** include `{shard}`, and `--insert-quorum` to guarantee writes reach all replicas before returning:

```bash
clicko \
  --cluster migration \
  --engine "ReplicatedMergeTree('/clickhouse/migration/table/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  ...
```

Your actual data migrations can still use `ON CLUSTER dev` inside the SQL files themselves.

## Examples

- [CLI example](example/cli/README.md) — SQL file migrations via the CLI
- [Go example](example/go/README.md) — Go function migrations embedded in an application

## Development

The `dev/cluster` directory contains a Docker Compose setup for a local ClickHouse cluster (2 shards x 2 replicas + 1 ClickHouse Keeper node).

Start the cluster:

```bash
make cluster-up
```

Run tests:

```bash
make test
```

Other commands:

```bash
make cluster-down       # stop and remove volumes
make cluster-restart    # restart the cluster
make build              # build the CLI binary to bin/clicko
```
