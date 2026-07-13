<p align="center">
  <img src="assets/clicko_no_smoking.png" alt="clicko no smoking" width="500" />
</p>

A ClickHouse migration tool built for self-hosted sharded clusters, inspired by [pressly/goose](https://github.com/pressly/goose). Works with ClickHouse Cloud too.

[![Test](https://github.com/arsura/clicko/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/arsura/clicko/actions/workflows/test.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/arsura/clicko.svg)](https://pkg.go.dev/github.com/arsura/clicko)

## Features

- **`ON CLUSTER` support.** DDL propagates across all nodes; DELETE mutations run with `mutations_sync=2`.
- **Engine selection.** Pick the engine for the migration tracking table: `MergeTree` standalone, `ReplicatedMergeTree` in cluster mode, or your own via `--engine`.
- **Insert quorum.** `--insert-quorum` guarantees migration records reach N replicas before being marked applied.
- **SQL and Go migrations.** Plain `.sql` files or Go functions, with `--dry-run` for both.

## Quick start

```bash
go install github.com/arsura/clicko/cmd/clicko@latest

clicko --uri "clickhouse://default:@localhost:9000/default" --dir migrations up
```

> **Keep credentials out of your shell history.** The URI contains a password, and
> anything passed as a command-line argument is visible in the process list (`ps`)
> and saved to shell history. Prefer the `CLICKO_URI` environment variable, which
> `clicko` reads automatically when `--uri` is omitted:
>
> ```bash
> export CLICKO_URI="clickhouse://default:PASSWORD@localhost:9000/default"
> clicko --dir migrations up
> ```

SQL migration files follow `{version}_{description}.{up|down}.sql`:

```
migrations/
├── 00001_create_users.up.sql
├── 00001_create_users.down.sql
└── 00002_create_orders.up.sql
```

**ClickHouse Cloud:** replication is managed for you, so skip `--cluster`, `--engine`, and `--insert-quorum` entirely:

```bash
clicko --uri "clickhouse://default:PASSWORD@SERVICE.clickhouse.cloud:9440/default?secure=true" --dir migrations up
```

## CLI reference

```
clicko --uri <uri> [flags] <command>
```

| Command | Description |
|---|---|
| `up` / `up-to <version>` | Apply pending migrations (all, or up to a version) |
| `down` / `down-to <version>` | Rollback the last migration (or down to a version), skipping any forward-only migration along the way |
| `reset` | Rollback all applied migrations, skipping any forward-only migration along the way |
| `status` | Show migration status |

| Flag | Default | Description |
|---|---|---|
| `--uri` | *(required)* | Connection URI, e.g. `clickhouse://user:pass@host:9000/db`. Can be supplied via the `CLICKO_URI` env var instead, to keep credentials out of the process list and shell history |
| `--dir` | `migrations` | Migration files directory |
| `--table` | `migration_versions` | Tracking table name |
| `--cluster` | | Cluster name (enables `ON CLUSTER`) |
| `--engine` | | Custom engine for the tracking table (MergeTree family — clicko appends `ORDER BY version` to the DDL) |
| `--insert-quorum` | | Write quorum (number or `"auto"`) |
| `--dry-run` | | Print SQL without executing |
| `--allow-out-of-order` | | Allow pending migrations older than the highest applied version |

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

### Dry-run

`--dry-run` previews the SQL for any command without applying it. For Go migrations, every `Exec`/`Query` call is captured against a no-op connection, so even dynamically-built SQL is shown in its final form. Dry-run performs no writes at all — not even creating the migration tracking table; if the table does not exist yet, its `CREATE TABLE` DDL is shown as part of the preview.

```
=== Version 1: create users (sql) ===
CREATE TABLE IF NOT EXISTS users (...) ENGINE = MergeTree() ORDER BY id;
```

## Go library

Embed clicko to run migrations from CI pipelines or integration tests, with no manual cluster access needed.

```go
conn, _ := clickhouse.Open(opts)

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
```

See the [Go integration example](example/go/README.md) for the full walkthrough, including Go function migrations via `clicko.RegisterMigration`.

## Best practices

**Prefer forward-only migrations.** ClickHouse DDL like `DROP COLUMN` runs as slow, uninterruptible background mutations. A `down` file doesn't undo anything; it just queues another mutation and can leave the cluster inconsistent in between. Instead of rolling back, write a new `up` migration that reverts the intent. (`down` still works fine for local/dev setups.)

A migration with no `.down.sql` file (or no `DownFunc`, for Go migrations) is forward-only. `down`, `down-to`, and `reset` skip over it — leaving it recorded as applied — and keep reverting the independent migrations underneath. This can leave the tracking table inconsistent with reality if a migration below the forward-only one is something its changes depended on; that's the tradeoff of skipping instead of stopping outright, and it's on you as the operator to know when that's safe. It also means a subsequent `up` may need `--allow-out-of-order` to re-apply the skipped version's neighbors.

**Write idempotent statements.** ClickHouse has no transactional DDL, so a failed multi-statement migration won't roll back. Use `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `DROP TABLE IF EXISTS` so re-runs are always safe.

**One statement per migration file.** A file with multiple statements can fail halfway, making it hard to tell what succeeded. One statement per file keeps failures obvious and pairs well with idempotent writes.

**Run migrations from CI/CD, not on app boot.** ClickHouse has no advisory locks, so multiple instances migrating at startup will race, causing duplicate tracking rows or conflicting DDL. Run migrations as a single dedicated step (e.g. a Kubernetes `Job` or CI stage) before deploying.

## Migrations on a sharded cluster

On a sharded data cluster, running `ON CLUSTER dev` shards the migration tracking table too, and each shard ends up with independent migration state.

The fix: define a **logical cluster** just for migrations, with all replicas from every shard in a single shard, so the tracking table replicates uniformly everywhere.

```
dev (data)                     migration (logical)
├── shard 1: ch-1-1, ch-1-2    └── shard 1: ch-1-1, ch-1-2, ch-2-1, ch-2-2
└── shard 2: ch-2-1, ch-2-2
```

Run clicko with `--cluster migration`, use an engine whose ZooKeeper path does **not** include `{shard}`, and set `--insert-quorum` so writes reach every replica:

```bash
clicko \
  --cluster migration \
  --engine "ReplicatedMergeTree('/clickhouse/migration/table/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  ...
```

Your data migrations can still use `ON CLUSTER dev` inside the SQL files themselves. See [dev/cluster](https://github.com/arsura/clicko/tree/main/dev/cluster) for a working setup.

## Examples

- [CLI example](example/cli/README.md): SQL file migrations via the CLI
- [Go example](example/go/README.md): Go function migrations embedded in an application

## Development

`dev/cluster` provides a Docker Compose setup: 2 shards × 2 replicas + 1 ClickHouse Keeper.

```bash
make cluster-up         # start the local cluster
make test               # run tests
make cluster-down       # stop and remove volumes
make build              # build the CLI to bin/clicko
```
