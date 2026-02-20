# Go Migrations Example

This example shows how to use **clicko** as a Go library to write migrations as Go functions instead of SQL files.

## Project Structure

```
go-migrations/
├── main.go                                # Entry point — wires up clicko and runs a command
└── migrations/
    ├── 00001_create_users.go              # Up + Down
    ├── 00002_create_orders.go             # Up + Down
    └── 00003_add_users_age_column.go      # Up only (no rollback)
```

## How It Works

1. Each migration file calls `clicko.AddMigration(upFn, downFn)` inside `init()`.
   The version is derived from the filename automatically (e.g. `00001_create_users.go` → version 1).

2. `main.go` does a blank import of the migrations package (`_ "…/migrations"`),
   which triggers all `init()` functions and registers every migration.

3. A `clicko.NewGoLoader()` reads from the global registry and feeds them into the `Migrator`.

## Running

```bash
# Make sure ClickHouse is running on localhost:9000
# Or set CLICKHOUSE_URI to override

go run . up       # Apply all pending migrations
go run . status   # Show migration status
go run . down     # Rollback the last migration
go run . reset    # Rollback all migrations
```

## Writing a New Migration

Create a new file following the naming convention `<version>_<description>.go`:

```go
package migrations

import (
    "context"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/arsura/clicko"
)

func init() {
    clicko.AddMigration(upMyMigration, downMyMigration)
}

func upMyMigration(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS ...`)
}

func downMyMigration(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `DROP TABLE IF EXISTS ...`)
}
```

Pass `nil` as the second argument to `AddMigration` if the migration is forward-only (no rollback).

## Cluster Mode

For cluster deployments, configure the store with cluster settings:

```go
store, _ := clicko.NewStore(conn, clicko.StoreConfig{
    TableName:    "migration_versions",
    Cluster:      "my_cluster",
    CustomEngine: "ReplicatedMergeTree(...)",
    InsertQuorum: "auto",
})
```

Then use cluster-aware DDL in your migration functions:

```go
func upCreateTable(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS events ON CLUSTER my_cluster (
            ...
        ) ENGINE = ReplicatedMergeTree(...)
        ORDER BY id
    `)
}
```
