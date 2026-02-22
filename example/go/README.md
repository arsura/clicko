# Go Integration Example

This example demonstrates how to embed **clicko** directly in a Go application and manage ClickHouse migrations programmatically using Go functions.

## Structure

```
example/go/
├── main.go
└── migrations/
    ├── 00001_create_users.go
    ├── 00002_create_orders.go
    └── 00003_add_users_age_column.go
```

## How It Works

Each migration file in `migrations/` registers itself via an `init()` function using `clicko.RegisterMigration`:

```go
func init() {
    clicko.RegisterMigration(upCreateUsers, downCreateUsers)
}
```

The migrations package is imported with a blank import in `main.go`, which triggers all `init()` functions automatically:

```go
import _ "github.com/arsura/clicko/example/go/migrations"
```

The migrator is created with `clicko.New`, which wires up the store and Go migration loader:

```go
migrator, err := clicko.New(conn, clicko.StoreConfig{
    TableName: "migration_versions",
})
```

## Cluster Mode

The example in `main.go` demonstrates cluster configuration via `StoreConfig`:

```go
migrator, err := clicko.New(conn, clicko.StoreConfig{
    TableName:    "migration_versions",
    Cluster:      "migration",
    CustomEngine: "ReplicatedMergeTree('/clickhouse/migration/table/all/{database}/{table}', '{replica}')",
    InsertQuorum: "2",
})
```

Each Go migration function can also include `ON CLUSTER` in its SQL statements:

```go
func upCreateUsers(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS users ON CLUSTER dev (
            ...
        ) ENGINE = ReplicatedMergeTree
        ORDER BY id
    `)
}
```

## Writing a New Migration

Create a new file in `migrations/` following the naming convention `{version}_{description}.go`:

```go
package migrations

import (
    "context"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/arsura/clicko"
)

func init() {
    clicko.RegisterMigration(upMyMigration, downMyMigration)
}

func upMyMigration(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS ...`)
}

func downMyMigration(ctx context.Context, conn clickhouse.Conn) error {
    return conn.Exec(ctx, `DROP TABLE IF EXISTS ...`)
}
```
