# clicko

A SQL migration tool built specifically for ClickHouse, with first-class support for clusters and `ReplicatedMergeTree`. Inspired by [Goose](https://github.com/pressly/goose).

## Features

- **Cluster-aware** -- `ON CLUSTER`, `ReplicatedMergeTree`, `insert_quorum`, `mutations_sync`
- **Custom engine** -- Override the default tracking table engine via CLI
- **Strict validation** -- Invalid migration files fail immediately, no silent skips
- **SQL injection prevention** -- All interpolated config values are validated before use
- **SQL-only** -- Simple `.sql` files, no DSL or annotations needed

## Installation

```bash
go install github.com/arsura/clicko/cmd/cli@latest
```

Or build from source:

```bash
go build -o clicko ./cmd/cli
```

## Quick Start

```bash
# Apply all pending migrations
clicko --uri "clickhouse://default:@localhost:9000/default" up

# Check migration status
clicko --uri "clickhouse://default:@localhost:9000/default" status
```

## Commands

| Command   | Description                                                   |
|-----------|---------------------------------------------------------------|
| `up`      | Apply all pending migrations                                  |
| `up-to`   | Apply migrations up to and including a specific version       |
| `down`    | Rollback the last applied migration                           |
| `down-to` | Rollback migrations down to (but not including) a version    |
| `reset`   | Rollback all applied migrations                               |
| `status`  | Show migration status                                         |

## Flags

| Flag              | Default              | Description                                              |
|-------------------|----------------------|----------------------------------------------------------|
| `--uri`           | *(required)*         | ClickHouse URI (e.g. `clickhouse://user:pass@host:9000/db`) |
| `--dir`           | `migrations`         | Directory containing migration files                     |
| `--table`         | `migration_versions` | Name of the migration tracking table                     |
| `--cluster`       | *(empty)*            | Cluster name (enables `ON CLUSTER` mode)                 |
| `--engine`        | *(auto)*             | Custom table engine for the tracking table               |
| `--insert-quorum` | *(empty)*            | Insert quorum for cluster writes (number or `auto`)      |

## Migration Files

Files must follow the naming convention:

```
<version>_<description>.<up|down>.sql
```

Example:

```
migrations/
├── 00001_create_users.up.sql
├── 00001_create_users.down.sql
├── 00002_create_orders.up.sql
└── 00002_create_orders.down.sql
```

Rules:

- Every `.sql` file must match the naming convention exactly
- Every version **must** have an `.up.sql` file; `.down.sql` is optional
- Up and down files for the same version must share the same description
- Non-`.sql` files and subdirectories are ignored

## Usage Examples

### Standalone

```bash
clicko \
  --uri "clickhouse://default:@localhost:9000/default" \
  --dir ./migrations \
  up
```

### Cluster Mode

```bash
clicko \
  --uri "clickhouse://default:@localhost:29000/default" \
  --dir ./migrations \
  --cluster "all-replicated" \
  --engine "ReplicatedMergeTree('/clickhouse/all-replicated/tables/all/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  up
```

## Tracking Table

The migrator creates a tracking table to record applied migrations:

```sql
CREATE TABLE migration_versions (
    version      UInt64,
    description  String,
    applied_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY version
```

Engine selection (when `--engine` is not set):

- Without `--cluster`: `MergeTree()`
- With `--cluster`: `ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')`

## Development

### Start ClickHouse Cluster

A local 2-shard x 2-replica cluster with ClickHouse Keeper is provided for development and testing:

```bash
make cluster-up        # Start cluster
make cluster-down      # Stop and remove volumes
make cluster-restart   # Restart cluster
make cluster-logs      # Follow logs
make cluster-status    # Show container status
```

Cluster topology:

| Container | Shard | Replica | TCP Port | HTTP Port |
|-----------|-------|---------|----------|-----------|
| ch-1-1    | 1     | 1       | 29000    | 28123     |
| ch-1-2    | 1     | 2       | 29001    | 28124     |
| ch-2-1    | 2     | 1       | 29002    | 28125     |
| ch-2-2    | 2     | 2       | 29003    | 28126     |

### Running Tests

Unit tests (no ClickHouse required):

```bash
go test . -v
```

Integration tests (requires cluster):

```bash
make cluster-up
go test ./cmd/cli/test/ -v
```

Integration tests skip automatically if ClickHouse is not running.

## Known Limitations

- ClickHouse does not support transactions. If a migration SQL succeeds but the tracking table write fails, the state may become inconsistent. Future plans include retry logic and `force-add`/`force-remove` commands for manual recovery.

## Roadmap

| Story | Description |
|-------|-------------|
| CLI | Run migrations from the command line |
| Custom Engine | Set custom engine via CLI, including ZooKeeper paths |
| Up, UpTo, Down, DownTo, Reset, Status | Core migration commands |
| Go Integration | Use as a Go library |
| Easy Go Interface | Simple API like `chmigrator.Up()` |

### Status

- [x] CLI: `up`, `up-to`, `down`, `down-to`, `reset`, `status`
- [x] Custom engine via `--engine` flag
- [x] Cluster mode (`--cluster`, `--insert-quorum`)
- [x] Strict migration file validation
- [x] Unit tests (loader) + Integration tests (CLI + ClickHouse cluster)
- [x] SQL injection prevention
- [x] Vulnerability check
- [x] Go integration
- 
- [ ] Easy Go interface

## License

[MIT](LICENSE)
