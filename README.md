# ClickHouse Migrator

A dedicated migration tool for ClickHouse, supporting Cluster and ReplicatedMergeTree.

## Installation

```bash
go install github.com/siwakorn-r/clickhouse-migrator/cmd/clickhouse-migrator@latest
```

## Usage

### CLI

The tool expects migration files to be in the format `<timestamp>_<name>.up.sql` and `<timestamp>_<name>.down.sql`.

```bash
export CLICKHOUSE_DSN="clickhouse://user:password@localhost:9000/db"

# Create a migration
clickhouse-migrator create add_users_table

# Apply migrations
clickhouse-migrator up

# Revert last migration
clickhouse-migrator down

# Check status
clickhouse-migrator status
```

### Cluster Support

The migrator supports `ON CLUSTER` execution. Ensure your DSN connects to a node in the cluster.
Configure the following environment variables if you are using `ReplicatedMergeTree`:

- `CLICKHOUSE_CLUSTER`: Your cluster name (e.g. `{cluster}`).
- `CLICKHOUSE_REPLICATED_MERGE_TREE_ZOOKEEPER_PATH`: ZooKeeper path.
- `CLICKHOUSE_REPLICATED_MERGE_TREE_REPLICA_NAME`: Replica name.
