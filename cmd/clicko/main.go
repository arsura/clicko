package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kong"

	"github.com/arsura/clicko"
	"github.com/arsura/clicko/internal/clickhouse"
)

type CLI struct {
	URI          string `help:"ClickHouse URI (e.g. clickhouse://user:pass@host:9000/db)" required:"" name:"uri"`
	Dir          string `help:"Directory with migration files." default:"migrations" name:"dir"`
	Table        string `help:"Migrations table name." default:"migration_versions" name:"table"`
	Cluster      string `help:"ClickHouse cluster name (enables ON CLUSTER)." name:"cluster"`
	Engine       string `help:"Custom table engine (overrides default MergeTree)." name:"engine"`
	InsertQuorum string `help:"Insert quorum for cluster writes (--cluster required). Set to the total number of nodes in the cluster (shards x replicas) so every node acknowledges the write — this works because the migration table is replicated across all nodes via a single ZooKeeper path. Accepts a positive integer or 'auto'." name:"insert-quorum"`
	DryRun       bool   `help:"Print the SQL each command would execute without applying." name:"dry-run"`

	Up     UpCmd     `cmd:"" help:"Apply all pending migrations."`
	UpTo   UpToCmd   `cmd:"up-to" help:"Apply migrations up to a specific version."`
	Down   DownCmd   `cmd:"" help:"Rollback the last applied migration."`
	DownTo DownToCmd `cmd:"down-to" help:"Rollback migrations down to a specific version."`
	Reset  ResetCmd  `cmd:"" help:"Rollback all applied migrations."`
	Status StatusCmd `cmd:"" help:"Show migration status."`
}

type UpCmd struct{}
type UpToCmd struct {
	Version uint64 `arg:"" required:"" help:"Target version."`
}
type DownCmd struct{}
type DownToCmd struct {
	Version uint64 `arg:"" required:"" help:"Target version."`
}
type ResetCmd struct{}
type StatusCmd struct{}

func (c *UpCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.Up(ctx)
	})
}

func (c *UpToCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.UpTo(ctx, c.Version)
	})
}

func (c *DownCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.Down(ctx)
	})
}

func (c *DownToCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.DownTo(ctx, c.Version)
	})
}

func (c *ResetCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.Reset(ctx)
	})
}

func (c *StatusCmd) Run(globals *CLI) error {
	return run(globals, func(ctx context.Context, m *clicko.Migrator) error {
		return m.Status(ctx)
	})
}

func run(globals *CLI, fn func(context.Context, *clicko.Migrator) error) error {
	ctx := context.Background()

	conn, cleanup, err := clickhouse.Dial(ctx, globals.URI)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer cleanup()

	loader := clicko.NewSQLLoader(globals.Dir)
	store, err := clicko.NewStore(conn, clicko.StoreConfig{
		TableName:    globals.Table,
		Cluster:      globals.Cluster,
		CustomEngine: globals.Engine,
		InsertQuorum: globals.InsertQuorum,
	})
	if err != nil {
		return fmt.Errorf("invalid store config: %w", err)
	}

	m := clicko.NewMigrator(conn, loader, store)
	if globals.DryRun {
		m.SetDryRun(true)
	}
	return fn(ctx, m)
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli, kong.UsageOnError())
	err := ctx.Run(&cli)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
