package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"

	// Blank import triggers init() in each migration file,
	// which registers them via clicko.AddMigration.
	_ "github.com/arsura/clicko/example/go/migrations"
)

func main() {
	ctx := context.Background()

	uri := "clickhouse://default:@localhost:29000/default"

	opts, err := clickhouse.ParseDSN(uri)
	if err != nil {
		fatal(err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		fatal(err)
	}
	if err := conn.Ping(ctx); err != nil {
		fatal(err)
	}
	defer conn.Close()

	migrator, err := clicko.New(conn, clicko.StoreConfig{
		TableName:    "migration_versions",
		Cluster:      "migration",
		CustomEngine: "ReplicatedMergeTree('/clickhouse/migration/table/all/{database}/{table}', '{replica}')",
		InsertQuorum: "4",
	})
	if err != nil {
		fatal(err)
	}

	cmd := "up"
	dryRun := false
	for _, arg := range os.Args[1:] {
		if arg == "--dry-run" {
			dryRun = true
		} else {
			cmd = arg
		}
	}

	if dryRun {
		migrator.SetDryRun(true)
	}

	switch cmd {
	case "up":
		err = migrator.Up(ctx)
	case "down":
		err = migrator.Down(ctx)
	case "reset":
		err = migrator.Reset(ctx)
	case "status":
		err = migrator.Status(ctx)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\nusage: go run . [up|down|reset|status] [--dry-run]\n", cmd)
		os.Exit(1)
	}

	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
