package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

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
	if len(os.Args) > 1 {
		cmd = os.Args[1]
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
	case "dry-run":
		if len(os.Args) > 2 {
			v, parseErr := strconv.ParseUint(os.Args[2], 10, 64)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "invalid version: %s\n", os.Args[2])
				os.Exit(1)
			}
			err = migrator.DryRunTo(ctx, v)
		} else {
			err = migrator.DryRun(ctx)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\nusage: go run . [up|down|reset|status|dry-run]\n", cmd)
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
