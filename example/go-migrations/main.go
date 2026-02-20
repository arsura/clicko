package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"

	// Blank import triggers init() in each migration file,
	// which registers them via clicko.AddMigration.
	_ "github.com/arsura/clicko/example/go-migrations/migrations"
)

func main() {
	uri := "clickhouse://default:@localhost:9000/default"
	if v := os.Getenv("CLICKHOUSE_URI"); v != "" {
		uri = v
	}

	ctx := context.Background()

	// 1. Connect to ClickHouse
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

	// 2. Create migrator
	migrator, err := clicko.New(conn, clicko.StoreConfig{})
	if err != nil {
		fatal(err)
	}

	// 4. Run command based on first argument
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
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\nusage: go run . [up|down|reset|status]\n", cmd)
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
