package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clickhouse-migrator/pkg/migrator"
)

var (
	flags        = flag.NewFlagSet("clickhouse-migrator", flag.ExitOnError)
	dir          = flags.String("dir", "migrations", "directory with migration files")
	dsn          = flags.String("dsn", "", "ClickHouse DSN (e.g. clickhouse://user:pass@host:9000/db)")
	table        = flags.String("table", migrator.DefaultTableName, "migrations table name")
	cluster      = flags.String("cluster", "", "ClickHouse cluster name (enables ON CLUSTER)")
	customEngine = flags.String("engine", "", "custom table engine (overrides default MergeTree)")
	insertQuorum = flags.String("insert-quorum", "", "insert quorum for cluster writes")
	help         = flags.Bool("help", false, "print help")
)

func main() {
	flags.Usage = usage
	flags.Parse(os.Args[1:])

	if *help {
		flags.Usage()
		return
	}

	args := flags.Args()
	if len(args) < 1 {
		flags.Usage()
		os.Exit(1)
	}

	command := args[0]

	if *dsn == "" {
		log.Fatal("--dsn is required")
	}

	options, err := clickhouse.ParseDSN(*dsn)
	if err != nil {
		log.Fatalf("failed to parse DSN: %v", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}

	loader := migrator.NewFileLoader(*dir)
	store, err := migrator.NewStore(conn, migrator.StoreConfig{
		TableName:    *table,
		Cluster:      *cluster,
		CustomEngine: *customEngine,
		InsertQuorum: *insertQuorum,
	})
	if err != nil {
		log.Fatalf("invalid store config: %v", err)
	}
	m := migrator.NewMigrator(conn, loader, store)

	switch command {
	case "up":
		if err := m.Up(ctx); err != nil {
			log.Fatalf("up failed: %v", err)
		}
	case "up-to":
		if len(args) < 2 {
			log.Fatal("usage: up-to VERSION")
		}
		ver, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			log.Fatalf("invalid version: %v", err)
		}
		if err := m.UpTo(ctx, ver); err != nil {
			log.Fatalf("up-to failed: %v", err)
		}
	case "down":
		if err := m.Down(ctx); err != nil {
			log.Fatalf("down failed: %v", err)
		}
	case "down-to":
		if len(args) < 2 {
			log.Fatal("usage: down-to VERSION")
		}
		ver, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			log.Fatalf("invalid version: %v", err)
		}
		if err := m.DownTo(ctx, ver); err != nil {
			log.Fatalf("down-to failed: %v", err)
		}
	default:
		fmt.Printf("unknown command: %s\n", command)
		flags.Usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`Usage: clickhouse-migrator [OPTIONS] COMMAND

Commands:
    up          Apply all pending migrations
    up-to       Apply migrations up to a specific version
    down        Rollback the last applied migration
    down-to     Rollback migrations down to a specific version

Options:`)
	flags.PrintDefaults()
}
