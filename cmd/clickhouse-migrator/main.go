package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/arsura/clickhouse-migrator/internal/clickhouse"
	"github.com/arsura/clickhouse-migrator/pkg/migrator"
)

var (
	flags        = flag.NewFlagSet("clickhouse-migrator", flag.ExitOnError)
	dir          = flags.String("dir", "migrations", "directory with migration files")
	uri          = flags.String("uri", "", "ClickHouse URI (e.g. clickhouse://user:pass@host:9000/db)")
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

	cmd := parseCommandArgs(args)
	validateFlags()

	ctx := context.Background()

	conn, cleanup, err := clickhouse.Dial(ctx, *uri)
	if err != nil {
		exitf("failed to connect to ClickHouse: %v", err)
	}
	defer cleanup()

	loader := migrator.NewFileLoader(*dir)
	store, err := migrator.NewStore(conn, migrator.StoreConfig{
		TableName:    *table,
		Cluster:      *cluster,
		CustomEngine: *customEngine,
		InsertQuorum: *insertQuorum,
	})
	if err != nil {
		exitf("invalid store config: %v", err)
	}
	m := migrator.NewMigrator(conn, loader, store)

	switch cmd.command {
	case migrator.MigrationDirectionUp:
		if err := m.Up(ctx); err != nil {
			exitf("up failed: %v", err)
		}
	case migrator.MigrationDirectionUpTo:
		if err := m.UpTo(ctx, cmd.version); err != nil {
			exitf("up-to failed: %v", err)
		}
	case migrator.MigrationDirectionDown:
		if err := m.Down(ctx); err != nil {
			exitf("down failed: %v", err)
		}
	case migrator.MigrationDirectionDownTo:
		if err := m.DownTo(ctx, cmd.version); err != nil {
			exitf("down-to failed: %v", err)
		}
	case migrator.MigrationDirectionReset:
		if err := m.Reset(ctx); err != nil {
			exitf("reset failed: %v", err)
		}
	}
}

type commandArgs struct {
	command string
	version uint64
}

// parseCommandArgs validates the command name and parses any required
// positional arguments (e.g. VERSION for up-to / down-to).
func parseCommandArgs(args []string) commandArgs {
	command := args[0]

	switch command {
	case migrator.MigrationDirectionUp, migrator.MigrationDirectionDown, migrator.MigrationDirectionReset:
		return commandArgs{command: command}

	case migrator.MigrationDirectionUpTo, migrator.MigrationDirectionDownTo:
		if len(args) < 2 {
			exitf("version is required")
		}
		ver, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			exitf("version must be a positive number")
		}
		return commandArgs{command: command, version: ver}

	default:
		exitf("unknown command: %s", command)
		return commandArgs{}
	}
}

func validateFlags() {
	if *uri == "" {
		exitf("uri is required")
	}
}

func exitf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	flags.Usage()
	os.Exit(1)
}

func usage() {
	fmt.Println(`Usage: clickhouse-migrator [OPTIONS] COMMAND

Commands:
    up                  Apply all pending migrations
    up-to <version>     Apply migrations up to a specific version
    down                Rollback the last applied migration
    down-to <version>   Rollback migrations down to a specific version
    reset               Rollback all applied migrations

Options:`)
	flags.PrintDefaults()
}
