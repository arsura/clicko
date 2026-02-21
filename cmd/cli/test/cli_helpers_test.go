package cli_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	ch "github.com/arsura/clicko/internal/clickhouse"
	"github.com/stretchr/testify/require"
)

const (
	testURI          = "clickhouse://default:@localhost:29000/default"
	migrationCluster = "migration"
	dataCluster      = "dev"
	customEngine     = "ReplicatedMergeTree('/clickhouse/migration/tables/all/{database}/{table}', '{replica}')"
	insertQuorum     = "4"

	testClusterMigrationTable              = "cluster_migration_versions"
	testStandaloneMigrationTable           = "standalone_migration_versions"
	testForwardOnlyMigrationTable          = "forward_only_migration_versions"
	testDefaultEngineClusterMigrationTable = "default_engine_cluster_migration_versions"

	clusterDataTable     = "cluster_table"
	standaloneDataTable  = "standalone_table"
	forwardOnlyDataTable = "forward_only_table"
)

// flagsUsage is the shared flags section that appears in every usage context.
const flagsUsage = `Flags:
  -h, --help                    Show context-sensitive help.
      --uri=STRING              ClickHouse URI (e.g.
                                clickhouse://user:pass@host:9000/db)
      --dir="migrations"        Directory with migration files.
      --table="migration_versions"
                                Migrations table name.
      --cluster=STRING          ClickHouse cluster name (enables ON CLUSTER).
      --engine=STRING           Custom table engine (overrides default
                                MergeTree).
      --insert-quorum=STRING    Insert quorum for cluster writes.
`

// globalUsage is the full help text shown when no command is given or an unknown command is used.
const globalUsage = `Usage: clicko --uri=STRING <command> [flags]

` + flagsUsage + `
Commands:
  up --uri=STRING [flags]
    Apply all pending migrations.

  up-to --uri=STRING <version> [flags]
    Apply migrations up to a specific version.

  down --uri=STRING [flags]
    Rollback the last applied migration.

  down-to --uri=STRING <version> [flags]
    Rollback migrations down to a specific version.

  reset --uri=STRING [flags]
    Rollback all applied migrations.

  status --uri=STRING [flags]
    Show migration status.

Run "clicko <command> --help" for more information on a command.
`

// upCmdUsage is the help text shown for the "up" command.
const upCmdUsage = `Usage: clicko up --uri=STRING [flags]

Apply all pending migrations.

` + flagsUsage

// upToCmdUsage is the help text shown for the "up-to" command.
const upToCmdUsage = `Usage: clicko up-to --uri=STRING <version> [flags]

Apply migrations up to a specific version.

Arguments:
  <version>    Target version.

` + flagsUsage

// downToCmdUsage is the help text shown for the "down-to" command.
const downToCmdUsage = `Usage: clicko down-to --uri=STRING <version> [flags]

Rollback migrations down to a specific version.

Arguments:
  <version>    Target version.

` + flagsUsage

// testDir returns the absolute path of the directory containing this test file.
func testDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filename)
}

// buildCLI compiles the CLI binary into a temp directory and returns its path.
func buildCLI(t *testing.T) string {
	t.Helper()

	binPath := filepath.Join(t.TempDir(), "clicko")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	cmd := exec.Command("go", "build", "-o", binPath, "../.")
	cmd.Dir = testDir()
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to build binary: %s", string(out))

	return binPath
}

// dialClickHouse opens a direct connection to ClickHouse for verification queries.
func dialClickHouse(t *testing.T) (clickhouse.Conn, func() error) {
	t.Helper()

	conn, cleanup, err := ch.Dial(context.Background(), testURI)
	require.NoError(t, err)

	return conn, cleanup
}

// runCLI executes the CLI binary with the given arguments and returns combined output.
func runCLI(binaryPath string, args ...string) (string, error) {
	cmd := exec.Command(binaryPath, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// cliArgs returns the common flags for cluster mode with the given command prepended.
func cliArgs(migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", testURI,
		"--dir", migrationsDir,
		"--cluster", migrationCluster,
		"--insert-quorum", insertQuorum,
		"--engine", customEngine,
		"--table", testClusterMigrationTable,
	)
	return args
}

// standaloneCliArgs returns the common flags for standalone (single node) mode.
func standaloneCliArgs(migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", testURI,
		"--dir", migrationsDir,
		"--table", testStandaloneMigrationTable,
	)
	return args
}

// forwardOnlyCliArgs returns CLI flags for the forward-only migration test suite.
func forwardOnlyCliArgs(migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", testURI,
		"--dir", migrationsDir,
		"--table", testForwardOnlyMigrationTable,
	)
	return args
}

// appliedMigration represents a row from the migration tracking table.
type appliedMigration struct {
	Version     uint64
	Description string
	AppliedAt   time.Time
}

// queryAppliedMigrationsFrom returns all rows from the given tracking table sorted by version ascending.
func queryAppliedMigrationsFrom(t *testing.T, conn clickhouse.Conn, table string) []appliedMigration {
	t.Helper()

	rows, err := conn.Query(context.Background(),
		"SELECT version, description, applied_at FROM "+table+" ORDER BY version")
	require.NoError(t, err)
	defer rows.Close()

	var migrations []appliedMigration
	for rows.Next() {
		var m appliedMigration
		require.NoError(t, rows.Scan(&m.Version, &m.Description, &m.AppliedAt))
		migrations = append(migrations, m)
	}

	return migrations
}

// queryAppliedMigrations returns all rows from the default tracking table sorted by version ascending.
func queryAppliedMigrations(t *testing.T, conn clickhouse.Conn) []appliedMigration {
	t.Helper()

	rows, err := conn.Query(context.Background(),
		"SELECT version, description, applied_at FROM "+testClusterMigrationTable+" ORDER BY version")
	require.NoError(t, err)
	defer rows.Close()

	var migrations []appliedMigration
	for rows.Next() {
		var m appliedMigration
		require.NoError(t, rows.Scan(&m.Version, &m.Description, &m.AppliedAt))
		migrations = append(migrations, m)
	}

	return migrations
}

// assertAppliedMigrations verifies that the actual rows match the expected
// version and description, and that applied_at is populated.
func assertAppliedMigrations(t *testing.T, actual []appliedMigration, expected []appliedMigration) {
	t.Helper()

	require.Len(t, actual, len(expected))
	for i := range expected {
		require.Equal(t, expected[i].Version, actual[i].Version, "version mismatch at index %d", i)
		require.Equal(t, expected[i].Description, actual[i].Description, "description mismatch at index %d", i)
		require.NotZero(t, actual[i].AppliedAt, "applied_at should not be zero at index %d", i)
	}
}

var (
	logTimestampRe    = regexp.MustCompile(`\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)
	okDurationRe      = regexp.MustCompile(`OK \([^)]+\)`)
	statusTimestampRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`)
)

// normalizeOutput strips log timestamp prefixes and replaces "OK (<duration>)"
// with "OK" so that CLI output can be compared deterministically.
func normalizeOutput(s string) string {
	s = logTimestampRe.ReplaceAllString(s, "")
	s = okDurationRe.ReplaceAllString(s, "OK")
	s = statusTimestampRe.ReplaceAllString(s, "APPLIED_AT         ")
	return s
}
