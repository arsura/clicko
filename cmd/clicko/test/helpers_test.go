package clicko_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	ch "github.com/arsura/clicko/internal/clickhouse"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Remove(filepath.Join(os.TempDir(), "clicko"))
	os.Remove(filepath.Join(os.TempDir(), "clicko.exe"))
	os.Exit(code)
}

const (
	testAdminURI     = "clickhouse://default:@localhost:29000/default"
	migrationCluster = "migration"
	customEngine     = "ReplicatedMergeTree('/clickhouse/migration/tables/all/{database}/{table}', '{replica}')"
	insertQuorum     = "4"

	testClusterMigrationTable              = "cluster_migration_versions"
	testStandaloneMigrationTable           = "standalone_migration_versions"
	testDryRunMigrationTable               = "dry_run_migration_versions"
	testForwardOnlyMigrationTable          = "forward_only_migration_versions"
	testDefaultEngineClusterMigrationTable = "default_engine_cluster_migration_versions"
	testOutOfOrderMigrationTable           = "out_of_order_migration_versions"
)

// flagsUsage is the shared flags section that appears in every usage context.
const flagsUsage = `Flags:
  -h, --help                    Show context-sensitive help.
  -v, --version                 Show version and quit.
      --uri=STRING              ClickHouse URI (e.g.
                                clickhouse://user:pass@host:9000/db)
      --dir="migrations"        Directory with migration files.
      --table="migration_versions"
                                Migrations table name.
      --cluster=STRING          ClickHouse cluster name (enables ON CLUSTER).
      --engine=STRING           Custom table engine (overrides default
                                MergeTree).
      --insert-quorum=STRING    Insert quorum for cluster writes (--cluster
                                required). Set to the total number of nodes in
                                the cluster (shards x replicas) so every node
                                acknowledges the write — this works because the
                                migration table is replicated across all nodes
                                via a single ZooKeeper path. Accepts a positive
                                integer or 'auto'.
      --dry-run                 Print the SQL each command would execute without
                                applying.
      --allow-out-of-order      Allow pending migrations with a lower version
                                than the highest applied version.
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

var (
	cachedBinaryPath string
	cachedBinaryErr  error
	buildOnce        sync.Once
)

// buildClicko compiles the CLI binary once per test process and returns its
// path. Subsequent calls reuse the cached binary without rebuilding. If the
// binary already exists on disk from a previous run it is reused directly,
// skipping the build step entirely.
func buildClicko(t *testing.T) string {
	t.Helper()

	buildOnce.Do(func() {
		binPath := filepath.Join(os.TempDir(), "clicko")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}

		if _, err := os.Stat(binPath); err == nil {
			cachedBinaryPath = binPath
			return
		}

		cmd := exec.Command("go", "build", "-o", binPath, "../.")
		cmd.Dir = testDir()
		out, err := cmd.CombinedOutput()
		if err != nil {
			cachedBinaryErr = fmt.Errorf("failed to build binary: %s", string(out))
			return
		}

		cachedBinaryPath = binPath
	})

	require.NoError(t, cachedBinaryErr, "clicko binary build failed")
	return cachedBinaryPath
}

// dialClickHouse opens a direct connection to ClickHouse for verification queries.
func dialClickHouse(t *testing.T) (clickhouse.Conn, func() error) {
	t.Helper()

	conn, cleanup, err := ch.Dial(context.Background(), testAdminURI)
	require.NoError(t, err)

	return conn, cleanup
}

// testURIWithDB returns a ClickHouse connection URI pointing to dbName.
func testURIWithDB(dbName string) string {
	return fmt.Sprintf("clickhouse://default:@localhost:29000/%s", dbName)
}

// createTestDB creates an isolated database for a single test case and returns
// its name. The name encodes the test name and a Unix timestamp so that
// databases left in ClickHouse can be traced back to the exact test and run
// that created them. Pass a non-empty onCluster to propagate the CREATE across
// a ClickHouse cluster.
func createTestDB(t *testing.T, conn clickhouse.Conn, onCluster string) string {
	t.Helper()

	safeName := testDBNameSanitizeRe.ReplaceAllString(t.Name(), "_")
	dbName := fmt.Sprintf("clicko_%s_%d", safeName, time.Now().Unix())

	q := "CREATE DATABASE IF NOT EXISTS " + dbName
	if onCluster != "" {
		q += " ON CLUSTER `" + onCluster + "`"
	}
	err := conn.Exec(context.Background(), q)
	require.NoError(t, err)

	return dbName
}

// runCLI executes the CLI binary with the given arguments and returns combined output.
func runCLI(binaryPath string, args ...string) (string, error) {
	cmd := exec.Command(binaryPath, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// args returns the common flags for cluster mode with the given command prepended.
func args(uri, migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", uri,
		"--dir", migrationsDir,
		"--cluster", migrationCluster,
		"--insert-quorum", insertQuorum,
		"--engine", customEngine,
		"--table", testClusterMigrationTable,
	)
	return args
}

// standaloneArgs returns the common flags for standalone (single node) mode.
func standaloneArgs(uri, migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", uri,
		"--dir", migrationsDir,
		"--table", testStandaloneMigrationTable,
	)
	return args
}

// dryRunArgs returns CLI flags for the dry-run test suite.
func dryRunArgs(uri, migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", uri,
		"--dir", migrationsDir,
		"--table", testDryRunMigrationTable,
	)
	return args
}

// outOfOrderArgs returns CLI flags for the out-of-order migration test suite.
func outOfOrderArgs(uri, migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", uri,
		"--dir", migrationsDir,
		"--table", testOutOfOrderMigrationTable,
	)
	return args
}

// forwardOnlyArgs returns CLI flags for the forward-only migration test suite.
func forwardOnlyArgs(uri, migrationsDir string, command ...string) []string {
	args := append(command,
		"--uri", uri,
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
// table may be a fully-qualified name (e.g. "dbname.tablename").
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
	testDBNameSanitizeRe = regexp.MustCompile(`/+`)

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
