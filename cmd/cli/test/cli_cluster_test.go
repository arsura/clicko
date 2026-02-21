package cli_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIClusterSuite is an integration test suite that builds the real CLI binary
// and runs it against a live ClickHouse cluster.
//
// Prerequisites: ClickHouse cluster must be running.
// Start it with: make cluster-up
type CLIClusterSuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error
	migrationsDir         string
}

func TestCLISuite(t *testing.T) {
	suite.Run(t, new(CLIClusterSuite))
}

func (s *CLIClusterSuite) SetupSuite() {
	s.binaryPath = buildCLI(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "cluster")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIClusterSuite) TearDownSuite() {
	s.cleanup()
	s.clickHouseCleanupFunc()
}

func (s *CLIClusterSuite) SetupTest() {
	s.cleanup()
}

// cleanup drops test tables and removes any orphaned ZooKeeper replica
// entries that ClickHouse may leave behind after an asynchronous DROP.
// test_cluster_migration is created by migration SQL on cluster "dev",
// while the tracking table uses the "migration" cluster.
func (s *CLIClusterSuite) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := s.conn.Exec(ctx, "DROP TABLE IF EXISTS test_cluster_migration ON CLUSTER `"+dataCluster+"` SYNC")
	require.NoError(s.T(), err)
	err = s.conn.Exec(ctx, "DROP TABLE IF EXISTS `"+clusterTableName+"` ON CLUSTER `"+migrationCluster+"` SYNC")
	require.NoError(s.T(), err)

	s.dropOrphanedReplicas(ctx, "/clickhouse/tables/1/test_cluster_migration")
	s.dropOrphanedReplicas(ctx, "/clickhouse/tables/2/test_cluster_migration")
}

// dropOrphanedReplicas queries ZooKeeper for any remaining replica entries
// under the given path and removes them with SYSTEM DROP REPLICA.
func (s *CLIClusterSuite) dropOrphanedReplicas(ctx context.Context, zkPath string) {
	rows, err := s.conn.Query(ctx,
		"SELECT name FROM system.zookeeper WHERE path = $1",
		zkPath+"/replicas")
	require.NoError(s.T(), err)
	defer rows.Close()

	for rows.Next() {
		var replica string
		if err := rows.Scan(&replica); err != nil {
			continue
		}
		err = s.conn.Exec(ctx, fmt.Sprintf(
			"SYSTEM DROP REPLICA '%s' FROM ZKPATH '%s'", replica, zkPath))
		require.NoError(s.T(), err)
	}
}

// ---------------------------------------------------------------------------
// Up
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestUpAppliesAllMigrations() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// expectedMigrations defines the expected state for all three test migrations.
var expectedMigrations = []appliedMigration{
	{Version: 1, Description: "create test table"},
	{Version: 2, Description: "add email column"},
	{Version: 3, Description: "add age column"},
}

func (s *CLIClusterSuite) TestUpIdempotent() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Up-to
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestUpToTargetVersion() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIClusterSuite) TestUpToAlreadyApplied() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIClusterSuite) TestUpToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up-to", "999")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Down
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestDownRevertsLastMigration() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIClusterSuite) TestDownNothingToRevert() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Down-to
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestDownToTargetVersion() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down-to", "1")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:1])
}

func (s *CLIClusterSuite) TestDownToZeroRevertsAll() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down-to", "0")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n"+
		"Reverting migration 1: create test table\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	require.Empty(s.T(), actual)
}

func (s *CLIClusterSuite) TestDownToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down-to", "999")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIClusterSuite) TestDownToOnEmptyState() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down-to", "0")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestResetRevertsAllMigrations() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n"+
		"Reverting migration 1: create test table\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	require.Empty(s.T(), actual)
}

func (s *CLIClusterSuite) TestResetOnEmptyState() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

const statusHeader = "Version    Description               Status     Applied At\n" +
	"----------------------------------------------------------------------\n"

func (s *CLIClusterSuite) TestStatusAllPending() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Pending    \n"+
		"2          add email column          Pending    \n"+
		"3          add age column            Pending    \n",
		normalizeOutput(out))
}

func (s *CLIClusterSuite) TestStatusAllApplied() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "status: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Applied    APPLIED_AT         \n"+
		"2          add email column          Applied    APPLIED_AT         \n"+
		"3          add age column            Applied    APPLIED_AT         \n",
		normalizeOutput(out))
}

func (s *CLIClusterSuite) TestStatusPartiallyApplied() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "status: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Applied    APPLIED_AT         \n"+
		"2          add email column          Applied    APPLIED_AT         \n"+
		"3          add age column            Pending    \n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Combined
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestUpThenDownThenUpAgain() {
	out, err := runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, cliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrations(s.T(), s.conn)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestInvalidMigrationsDir() {
	out, err := runCLI(s.binaryPath, cliArgs("/nonexistent/path", "up")...)
	require.Error(s.T(), err)
	require.Equal(s.T(),
		"failed to load migrations: failed to read migrations directory \"/nonexistent/path\": open /nonexistent/path: no such file or directory\n",
		out)
}
