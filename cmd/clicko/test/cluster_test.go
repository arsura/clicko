package clicko_test

import (
	"context"
	"path/filepath"
	"testing"

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

	// testDBName and testDBURI are set fresh for every test case so that each
	// test operates on an entirely isolated database. No cleanup is needed
	// between tests — the whole environment is torn down via docker-compose.
	testDBName string
	testDBURI  string
}

func TestCLIClusterSuite(t *testing.T) {
	suite.Run(t, new(CLIClusterSuite))
}

func (s *CLIClusterSuite) SetupSuite() {
	s.binaryPath = buildClicko(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "cluster")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIClusterSuite) TearDownSuite() {
	s.clickHouseCleanupFunc()
}

func (s *CLIClusterSuite) SetupTest() {
	s.testDBName = createTestDB(s.T(), s.conn, migrationCluster)
	s.testDBURI = testURIWithDB(s.testDBName)
}

// ---------------------------------------------------------------------------
// Up
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestUpAppliesAllMigrations() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// expectedMigrations defines the expected state for all three test migrations.
var expectedMigrations = []appliedMigration{
	{Version: 1, Description: "create test table"},
	{Version: 2, Description: "add email column"},
	{Version: 3, Description: "add age column"},
}

func (s *CLIClusterSuite) TestUpIdempotent() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Up-to
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestUpToTargetVersion() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIClusterSuite) TestUpToAlreadyApplied() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIClusterSuite) TestUpToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up-to", "999")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Down
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestDownRevertsLastMigration() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIClusterSuite) TestDownNothingToRevert() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Down-to
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestDownToTargetVersion() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down-to", "1")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:1])
}

// TestDownToZeroRejected pins that the internal target=0 "no bound" sentinel
// is not reachable from the CLI: version 0 can never be a real migration, so
// a 0 target is treated as a typo instead of silently running a full reset.
func (s *CLIClusterSuite) TestDownToZeroRejected() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down-to", "0")...)
	require.Error(s.T(), err)
	require.Equal(s.T(),
		"target version 0 is reserved (migration versions start at 1); to revert all applied migrations use \"reset\" (CLI) or Reset (Go)\n",
		out)

	// Nothing may have been reverted.
	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIClusterSuite) TestDownToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down-to", "999")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIClusterSuite) TestDownToOnEmptyState() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down-to", "1")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestResetRevertsAllMigrations() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n"+
		"Reverting migration 1: create test table\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	require.Empty(s.T(), actual)
}

func (s *CLIClusterSuite) TestResetOnEmptyState() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

const statusHeader = "Version    Description               Status     Applied At\n" +
	"----------------------------------------------------------------------\n"

func (s *CLIClusterSuite) TestStatusDoesNotCreateTrackingTable() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "cli output: %s", out)

	assertTableNotExists(s.T(), s.conn, s.testDBName, testClusterMigrationTable)
}

func (s *CLIClusterSuite) TestStatusAllPending() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Pending    \n"+
		"2          add email column          Pending    \n"+
		"3          add age column            Pending    \n",
		normalizeOutput(out))
}

func (s *CLIClusterSuite) TestStatusAllApplied() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "status: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Applied    APPLIED_AT         \n"+
		"2          add email column          Applied    APPLIED_AT         \n"+
		"3          add age column            Applied    APPLIED_AT         \n",
		normalizeOutput(out))
}

func (s *CLIClusterSuite) TestStatusReadsStateWhenTableMissingOnSomeReplicas() {
	err := s.conn.Exec(context.Background(),
		"CREATE TABLE "+s.testDBName+"."+testClusterMigrationTable+
			" (version UInt64, description String, applied_at DateTime64(6) DEFAULT now64(6))"+
			" ENGINE = MergeTree() ORDER BY version")
	require.NoError(s.T(), err)
	err = s.conn.Exec(context.Background(),
		"INSERT INTO "+s.testDBName+"."+testClusterMigrationTable+
			" (version, description) VALUES (1, 'create test table')")
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "status: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Applied    APPLIED_AT         \n"+
		"2          add email column          Pending    \n"+
		"3          add age column            Pending    \n",
		normalizeOutput(out))
}

func (s *CLIClusterSuite) TestStatusPartiallyApplied() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "status")...)
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
	out, err := runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, args(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// TestUpWithDefaultClusterEngine verifies that EnsureTable uses the built-in
// ReplicatedMergeTree path when --cluster is set but --engine is omitted.
func (s *CLIClusterSuite) TestUpWithDefaultClusterEngine() {
	args := []string{
		"up",
		"--uri", s.testDBURI,
		"--dir", s.migrationsDir,
		"--cluster", migrationCluster,
		"--table", testDefaultEngineClusterMigrationTable,
		// deliberately omitting --engine to exercise the defaultClusterEngine branch
		// deliberately omitting --insert-quorum since this test is about engine selection only
	}
	out, err := runCLI(s.binaryPath, args...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(),
		"Warning: cluster mode without insert quorum; a node that misses a migration write may re-run the migration — set InsertQuorum (Go) or --insert-quorum (CLI) to the total number of nodes (shards × replicas) or \"auto\"\n"+
			"Warning: no custom engine specified for cluster mode; falling back to the default engine whose ZooKeeper path includes {shard}, which may result in separate replication groups per shard and inconsistent migration state across nodes — set a custom engine with a unified ZooKeeper path to avoid this\n"+
			"Applying migration 1: create test table\n"+
			"OK\n"+
			"Applying migration 2: add email column\n"+
			"OK\n"+
			"Applying migration 3: add age column\n"+
			"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testDefaultEngineClusterMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func (s *CLIClusterSuite) TestInvalidMigrationsDir() {
	out, err := runCLI(s.binaryPath, args(s.testDBURI, "/nonexistent/path", "up")...)
	require.Error(s.T(), err)
	require.Equal(s.T(),
		"failed to load migrations: failed to read migrations directory \"/nonexistent/path\": open /nonexistent/path: no such file or directory\n",
		out)
}
