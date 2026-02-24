package clicko_test

import (
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIDryRunSuite is an integration test suite dedicated to --dry-run behaviour.
// It uses the same standalone testdata migrations but records state in a
// separate tracking table so it never collides with CLIStandaloneSuite.
//
// Prerequisites: ClickHouse must be running on localhost:29000.
// Start it with: make cluster-up
type CLIDryRunSuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error
	migrationsDir         string

	testDBName string
	testDBURI  string
}

func TestCLIDryRunSuite(t *testing.T) {
	suite.Run(t, new(CLIDryRunSuite))
}

func (s *CLIDryRunSuite) SetupSuite() {
	s.binaryPath = buildClicko(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "standalone")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIDryRunSuite) TearDownSuite() {
	s.clickHouseCleanupFunc()
}

func (s *CLIDryRunSuite) SetupTest() {
	s.testDBName = createTestDB(s.T(), s.conn, "")
	s.testDBURI = testURIWithDB(s.testDBName)
}

// ---------------------------------------------------------------------------
// Expected dry-run SQL fragments
// ---------------------------------------------------------------------------

const dryRunUpMigration1 = "=== Version 1: create test table (sql) ===\n" +
	"CREATE TABLE IF NOT EXISTS standalone_table (\n" +
	"    id UInt64,\n" +
	"    name String\n" +
	") ENGINE = MergeTree()\n" +
	"ORDER BY id;\n\n"

const dryRunUpMigration2 = "=== Version 2: add email column (sql) ===\n" +
	"ALTER TABLE standalone_table ADD COLUMN IF NOT EXISTS email String DEFAULT '';\n\n"

const dryRunUpMigration3 = "=== Version 3: add age column (sql) ===\n" +
	"ALTER TABLE standalone_table ADD COLUMN IF NOT EXISTS age UInt32 DEFAULT 0;\n\n"

const dryRunDownMigration1 = "=== Version 1: create test table (sql) ===\n" +
	"DROP TABLE IF EXISTS standalone_table;\n\n"

const dryRunDownMigration2 = "=== Version 2: add email column (sql) ===\n" +
	"ALTER TABLE standalone_table DROP COLUMN IF EXISTS email;\n\n"

const dryRunDownMigration3 = "=== Version 3: add age column (sql) ===\n" +
	"ALTER TABLE standalone_table DROP COLUMN IF EXISTS age;\n\n"

// ---------------------------------------------------------------------------
// Up --dry-run
// ---------------------------------------------------------------------------

func (s *CLIDryRunSuite) TestUpAllPending() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunUpMigration1+dryRunUpMigration2+dryRunUpMigration3, out)
}

func (s *CLIDryRunSuite) TestUpToTargetVersion() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up-to", "2", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunUpMigration1+dryRunUpMigration2, out)
}

func (s *CLIDryRunSuite) TestUpToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up-to", "999", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunUpMigration1+dryRunUpMigration2+dryRunUpMigration3, out)
}

func (s *CLIDryRunSuite) TestUpNoPending() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n", normalizeOutput(out))
}

func (s *CLIDryRunSuite) TestUpPartiallyApplied() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunUpMigration3, out)
}

func (s *CLIDryRunSuite) TestUpDoesNotModifyState() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunUpMigration1+dryRunUpMigration2+dryRunUpMigration3, out)

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testDryRunMigrationTable)
	require.Empty(s.T(), actual, "dry-run must not apply any migrations")
}

// ---------------------------------------------------------------------------
// Down --dry-run
// ---------------------------------------------------------------------------

func (s *CLIDryRunSuite) TestDownLastMigration() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "down", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunDownMigration3, out)
}

func (s *CLIDryRunSuite) TestDownToTargetVersion() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "down-to", "1", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunDownMigration3+dryRunDownMigration2, out)
}

func (s *CLIDryRunSuite) TestResetAllApplied() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "reset", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunDownMigration3+dryRunDownMigration2+dryRunDownMigration1, out)
}

func (s *CLIDryRunSuite) TestResetOnEmptyState() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "reset", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n", normalizeOutput(out))
}

func (s *CLIDryRunSuite) TestDownToVersionBeyondMax() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "down-to", "999", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n", normalizeOutput(out))
}

func (s *CLIDryRunSuite) TestDownNothingToRevert() {
	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "down", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n", normalizeOutput(out))
}

func (s *CLIDryRunSuite) TestDownDoesNotModifyState() {
	_, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err)

	out, err := runCLI(s.binaryPath, dryRunArgs(s.testDBURI, s.migrationsDir, "reset", "--dry-run")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), dryRunDownMigration3+dryRunDownMigration2+dryRunDownMigration1, out)

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testDryRunMigrationTable)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}
