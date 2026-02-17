package cli_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIStandaloneSuite is an integration test suite that tests standalone
// (single node) mode — no --cluster, --engine, or --insert-quorum flags.
//
// Prerequisites: ClickHouse must be running on localhost:29000.
// Start it with: make cluster-up
type CLIStandaloneSuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error
	migrationsDir         string
}

func TestCLIStandaloneSuite(t *testing.T) {
	suite.Run(t, new(CLIStandaloneSuite))
}

func (s *CLIStandaloneSuite) SetupSuite() {
	s.binaryPath = buildCLI(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "standalone")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIStandaloneSuite) TearDownSuite() {
	s.cleanup()
	s.clickHouseCleanupFunc()
}

func (s *CLIStandaloneSuite) SetupTest() {
	s.cleanup()
}

func (s *CLIStandaloneSuite) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := s.conn.Exec(ctx, "DROP TABLE IF EXISTS test_standalone_migration SYNC")
	require.NoError(s.T(), err)
	err = s.conn.Exec(ctx, "DROP TABLE IF EXISTS `"+standaloneTableName+"` SYNC")
	require.NoError(s.T(), err)
}

// ---------------------------------------------------------------------------
// Up
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestUpAppliesAllMigrations() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIStandaloneSuite) TestUpIdempotent() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Up-to
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestUpToTargetVersion() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIStandaloneSuite) TestUpToAlreadyApplied() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)
	require.Equal(s.T(), "No pending migrations to apply\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIStandaloneSuite) TestUpToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up-to", "999")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Down
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestDownRevertsLastMigration() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:2])
}

func (s *CLIStandaloneSuite) TestDownNothingToRevert() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Down-to
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestDownToTargetVersion() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down-to", "1")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations[:1])
}

func (s *CLIStandaloneSuite) TestDownToZeroRevertsAll() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down-to", "0")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n"+
		"Reverting migration 1: create test table\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	require.Empty(s.T(), actual)
}

func (s *CLIStandaloneSuite) TestDownToVersionBeyondMax() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down-to", "999")...)
	require.NoError(s.T(), err, "down-to: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

func (s *CLIStandaloneSuite) TestDownToOnEmptyState() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down-to", "0")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestResetRevertsAllMigrations() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n"+
		"Reverting migration 2: add email column\n"+
		"OK\n"+
		"Reverting migration 1: create test table\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	require.Empty(s.T(), actual)
}

func (s *CLIStandaloneSuite) TestResetOnEmptyState() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), "No migrations to revert\n",
		normalizeOutput(out))
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestStatusAllPending() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "cli output: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Pending    \n"+
		"2          add email column          Pending    \n"+
		"3          add age column            Pending    \n",
		normalizeOutput(out))
}

func (s *CLIStandaloneSuite) TestStatusAllApplied() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "status")...)
	require.NoError(s.T(), err, "status: %s", out)
	require.Equal(s.T(), statusHeader+
		"1          create test table         Applied    APPLIED_AT         \n"+
		"2          add email column          Applied    APPLIED_AT         \n"+
		"3          add age column            Applied    APPLIED_AT         \n",
		normalizeOutput(out))
}

func (s *CLIStandaloneSuite) TestStatusPartiallyApplied() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up-to", "2")...)
	require.NoError(s.T(), err, "up-to: %s", out)

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "status")...)
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

func (s *CLIStandaloneSuite) TestUpThenDownThenUpAgain() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "first up: %s", out)
	require.Equal(s.T(), "Applying migration 1: create test table\n"+
		"OK\n"+
		"Applying migration 2: add email column\n"+
		"OK\n"+
		"Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(), "Reverting migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	out, err = runCLI(s.binaryPath, standaloneCliArgs(s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "second up: %s", out)
	require.Equal(s.T(), "Applying migration 3: add age column\n"+
		"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, standaloneTableName)
	assertAppliedMigrations(s.T(), actual, expectedMigrations)
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

func (s *CLIStandaloneSuite) TestInvalidMigrationsDir() {
	out, err := runCLI(s.binaryPath, standaloneCliArgs("/nonexistent/path", "up")...)
	require.Error(s.T(), err)
	require.Equal(s.T(),
		"failed to load migrations: failed to read migrations directory \"/nonexistent/path\": open /nonexistent/path: no such file or directory\n",
		out)
}
