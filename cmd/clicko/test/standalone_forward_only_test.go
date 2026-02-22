package clicko_test

import (
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIStandaloneForwardOnlySuite tests rollback behaviour when one migration
// has no .down.sql file (forward-only). Migration 2 is intentionally missing
// its down file so that Down() and Reset() must skip it.
type CLIStandaloneForwardOnlySuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error
	migrationsDir         string

	testDBName string
	testDBURI  string
}

func TestCLIStandaloneForwardOnlySuite(t *testing.T) {
	suite.Run(t, new(CLIStandaloneForwardOnlySuite))
}

func (s *CLIStandaloneForwardOnlySuite) SetupSuite() {
	s.binaryPath = buildClicko(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "standalone_with_forward_only")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIStandaloneForwardOnlySuite) TearDownSuite() {
	s.clickHouseCleanupFunc()
}

func (s *CLIStandaloneForwardOnlySuite) SetupTest() {
	s.testDBName = createTestDB(s.T(), s.conn, "")
	s.testDBURI = testURIWithDB(s.testDBName)
}

// TestDownSkipsForwardOnlyMigrations verifies that Down() skips all applied
// migrations when none of them define a down direction, and reports that
// there is nothing to revert.
func (s *CLIStandaloneForwardOnlySuite) TestDownSkipsForwardOnlyMigrations() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(),
		"Skipping migration 3: add age column (forward-only, no down defined)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}

// TestResetSkipsForwardOnlyMigrations verifies that Reset() skips all applied
// migrations when none of them define a down direction, and reports that
// there is nothing to revert.
func (s *CLIStandaloneForwardOnlySuite) TestResetSkipsForwardOnlyMigrations() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(),
		"Skipping migration 3: add age column (forward-only, no down defined)\n"+
			"Skipping migration 2: add email column (forward-only, no down defined)\n"+
			"Skipping migration 1: create test table (forward-only, no down defined)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}
