package clicko_test

import (
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIForwardOnlySuite tests rollback behaviour around forward-only migrations
// (migrations without a .down.sql file). Rollback must stop when it reaches
// one: skipping past it and reverting older migrations would leave newer state
// applied on top of reverted foundations.
type CLIForwardOnlySuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error
	// migrationsDir has no down files at all; mixedDir has down files for
	// versions 1 and 3 but not 2.
	migrationsDir string
	mixedDir      string

	testDBName string
	testDBURI  string
}

func TestCLIForwardOnlySuite(t *testing.T) {
	suite.Run(t, new(CLIForwardOnlySuite))
}

func (s *CLIForwardOnlySuite) SetupSuite() {
	s.binaryPath = buildClicko(s.T())
	s.migrationsDir = filepath.Join(testDir(), "testdata", "forward_only")
	s.mixedDir = filepath.Join(testDir(), "testdata", "forward_only_mixed")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIForwardOnlySuite) TearDownSuite() {
	s.clickHouseCleanupFunc()
}

func (s *CLIForwardOnlySuite) SetupTest() {
	s.testDBName = createTestDB(s.T(), s.conn, "")
	s.testDBURI = testURIWithDB(s.testDBName)
}

// TestDownStopsAtForwardOnlyMigration verifies that Down() stops at the newest
// applied migration when it has no down direction, reverting nothing.
func (s *CLIForwardOnlySuite) TestDownStopsAtForwardOnlyMigration() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(),
		"Stopping rollback at migration 3: add age column (forward-only, no down defined)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}

// TestResetStopsAtForwardOnlyMigration verifies that Reset() stops at the
// newest applied migration when it has no down direction — it must not skip
// past it and revert the older ones underneath.
func (s *CLIForwardOnlySuite) TestResetStopsAtForwardOnlyMigration() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(),
		"Stopping rollback at migration 3: add age column (forward-only, no down defined)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}

// TestResetStopsAtForwardOnlyWithoutRevertingBelow guards the pre-fix
// behaviour where Reset() skipped a forward-only migration and kept reverting
// older ones, leaving version 2 applied while the table from version 1 was
// dropped. Version 3 (has a down) is reverted, then rollback stops at the
// forward-only version 2, keeping versions 1 and 2 applied.
func (s *CLIForwardOnlySuite) TestResetStopsAtForwardOnlyWithoutRevertingBelow() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.mixedDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.mixedDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(),
		"Reverting migration 3: add age column\n"+
			"OK\n"+
			"Stopping rollback at migration 2: add email column (forward-only, no down defined)\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
	})
}
