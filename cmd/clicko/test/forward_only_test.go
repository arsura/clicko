package clicko_test

import (
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIForwardOnlySuite tests rollback behaviour around forward-only migrations
// (migrations without a .down.sql file). Rollback leaves a forward-only
// migration applied and moves on to revert older migrations underneath it,
// since those are independent of it.
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

// TestDownSkipsForwardOnlyMigration verifies that Down() skips over the
// newest applied migration when it has no down direction, leaving it applied,
// and finds nothing else to revert underneath (every version in this suite's
// migrationsDir is forward-only).
func (s *CLIForwardOnlySuite) TestDownSkipsForwardOnlyMigration() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "down")...)
	require.NoError(s.T(), err, "down: %s", out)
	require.Equal(s.T(),
		"Skipping migration 3: add age column (forward-only, no down defined; left applied)\n"+
			"Skipping migration 2: add email column (forward-only, no down defined; left applied)\n"+
			"Skipping migration 1: create test table (forward-only, no down defined; left applied)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}

// TestResetSkipsForwardOnlyMigrations verifies that Reset() skips every
// forward-only migration, leaving them all applied, when none in the set has
// a down direction.
func (s *CLIForwardOnlySuite) TestResetSkipsForwardOnlyMigrations() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.migrationsDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(),
		"Skipping migration 3: add age column (forward-only, no down defined; left applied)\n"+
			"Skipping migration 2: add email column (forward-only, no down defined; left applied)\n"+
			"Skipping migration 1: create test table (forward-only, no down defined; left applied)\n"+
			"No migrations to revert\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 1, Description: "create test table"},
		{Version: 2, Description: "add email column"},
		{Version: 3, Description: "add age column"},
	})
}

// TestResetSkipsForwardOnlyAndRevertsBelow verifies Reset() reverts 3 and 1
// but leaves forward-only version 2 applied — a known, accepted tracking
// table inconsistency (version 2's table no longer exists but its record
// stays "applied"); see IMPROVEMENT.md #5.
func (s *CLIForwardOnlySuite) TestResetSkipsForwardOnlyAndRevertsBelow() {
	out, err := runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.mixedDir, "up")...)
	require.NoError(s.T(), err, "up: %s", out)

	out, err = runCLI(s.binaryPath, forwardOnlyArgs(s.testDBURI, s.mixedDir, "reset")...)
	require.NoError(s.T(), err, "reset: %s", out)
	require.Equal(s.T(),
		"Reverting migration 3: add age column\n"+
			"OK\n"+
			"Skipping migration 2: add email column (forward-only, no down defined; left applied)\n"+
			"Reverting migration 1: create test table\n"+
			"OK\n",
		normalizeOutput(out))

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testForwardOnlyMigrationTable)
	assertAppliedMigrations(s.T(), actual, []appliedMigration{
		{Version: 2, Description: "add email column"},
	})
}
