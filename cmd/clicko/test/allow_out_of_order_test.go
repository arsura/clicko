package clicko_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIAllowOutOfOrderSuite tests the --allow-out-of-order flag behaviour.
//
// The scenario mirrors the motivating case: versions 1 and 3 are already
// applied (using the "initial" testdata directory), then a developer adds
// version 2 after the fact (the "with_gap" testdata directory).
//
// Prerequisites: ClickHouse must be running on localhost:29000.
// Start it with: make cluster-up
type CLIAllowOutOfOrderSuite struct {
	suite.Suite
	binaryPath            string
	conn                  clickhouse.Conn
	clickHouseCleanupFunc func() error

	initialDir string
	withGapDir string
	testDBName string
	testDBURI  string
}

func TestCLIAllowOutOfOrderSuite(t *testing.T) {
	suite.Run(t, new(CLIAllowOutOfOrderSuite))
}

func (s *CLIAllowOutOfOrderSuite) SetupSuite() {
	s.binaryPath = buildClicko(s.T())
	s.initialDir = filepath.Join(testDir(), "testdata", "out_of_order", "initial")
	s.withGapDir = filepath.Join(testDir(), "testdata", "out_of_order", "with_gap")
	s.conn, s.clickHouseCleanupFunc = dialClickHouse(s.T())
}

func (s *CLIAllowOutOfOrderSuite) TearDownSuite() {
	s.clickHouseCleanupFunc()
}

func (s *CLIAllowOutOfOrderSuite) SetupTest() {
	s.testDBName = createTestDB(s.T(), s.conn, "")
	s.testDBURI = testURIWithDB(s.testDBName)
}

// ootInitialMigrations are the two migrations applied from the "initial" directory (v1, v3).
var ootInitialMigrations = []appliedMigration{
	{Version: 1, Description: "create main table"},
	{Version: 3, Description: "add email column"},
}

// ootAllMigrations includes the out-of-order v2 that was added after v3 was already applied.
var ootAllMigrations = []appliedMigration{
	{Version: 1, Description: "create main table"},
	{Version: 2, Description: "create audit table"},
	{Version: 3, Description: "add email column"},
}

// ---------------------------------------------------------------------------
// Up — without flag
// ---------------------------------------------------------------------------

func (s *CLIAllowOutOfOrderSuite) TestUpErrorsOnOutOfOrder() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up")...)
	require.Error(s.T(), err)
	require.Contains(s.T(), out, "out-of-order migration detected: version(s) [2] are pending but version 3 is already applied")

	// v2 must NOT have been applied.
	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testOutOfOrderMigrationTable)
	assertAppliedMigrations(s.T(), actual, ootInitialMigrations)
}

// applyInitial is a helper that applies migrations 1 and 3 via the initial directory.
func (s *CLIAllowOutOfOrderSuite) applyInitial() {
	s.T().Helper()
	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.initialDir, "up")...)
	require.NoError(s.T(), err, "initial up: %s", out)
}

// ---------------------------------------------------------------------------
// Up — with flag
// ---------------------------------------------------------------------------

func (s *CLIAllowOutOfOrderSuite) TestUpAppliesOutOfOrderWithFlag() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up", "--allow-out-of-order")...)
	require.NoError(s.T(), err, "cli output: %s", out)

	normalized := normalizeOutput(out)
	require.Contains(s.T(), normalized, "Warning: applying out-of-order migration 2 (version 3 is already applied)")
	require.Contains(s.T(), normalized, "Applying migration 2: create audit table")
	require.Contains(s.T(), normalized, "OK")

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testOutOfOrderMigrationTable)
	assertAppliedMigrations(s.T(), actual, ootAllMigrations)
}

// ---------------------------------------------------------------------------
// Up-to — without flag
// ---------------------------------------------------------------------------

func (s *CLIAllowOutOfOrderSuite) TestUpToErrorsOnOutOfOrder() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up-to", "3")...)
	require.Error(s.T(), err)
	require.Contains(s.T(), out, "out-of-order migration detected: version(s) [2] are pending but version 3 is already applied")

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testOutOfOrderMigrationTable)
	assertAppliedMigrations(s.T(), actual, ootInitialMigrations)
}

// ---------------------------------------------------------------------------
// Up-to — with flag
// ---------------------------------------------------------------------------

func (s *CLIAllowOutOfOrderSuite) TestUpToAppliesOutOfOrderWithFlag() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up-to", "2", "--allow-out-of-order")...)
	require.NoError(s.T(), err, "cli output: %s", out)

	normalized := normalizeOutput(out)
	require.Contains(s.T(), normalized, "Warning: applying out-of-order migration 2 (version 3 is already applied)")
	require.Contains(s.T(), normalized, "Applying migration 2: create audit table")

	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testOutOfOrderMigrationTable)
	assertAppliedMigrations(s.T(), actual, ootAllMigrations)
}

// ---------------------------------------------------------------------------
// Dry-run — mirrors real run (Option A)
// ---------------------------------------------------------------------------

func (s *CLIAllowOutOfOrderSuite) TestDryRunErrorsOnOutOfOrder() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up", "--dry-run")...)
	require.Error(s.T(), err)
	require.Contains(s.T(), out, "out-of-order migration detected: version(s) [2] are pending but version 3 is already applied")
}

func (s *CLIAllowOutOfOrderSuite) TestDryRunShowsSQLWithFlag() {
	s.applyInitial()

	out, err := runCLI(s.binaryPath, outOfOrderArgs(s.testDBURI, s.withGapDir, "up", "--dry-run", "--allow-out-of-order")...)
	require.NoError(s.T(), err, "cli output: %s", out)

	require.True(s.T(),
		strings.Contains(out, "Warning: applying out-of-order migration 2 (version 3 is already applied)"),
		"expected out-of-order warning in output:\n%s", out)
	require.Contains(s.T(), out, ootDryRunMigration2)

	// dry-run must not apply v3.
	actual := queryAppliedMigrationsFrom(s.T(), s.conn, s.testDBName+"."+testOutOfOrderMigrationTable)
	assertAppliedMigrations(s.T(), actual, ootInitialMigrations)
}

const ootDryRunMigration2 = "=== Version 2: create audit table (sql) ===\n" +
	"CREATE TABLE IF NOT EXISTS oot_audit (\n" +
	"    id UInt64,\n" +
	"    action String\n" +
	") ENGINE = MergeTree()\n" +
	"ORDER BY id;\n\n"
