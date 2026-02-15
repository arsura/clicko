package cli_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testDSN      = "clickhouse://default:@localhost:29000/default"
	clusterName  = "all-replicated"
	customEngine = "ReplicatedMergeTree('/clickhouse/all-replicated/tables/all/{database}/{table}', '{replica}')"
	insertQuorum = "4"
	tableName    = "migration_versions"
)

// CLISuite is an integration test suite that builds the real CLI binary
// and runs it against a live ClickHouse cluster.
//
// Prerequisites: ClickHouse cluster must be running.
// Start it with: docker compose -f examples/cluster/docker-compose.yaml up -d
type CLISuite struct {
	suite.Suite
	binaryPath    string
	conn          clickhouse.Conn
	migrationsDir string
}

func TestCLISuite(t *testing.T) {
	// Skip automatically if ClickHouse is not reachable.
	opts, err := clickhouse.ParseDSN(testDSN)
	if err != nil {
		t.Skipf("skipping: invalid DSN: %v", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Skipf("skipping: cannot open connection: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		conn.Close()
		t.Skipf("skipping: ClickHouse not reachable at %s: %v", testDSN, err)
	}
	conn.Close()

	suite.Run(t, new(CLISuite))
}

func (s *CLISuite) SetupSuite() {
	// Build the CLI binary once for all tests.
	binPath := filepath.Join(s.T().TempDir(), "clickhouse-migrator")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	cmd := exec.Command("go", "build", "-o", binPath, "../../.")
	cmd.Dir = testDir()
	out, err := cmd.CombinedOutput()
	require.NoError(s.T(), err, "failed to build binary: %s", string(out))
	s.binaryPath = binPath
	s.migrationsDir = filepath.Join(testDir(), "testdata", "migrations")

	// Open a direct connection for verification queries.
	opts, err := clickhouse.ParseDSN(testDSN)
	require.NoError(s.T(), err)

	conn, err := clickhouse.Open(opts)
	require.NoError(s.T(), err)
	require.NoError(s.T(), conn.Ping(context.Background()))
	s.conn = conn
}

func (s *CLISuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *CLISuite) SetupTest() {
	s.cleanup()
}

// cleanup drops test tables ON CLUSTER with SYNC to ensure ZooKeeper
// paths are fully removed before the next test creates them again.
func (s *CLISuite) cleanup() {
	ctx := context.Background()
	_ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS test_cli_migration ON CLUSTER `"+clusterName+"` SYNC")
	_ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS `"+tableName+"` ON CLUSTER `"+clusterName+"` SYNC")
}

func (s *CLISuite) TearDownTest() {
	s.cleanup()
}

// runCLI executes the CLI binary with the given arguments and returns combined output.
func (s *CLISuite) runCLI(args ...string) (string, error) {
	cmd := exec.Command(s.binaryPath, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// cliArgs returns the common flags for cluster mode.
func (s *CLISuite) cliArgs(command ...string) []string {
	args := []string{
		"--dsn", testDSN,
		"--dir", s.migrationsDir,
		"--cluster", clusterName,
		"--insert-quorum", insertQuorum,
		"--engine", customEngine,
	}
	return append(args, command...)
}

func (s *CLISuite) TestUpAppliesMigration() {
	// Run "up" in cluster mode.
	out, err := s.runCLI(s.cliArgs("up")...)
	require.NoError(s.T(), err, "cli output: %s", out)

	// Verify the table was created by inserting and querying a row.
	ctx := context.Background()
	err = s.conn.Exec(ctx, "INSERT INTO test_cli_migration (id, name) VALUES (1, 'alice')")
	require.NoError(s.T(), err)

	var name string
	err = s.conn.QueryRow(ctx, "SELECT name FROM test_cli_migration WHERE id = 1").Scan(&name)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "alice", name)

	// Verify the version is recorded in the tracking table.
	var version uint64
	err = s.conn.QueryRow(ctx, "SELECT version FROM "+tableName+" WHERE version = 1").Scan(&version)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(1), version)
}

// testDir returns the absolute path of the directory containing this test file.
func testDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filename)
}
