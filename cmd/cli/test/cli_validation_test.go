package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// CLIValidationSuite tests CLI argument parsing and validation.
// These tests do NOT require a ClickHouse connection — they verify
// behaviour that occurs before the CLI attempts to connect.
type CLIValidationSuite struct {
	suite.Suite
	binaryPath string
}

func TestCLIValidationSuite(t *testing.T) {
	suite.Run(t, new(CLIValidationSuite))
}

func (s *CLIValidationSuite) SetupSuite() {
	s.binaryPath = buildCLI(s.T())
}

func (s *CLIValidationSuite) TestHelp() {
	out, err := runCLI(s.binaryPath, "--help")
	require.NoError(s.T(), err)
	require.Equal(s.T(), globalUsage, out)
}

func (s *CLIValidationSuite) TestNoCommand() {
	out, err := runCLI(s.binaryPath)
	require.Error(s.T(), err)
	require.Equal(s.T(),
		globalUsage+"\nclicko: error: missing flags: --uri=STRING\n",
		out)
}

func (s *CLIValidationSuite) TestUnknownCommand() {
	out, err := runCLI(s.binaryPath, "--uri", "x", "foobar")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		globalUsage+"\nclicko: error: unexpected argument foobar\n",
		out)
}

func (s *CLIValidationSuite) TestMissingURI() {
	out, err := runCLI(s.binaryPath, "up")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		upCmdUsage+"\nclicko: error: missing flags: --uri=STRING\n",
		out)
}

func (s *CLIValidationSuite) TestUpToMissingVersion() {
	out, err := runCLI(s.binaryPath, "up-to", "--uri", "x")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		upToCmdUsage+"\nclicko: error: expected \"<version>\"\n",
		out)
}

func (s *CLIValidationSuite) TestDownToMissingVersion() {
	out, err := runCLI(s.binaryPath, "down-to", "--uri", "x")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		downToCmdUsage+"\nclicko: error: expected \"<version>\"\n",
		out)
}

func (s *CLIValidationSuite) TestUpToInvalidVersion() {
	out, err := runCLI(s.binaryPath, "up-to", "--uri", "x", "abc")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		upToCmdUsage+"\nclicko: error: <version>: expected a valid 64 bit uint but got \"abc\"\n",
		out)
}

func (s *CLIValidationSuite) TestDownToInvalidVersion() {
	out, err := runCLI(s.binaryPath, "down-to", "--uri", "x", "abc")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		downToCmdUsage+"\nclicko: error: <version>: expected a valid 64 bit uint but got \"abc\"\n",
		out)
}

func (s *CLIValidationSuite) TestUpToNegativeVersion() {
	out, err := runCLI(s.binaryPath, "up-to", "--uri", "x", "--", "-1")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		upToCmdUsage+"\nclicko: error: <version>: expected a valid 64 bit uint but got \"-1\"\n",
		out)
}

func (s *CLIValidationSuite) TestDownToNegativeVersion() {
	out, err := runCLI(s.binaryPath, "down-to", "--uri", "x", "--", "-1")
	require.Error(s.T(), err)
	require.Equal(s.T(),
		downToCmdUsage+"\nclicko: error: <version>: expected a valid 64 bit uint but got \"-1\"\n",
		out)
}
