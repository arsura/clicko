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
	require.Equal(s.T(), usageText, out)
}

func (s *CLIValidationSuite) TestNoCommand() {
	out, err := runCLI(s.binaryPath)
	require.Error(s.T(), err)
	require.Equal(s.T(), usageText, out)
}

func (s *CLIValidationSuite) TestUnknownCommand() {
	out, err := runCLI(s.binaryPath, "foobar")
	require.Error(s.T(), err)
	require.Equal(s.T(), "unknown command: foobar\n"+usageText, out)
}

func (s *CLIValidationSuite) TestMissingURI() {
	out, err := runCLI(s.binaryPath, "up")
	require.Error(s.T(), err)
	require.Equal(s.T(), "uri is required\n"+usageText, out)
}

func (s *CLIValidationSuite) TestUpToMissingVersion() {
	out, err := runCLI(s.binaryPath, "up-to")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version is required\n"+usageText, out)
}

func (s *CLIValidationSuite) TestDownToMissingVersion() {
	out, err := runCLI(s.binaryPath, "down-to")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version is required\n"+usageText, out)
}

func (s *CLIValidationSuite) TestUpToInvalidVersion() {
	out, err := runCLI(s.binaryPath, "up-to", "abc")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version must be a positive number\n"+usageText, out)
}

func (s *CLIValidationSuite) TestDownToInvalidVersion() {
	out, err := runCLI(s.binaryPath, "down-to", "abc")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version must be a positive number\n"+usageText, out)
}

func (s *CLIValidationSuite) TestUpToNegativeVersion() {
	out, err := runCLI(s.binaryPath, "up-to", "-1")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version must be a positive number\n"+usageText, out)
}

func (s *CLIValidationSuite) TestDownToNegativeVersion() {
	out, err := runCLI(s.binaryPath, "down-to", "-1")
	require.Error(s.T(), err)
	require.Equal(s.T(), "version must be a positive number\n"+usageText, out)
}
