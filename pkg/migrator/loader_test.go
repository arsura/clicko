package migrator_test

import (
	"testing"

	"github.com/arsura/clickhouse-migrator/pkg/migrator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const testdataDir = "testdata/loader"

type LoaderSuite struct {
	suite.Suite
}

func TestLoaderSuite(t *testing.T) {
	suite.Run(t, new(LoaderSuite))
}

func (s *LoaderSuite) TestUpAndDownPair() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_up_and_down")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*migrator.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: migrator.MigrationSource{
				UpSQL:   "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS users;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *LoaderSuite) TestMultipleVersionsSortedAscending() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_multiple_versions")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*migrator.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: migrator.MigrationSource{
				UpSQL:   "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS users;\n",
			},
		},
		{
			Version:     2,
			Description: "create orders",
			Source: migrator.MigrationSource{
				UpSQL:   "CREATE TABLE orders (id UInt64, user_id UInt64) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS orders;\n",
			},
		},
		{
			Version:     3,
			Description: "add email",
			Source: migrator.MigrationSource{
				UpSQL:   "ALTER TABLE users ADD COLUMN email String;\n",
				DownSQL: "ALTER TABLE users DROP COLUMN email;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *LoaderSuite) TestUpOnly() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_up_only")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*migrator.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: migrator.MigrationSource{
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *LoaderSuite) TestEmptyDirectory() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_empty_dir")

	got, err := loader.Load()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []*migrator.Migration{}, got)
}

func (s *LoaderSuite) TestNonSQLFilesIgnored() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_non_sql_files")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*migrator.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: migrator.MigrationSource{
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *LoaderSuite) TestSubdirectoryIgnored() {
	loader := migrator.NewFileLoader(testdataDir + "/happy_subdirectory")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*migrator.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: migrator.MigrationSource{
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *LoaderSuite) TestErrorDirectoryNotExist() {
	loader := migrator.NewFileLoader(testdataDir + "/this_does_not_exist")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "failed to read migrations directory")
}

func (s *LoaderSuite) TestErrorFilenameTooFewDots() {
	loader := migrator.NewFileLoader(testdataDir + "/err_filename_too_few_dots")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "invalid migration filename")
}

func (s *LoaderSuite) TestErrorFilenameTooManyDots() {
	loader := migrator.NewFileLoader(testdataDir + "/err_filename_too_many_dots")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "invalid migration filename")
}

func (s *LoaderSuite) TestErrorInvalidDirection() {
	loader := migrator.NewFileLoader(testdataDir + "/err_invalid_direction")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "direction must be")
}

func (s *LoaderSuite) TestErrorVersionNotNumeric() {
	loader := migrator.NewFileLoader(testdataDir + "/err_version_not_numeric")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "is not a valid number")
}

func (s *LoaderSuite) TestErrorDownOnly() {
	loader := migrator.NewFileLoader(testdataDir + "/err_down_only")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "has no .up.sql file")
}

func (s *LoaderSuite) TestErrorMismatchedDescription() {
	loader := migrator.NewFileLoader(testdataDir + "/err_mismatched_description")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "conflicting files for migration version")
}
