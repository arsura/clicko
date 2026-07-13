package clicko_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const testdataDir = "testdata"

type SQLLoaderSuite struct {
	suite.Suite
}

func TestSQLLoaderSuite(t *testing.T) {
	suite.Run(t, new(SQLLoaderSuite))
}

func (s *SQLLoaderSuite) TestUpAndDownPair() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_up_and_down")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:    clicko.MigrationSourceTypeSQL,
				UpSQL:   "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS users;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestMultipleVersionsSortedAscending() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_multiple_versions")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:    clicko.MigrationSourceTypeSQL,
				UpSQL:   "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS users;\n",
			},
		},
		{
			Version:     2,
			Description: "create orders",
			Source: clicko.MigrationSource{
				Type:    clicko.MigrationSourceTypeSQL,
				UpSQL:   "CREATE TABLE orders (id UInt64, user_id UInt64) ENGINE = MergeTree() ORDER BY id;\n",
				DownSQL: "DROP TABLE IF EXISTS orders;\n",
			},
		},
		{
			Version:     3,
			Description: "add email",
			Source: clicko.MigrationSource{
				Type:    clicko.MigrationSourceTypeSQL,
				UpSQL:   "ALTER TABLE users ADD COLUMN email String;\n",
				DownSQL: "ALTER TABLE users DROP COLUMN email;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestUpOnly() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_up_only")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:  clicko.MigrationSourceTypeSQL,
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestEmptyDirectory() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_empty_dir")

	got, err := loader.Load()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []*clicko.Migration{}, got)
}

func (s *SQLLoaderSuite) TestNonSQLFilesIgnored() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_non_sql_files")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:  clicko.MigrationSourceTypeSQL,
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestUppercaseExtensionLoaded() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_uppercase_extension")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:  clicko.MigrationSourceTypeSQL,
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestSymlinkedDirectoryIgnored() {
	dir := s.T().TempDir()
	linkTarget := s.T().TempDir()
	require.NoError(s.T(), os.WriteFile(
		filepath.Join(dir, "00001_create_users.up.sql"),
		[]byte("CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id;\n"),
		0o644,
	))
	require.NoError(s.T(), os.Symlink(linkTarget, filepath.Join(dir, "nested.sql")))

	loader := clicko.NewSQLLoader(dir)

	got, err := loader.Load()
	require.NoError(s.T(), err)
	require.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(1), got[0].Version)
}

func (s *SQLLoaderSuite) TestSubdirectoryIgnored() {
	loader := clicko.NewSQLLoader(testdataDir + "/happy_subdirectory")

	got, err := loader.Load()
	require.NoError(s.T(), err)

	expected := []*clicko.Migration{
		{
			Version:     1,
			Description: "create users",
			Source: clicko.MigrationSource{
				Type:  clicko.MigrationSourceTypeSQL,
				UpSQL: "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;\n",
			},
		},
	}
	assert.Equal(s.T(), expected, got)
}

func (s *SQLLoaderSuite) TestErrorDirectoryNotExist() {
	loader := clicko.NewSQLLoader(testdataDir + "/this_does_not_exist")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "failed to read migrations directory")
}

func (s *SQLLoaderSuite) TestErrorFilenameTooFewDots() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_filename_too_few_dots")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "invalid migration filename")
}

func (s *SQLLoaderSuite) TestErrorFilenameTooManyDots() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_filename_too_many_dots")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "invalid migration filename")
}

func (s *SQLLoaderSuite) TestErrorInvalidDirection() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_invalid_direction")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "direction must be")
}

func (s *SQLLoaderSuite) TestErrorVersionNotNumeric() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_version_not_numeric")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "is not a valid number")
}

func (s *SQLLoaderSuite) TestErrorVersionZero() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_version_zero")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "version 0 is reserved")
}

func (s *SQLLoaderSuite) TestErrorEmptyUpFile() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_empty_up_file")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, `migration file "00001_create_users.up.sql" is empty`)
}

func (s *SQLLoaderSuite) TestErrorEmptyDownFile() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_empty_down_file")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, `migration file "00001_create_users.down.sql" is empty`)
}

func (s *SQLLoaderSuite) TestErrorCommentOnlyDownFile() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_comment_only_down_file")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, `migration file "00001_create_users.down.sql" is empty`)
}

func (s *SQLLoaderSuite) TestErrorDownOnly() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_down_only")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "has no .up.sql file")
}

func (s *SQLLoaderSuite) TestErrorDuplicateDirection() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_duplicate_direction")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "conflicting files for migration version 1")
	assert.ErrorContains(s.T(), err, `both define the "up" direction`)
}

func (s *SQLLoaderSuite) TestErrorMismatchedDescription() {
	loader := clicko.NewSQLLoader(testdataDir + "/err_mismatched_description")

	_, err := loader.Load()
	assert.ErrorContains(s.T(), err, "conflicting files for migration version")
}

type GoLoaderSuite struct {
	suite.Suite
}

func TestGoLoaderSuite(t *testing.T) {
	suite.Run(t, new(GoLoaderSuite))
}

func (s *GoLoaderSuite) SetupTest() {
	clicko.ResetGlobalMigrations()
}

func (s *GoLoaderSuite) TestEmptyRegistry() {
	loader := clicko.NewGoLoader()

	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Empty(s.T(), got)
}

func (s *GoLoaderSuite) TestPreservesGoFuncs() {
	upCalled, downCalled := false, false
	up := func(ctx context.Context, conn clickhouse.Conn) error {
		upCalled = true
		return nil
	}
	down := func(ctx context.Context, conn clickhouse.Conn) error {
		downCalled = true
		return nil
	}

	clicko.RegisterNamedMigration("00001_test.go", up, down)

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), clicko.MigrationSourceTypeGo, got[0].Source.Type)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.NotNil(s.T(), got[0].Source.DownFunc)

	_ = got[0].Source.UpFunc(context.Background(), nil)
	assert.True(s.T(), upCalled)

	_ = got[0].Source.DownFunc(context.Background(), nil)
	assert.True(s.T(), downCalled)
}

func (s *GoLoaderSuite) TestLoadReturnsIndependentCopies() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }
	clicko.RegisterNamedMigration("00001_original.go", up, nil)

	loader := clicko.NewGoLoader()
	first, err := loader.Load()
	require.NoError(s.T(), err)
	require.Len(s.T(), first, 1)

	// Mutating a loaded migration must not leak back into the registry.
	first[0].Description = "mutated"
	first[0].Source.Type = "tampered"

	second, err := loader.Load()
	require.NoError(s.T(), err)
	require.Len(s.T(), second, 1)
	assert.Equal(s.T(), "original", second[0].Description)
	assert.Equal(s.T(), clicko.MigrationSourceTypeGo, second[0].Source.Type)
}
