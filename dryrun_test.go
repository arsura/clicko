package clicko_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
	"github.com/arsura/clicko/internal/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DryRunGoMigrationSuite struct {
	suite.Suite
}

func TestDryRunGoMigrationSuite(t *testing.T) {
	suite.Run(t, new(DryRunGoMigrationSuite))
}

func (s *DryRunGoMigrationSuite) SetupTest() {
	clicko.ResetGlobalMigrations()
}

func (s *DryRunGoMigrationSuite) TearDownSuite() {
	clicko.ResetGlobalMigrations()
}

func (s *DryRunGoMigrationSuite) TestUpCapturesExecSQL() {
	clicko.RegisterNamedMigration("00001_create_users.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id")
		},
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "DROP TABLE IF EXISTS users")
		},
	)

	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), &mock.MockStore{})
	m.SetDryRun(true)

	out := captureStdout(s.T(), func() {
		err := m.Up(context.Background())
		require.NoError(s.T(), err)
	})

	expected := "=== Version 1: create users (go) ===\n" +
		"CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id\n\n"
	assert.Equal(s.T(), expected, out)
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	fn()

	w.Close()
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	os.Stdout = old
	return string(out)
}

func (s *DryRunGoMigrationSuite) TestDownCapturesExecSQL() {
	clicko.RegisterNamedMigration("00001_create_users.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id")
		},
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "DROP TABLE IF EXISTS users")
		},
	)

	store := &mock.MockStore{
		Applied: map[uint64]*clicko.Migration{
			1: {Version: 1, Description: "create users"},
		},
	}
	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), store)
	m.SetDryRun(true)

	out := captureStdout(s.T(), func() {
		err := m.Down(context.Background())
		require.NoError(s.T(), err)
	})

	expected := "=== Version 1: create users (go) ===\n" +
		"DROP TABLE IF EXISTS users\n\n"
	assert.Equal(s.T(), expected, out)
}

func (s *DryRunGoMigrationSuite) TestUpCapturesMultipleStatements() {
	clicko.RegisterNamedMigration("00001_create_tables.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id"); err != nil {
				return err
			}
			return conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS orders (id UInt64) ENGINE = MergeTree() ORDER BY id")
		},
		nil,
	)

	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), &mock.MockStore{})
	m.SetDryRun(true)

	out := captureStdout(s.T(), func() {
		err := m.Up(context.Background())
		require.NoError(s.T(), err)
	})

	expected := "=== Version 1: create tables (go) ===\n" +
		"CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id\n\n" +
		"CREATE TABLE IF NOT EXISTS orders (id UInt64) ENGINE = MergeTree() ORDER BY id\n\n"
	assert.Equal(s.T(), expected, out)
}

func (s *DryRunGoMigrationSuite) TestUpCapturesSQLWithArgs() {
	clicko.RegisterNamedMigration("00001_insert_seed.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", 1, "alice")
		},
		nil,
	)

	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), &mock.MockStore{})
	m.SetDryRun(true)

	out := captureStdout(s.T(), func() {
		err := m.Up(context.Background())
		require.NoError(s.T(), err)
	})

	expected := "=== Version 1: insert seed (go) ===\n" +
		"INSERT INTO users (id, name) VALUES (?, ?)\n" +
		fmt.Sprintf("-- args: %v", []any{1, "alice"}) + "\n\n"
	assert.Equal(s.T(), expected, out)
}

func (s *DryRunGoMigrationSuite) TestUpOmitsDictionaryPassword() {
	clicko.RegisterNamedMigration("00001_create_dict.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "CREATE DICTIONARY dict (id UInt64) PRIMARY KEY id SOURCE(MYSQL(HOST 'localhost' USER 'root' PASSWORD 'super-secret' DB 'test' TABLE 'users')) LAYOUT(HASHED()) LIFETIME(0)")
		},
		nil,
	)

	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), &mock.MockStore{})
	m.SetDryRun(true)

	out := captureStdout(s.T(), func() {
		err := m.Up(context.Background())
		require.NoError(s.T(), err)
	})

	expected := "=== Version 1: create dict (go) ===\n" +
		"-- statement omitted (matched credential pattern)\n\n"
	assert.Equal(s.T(), expected, out)
	assert.NotContains(s.T(), out, "super-secret")
}

func (s *DryRunGoMigrationSuite) TestUpDoesNotModifyState() {
	clicko.RegisterNamedMigration("00001_create_users.go",
		func(ctx context.Context, conn clickhouse.Conn) error {
			return conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id UInt64) ENGINE = MergeTree() ORDER BY id")
		},
		nil,
	)

	store := &mock.MockStore{}
	m := clicko.NewMigrator(nil, clicko.NewGoLoader(), store)
	m.SetDryRun(true)

	_ = captureStdout(s.T(), func() {
		err := m.Up(context.Background())
		require.NoError(s.T(), err)
	})

	applied, err := store.GetAppliedVersions(context.Background())
	require.NoError(s.T(), err)
	assert.Empty(s.T(), applied, "dry-run must not record any applied migration")
}

func TestFormatDryRunSQL(t *testing.T) {
	t.Parallel()

	const omitted = "-- statement omitted (matched credential pattern)"

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "dictionary mysql password omitted",
			in:   "CREATE DICTIONARY dict (id UInt64) PRIMARY KEY id SOURCE(MYSQL(HOST 'localhost' USER 'root' PASSWORD 'super-secret' DB 'test' TABLE 'users')) LAYOUT(HASHED()) LIFETIME(0)",
			want: omitted,
		},
		{
			name: "identified by omitted",
			in:   "CREATE USER bob IDENTIFIED BY 'topsecret'",
			want: omitted,
		},
		{
			name: "no credentials unchanged",
			in:   "CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id",
			want: "CREATE TABLE users (id UInt64) ENGINE = MergeTree() ORDER BY id",
		},
		{
			name: "user_password column unchanged",
			in:   "ALTER TABLE users ADD COLUMN user_password String",
			want: "ALTER TABLE users ADD COLUMN user_password String",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, clicko.FormatDryRunSQL(tt.in))
		})
	}
}

func TestFormatDryRunStatement(t *testing.T) {
	t.Parallel()

	const omitted = "-- statement omitted (matched credential pattern)"

	t.Run("inline password omitted", func(t *testing.T) {
		t.Parallel()
		got := clicko.FormatDryRunStatement(
			"CREATE DICTIONARY dict (...) SOURCE(MYSQL(PASSWORD 'secret'))",
			nil,
		)
		assert.Equal(t, omitted, got)
	})

	t.Run("parameterized password omitted", func(t *testing.T) {
		t.Parallel()
		got := clicko.FormatDryRunStatement(
			"CREATE DICTIONARY dict (...) SOURCE(MYSQL(PASSWORD ?))",
			[]any{"secret"},
		)
		assert.Equal(t, omitted, got)
	})

	t.Run("non-credential args unchanged", func(t *testing.T) {
		t.Parallel()
		got := clicko.FormatDryRunStatement(
			"INSERT INTO users (id, name) VALUES (?, ?)",
			[]any{1, "alice"},
		)
		assert.Equal(t, "INSERT INTO users (id, name) VALUES (?, ?)\n-- args: [1 alice]", got)
	})
}
