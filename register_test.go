package clicko_test

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type RegisterSuite struct {
	suite.Suite
}

func TestRegisterSuite(t *testing.T) {
	suite.Run(t, new(RegisterSuite))
}

func (s *RegisterSuite) SetupTest() {
	clicko.ResetGlobalMigrations()
}

func (s *RegisterSuite) TestRegistersUpAndDown() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }
	down := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("20250317141923_create_users.go", up, down)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(20250317141923), got[0].Version)
	assert.Equal(s.T(), "create users", got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.NotNil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestRegistersUpOnly() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("00001_create_table.go", up, nil)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(1), got[0].Version)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestRegistersFullPath() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("/home/user/project/migrations/20250317141923_create_users.go", up, nil)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(20250317141923), got[0].Version)
	assert.Equal(s.T(), "create users", got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestPanicsOnDuplicateVersion() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("00001_create_users.go", up, nil)

	assert.PanicsWithValue(s.T(),
		`failed to add migration "00001_create_orders.go": version 1 conflicts with "create users"`,
		func() {
			clicko.RegisterNamedMigration("00001_create_orders.go", up, nil)
		},
	)
}

func (s *RegisterSuite) TestPanicsOnInvalidVersion() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	assert.PanicsWithValue(s.T(),
		`failed to parse version from filename "abc_invalid.go": strconv.ParseUint: parsing "abc": invalid syntax`,
		func() {
			clicko.RegisterNamedMigration("abc_invalid.go", up, nil)
		},
	)
}

func (s *RegisterSuite) TestPanicsOnNilUp() {
	assert.PanicsWithValue(s.T(),
		`failed to add migration "00001_create_users.go": up function must not be nil`,
		func() {
			clicko.RegisterNamedMigration("00001_create_users.go", nil, nil)
		},
	)
}

func (s *RegisterSuite) TestResetGlobalMigrations() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }
	clicko.RegisterNamedMigration("00001_create_users.go", up, nil)

	got, err := clicko.NewGoLoader().Load()
	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)

	clicko.ResetGlobalMigrations()

	got, err = clicko.NewGoLoader().Load()
	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 0)
}

func (s *RegisterSuite) TestMultiWordDescription() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("00001_create_users_table.go", up, nil)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), "create users table", got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestNoDescription() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("00001.go", up, nil)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(1), got[0].Version)
	assert.Empty(s.T(), got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestMultipleRegistrationsSortedAscending() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.RegisterNamedMigration("00003_add_column.go", up, nil)
	clicko.RegisterNamedMigration("00001_create_table.go", up, nil)
	clicko.RegisterNamedMigration("00002_create_index.go", up, nil)

	got, err := clicko.NewGoLoader().Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 3)
	assert.Equal(s.T(), uint64(1), got[0].Version)
	assert.Equal(s.T(), "create table", got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)

	assert.Equal(s.T(), uint64(2), got[1].Version)
	assert.Equal(s.T(), "create index", got[1].Description)
	assert.NotNil(s.T(), got[1].Source.UpFunc)
	assert.Nil(s.T(), got[1].Source.DownFunc)

	assert.Equal(s.T(), uint64(3), got[2].Version)
	assert.Equal(s.T(), "add column", got[2].Description)
	assert.NotNil(s.T(), got[2].Source.UpFunc)
	assert.Nil(s.T(), got[2].Source.DownFunc)
}
