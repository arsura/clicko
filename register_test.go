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

func (s *RegisterSuite) TestAddNamedMigrationRegistersUpAndDown() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }
	down := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.AddNamedMigration("20250317141923_create_users.go", up, down)

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(20250317141923), got[0].Version)
	assert.Equal(s.T(), "create users", got[0].Description)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.NotNil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestAddNamedMigrationUpOnly() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.AddNamedMigration("00001_create_table.go", up, nil)

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(1), got[0].Version)
	assert.NotNil(s.T(), got[0].Source.UpFunc)
	assert.Nil(s.T(), got[0].Source.DownFunc)
}

func (s *RegisterSuite) TestAddNamedMigrationFullPath() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.AddNamedMigration("/home/user/project/migrations/20250317141923_create_users.go", up, nil)

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 1)
	assert.Equal(s.T(), uint64(20250317141923), got[0].Version)
	assert.Equal(s.T(), "create users", got[0].Description)
}

func (s *RegisterSuite) TestAddNamedMigrationPanicsOnDuplicateVersion() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.AddNamedMigration("00001_create_users.go", up, nil)

	assert.PanicsWithValue(s.T(),
		`failed to add migration "00001_create_orders.go": version 1 conflicts with "create users"`,
		func() {
			clicko.AddNamedMigration("00001_create_orders.go", up, nil)
		},
	)
}

func (s *RegisterSuite) TestAddNamedMigrationPanicsOnInvalidVersion() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	assert.Panics(s.T(), func() {
		clicko.AddNamedMigration("abc_invalid.go", up, nil)
	})
}

func (s *RegisterSuite) TestResetGlobalMigrations() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }
	clicko.AddNamedMigration("00001_create_users.go", up, nil)

	clicko.ResetGlobalMigrations()

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Empty(s.T(), got)
}

func (s *RegisterSuite) TestMultipleRegistrations() {
	up := func(ctx context.Context, conn clickhouse.Conn) error { return nil }

	clicko.AddNamedMigration("00003_add_column.go", up, nil)
	clicko.AddNamedMigration("00001_create_table.go", up, nil)
	clicko.AddNamedMigration("00002_create_index.go", up, nil)

	loader := clicko.NewGoLoader()
	got, err := loader.Load()

	assert.NoError(s.T(), err)
	assert.Len(s.T(), got, 3)
	assert.Equal(s.T(), uint64(1), got[0].Version)
	assert.Equal(s.T(), uint64(2), got[1].Version)
	assert.Equal(s.T(), uint64(3), got[2].Version)
}
