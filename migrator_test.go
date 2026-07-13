package clicko_test

import (
	"context"
	"testing"

	"github.com/arsura/clicko"
	"github.com/arsura/clicko/internal/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MigratorSuite struct {
	suite.Suite
}

func TestMigratorTestSuite(t *testing.T) {
	suite.Run(t, new(MigratorSuite))
}

func (s *MigratorSuite) TestNoAppliedReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "init"),
		mock.NoopMigration(2, "second"),
	}}
	store := &mock.MockStore{} // applied is nil → returns empty map

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.NoError(s.T(), err, "no applied migrations: maxApplied=0, should never be out-of-order")
}

func (s *MigratorSuite) TestInOrderReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "second"),
		mock.NoopMigration(3, "third"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 2)}

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.NoError(s.T(), err, "pending v3 > maxApplied v2: in-order, should succeed")
}

func (s *MigratorSuite) TestSingleVersionReturnsError() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap"),
		mock.NoopMigration(3, "third"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 3)} // v2 pending, maxApplied=3

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "version(s) [2] are pending but version 3 is already applied; verify that the migration is independent of any previously applied changes before proceeding")
}

func (s *MigratorSuite) TestMultipleVersionsReturnsError() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap-a"),
		mock.NoopMigration(3, "gap-b"),
		mock.NoopMigration(4, "gap-c"),
		mock.NoopMigration(5, "fifth"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 5)} // v2,3,4 pending, maxApplied=5

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "version(s) [2 3 4] are pending but version 5 is already applied; verify that the migration is independent of any previously applied changes before proceeding")
}

func (s *MigratorSuite) TestSingleVersionAllowFlagReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap"),
		mock.NoopMigration(3, "third"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 3)}

	m := clicko.NewMigrator(nil, loader, store)
	m.SetAllowOutOfOrder(true)
	err := m.Up(context.Background())
	require.NoError(s.T(), err)
}

func (s *MigratorSuite) TestMultipleVersionsAllowFlagReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap-a"),
		mock.NoopMigration(3, "gap-b"),
		mock.NoopMigration(4, "gap-c"),
		mock.NoopMigration(5, "fifth"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 5)}

	m := clicko.NewMigrator(nil, loader, store)
	m.SetAllowOutOfOrder(true)
	err := m.Up(context.Background())
	require.NoError(s.T(), err)
}

func (s *MigratorSuite) TestUpErrorsOnNilUpFunc() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		{
			Version:     1,
			Description: "broken",
			Source:      clicko.MigrationSource{Type: clicko.MigrationSourceTypeGo},
		},
	}}
	store := &mock.MockStore{}

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "migration 1 has no up function")
}

func (s *MigratorSuite) TestDownDoesNotMutateLoaderSlice() {
	first := mock.NoopMigration(1, "first")
	second := mock.NoopMigration(2, "second")
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{first, second}}
	store := &mock.MockStore{} // nothing applied → down reverts nothing

	m := clicko.NewMigrator(nil, loader, store)
	require.NoError(s.T(), m.Down(context.Background()))

	assert.Equal(s.T(), []*clicko.Migration{first, second}, loader.Migrations,
		"down must not reorder the slice owned by the loader")
}

func (s *MigratorSuite) TestUpToBelowOutOfOrderVersionReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap"),
		mock.NoopMigration(3, "third"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 3)} // v2 pending, out of order, maxApplied=3

	m := clicko.NewMigrator(nil, loader, store)
	err := m.UpTo(context.Background(), 1)
	require.NoError(s.T(), err, "target 1 is already satisfied and below the out-of-order v2, which this run would never reach")
}

func (s *MigratorSuite) TestUpToAtOutOfOrderVersionReturnsError() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "first"),
		mock.NoopMigration(2, "gap"),
		mock.NoopMigration(3, "third"),
	}}
	store := &mock.MockStore{Applied: mock.AppliedVersions(1, 3)} // v2 pending, out of order, maxApplied=3

	m := clicko.NewMigrator(nil, loader, store)
	err := m.UpTo(context.Background(), 2)
	require.Error(s.T(), err, "target 2 would apply the out-of-order v2 this run")
	assert.Contains(s.T(), err.Error(), "version(s) [2] are pending but version 3 is already applied")
}
