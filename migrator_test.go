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

// ---------------------------------------------------------------------------
// checkOutOfOrder — tested indirectly through Migrator.Up
// ---------------------------------------------------------------------------

type CheckOutOfOrderSuite struct {
	suite.Suite
}

func TestCheckOutOfOrderSuite(t *testing.T) {
	suite.Run(t, new(CheckOutOfOrderSuite))
}

func (s *CheckOutOfOrderSuite) TestNoAppliedReturnsNil() {
	loader := &mock.MockLoader{Migrations: []*clicko.Migration{
		mock.NoopMigration(1, "init"),
		mock.NoopMigration(2, "second"),
	}}
	store := &mock.MockStore{} // applied is nil → returns empty map

	m := clicko.NewMigrator(nil, loader, store)
	err := m.Up(context.Background())
	require.NoError(s.T(), err, "no applied migrations: maxApplied=0, should never be out-of-order")
}

func (s *CheckOutOfOrderSuite) TestInOrderReturnsNil() {
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

func (s *CheckOutOfOrderSuite) TestSingleVersionReturnsError() {
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

func (s *CheckOutOfOrderSuite) TestMultipleVersionsReturnsError() {
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

func (s *CheckOutOfOrderSuite) TestSingleVersionAllowFlagReturnsNil() {
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

func (s *CheckOutOfOrderSuite) TestMultipleVersionsAllowFlagReturnsNil() {
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
