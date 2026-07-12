package mock

import (
	"context"
	"maps"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

// MockStore is an in-memory Store implementation for use in tests.
type MockStore struct {
	Applied map[uint64]*clicko.Migration
	// TableMissing simulates the tracking table not existing yet;
	// TableExists returns its negation.
	TableMissing bool
	// TableMissingOnSomeReplicas simulates cluster mode where the connected
	// node has the table but another replica does not: TableExistsEverywhere
	// returns false while TableExists stays true.
	TableMissingOnSomeReplicas bool
}

func (s *MockStore) EnsureTable(_ context.Context) error { return nil }
func (s *MockStore) TableExists(_ context.Context) (bool, error) {
	return !s.TableMissing, nil
}
func (s *MockStore) TableExistsEverywhere(_ context.Context) (bool, error) {
	return !s.TableMissing && !s.TableMissingOnSomeReplicas, nil
}
func (s *MockStore) GetCreateTableDDL() string {
	return "CREATE TABLE IF NOT EXISTS mock_migration_versions (version UInt64) ENGINE = MergeTree() ORDER BY version"
}
func (s *MockStore) GetAppliedVersions(_ context.Context) (map[uint64]*clicko.Migration, error) {
	out := make(map[uint64]*clicko.Migration, len(s.Applied))
	maps.Copy(out, s.Applied)
	return out, nil
}

// Add and Remove are intentional no-ops. They do not mutate Applied, so tests
// that need to assert post-Up state should set Applied directly rather than
// relying on these methods to update it.
func (s *MockStore) Add(_ context.Context, _ uint64, _ string) error { return nil }
func (s *MockStore) Remove(_ context.Context, _ uint64) error        { return nil }

// MockLoader is a Loader that returns a fixed list of migrations.
type MockLoader struct {
	Migrations []*clicko.Migration
}

func (l *MockLoader) Load() ([]*clicko.Migration, error) {
	return l.Migrations, nil
}

// NoopMigration returns a Migration whose Up function is a no-op,
// so it can be applied against a nil connection in unit tests.
func NoopMigration(version uint64, description string) *clicko.Migration {
	return &clicko.Migration{
		Version:     version,
		Description: description,
		Source: clicko.MigrationSource{
			Type:   clicko.MigrationSourceTypeGo,
			UpFunc: func(_ context.Context, _ clickhouse.Conn) error { return nil },
		},
	}
}

// AppliedVersions builds a map[uint64]*Migration from a list of versions,
// mirroring what MockStore returns after migrations have been recorded.
func AppliedVersions(versions ...uint64) map[uint64]*clicko.Migration {
	m := make(map[uint64]*clicko.Migration, len(versions))
	for _, v := range versions {
		m[v] = &clicko.Migration{Version: v}
	}
	return m
}
