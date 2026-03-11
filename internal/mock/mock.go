package mock

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

// MockStore is an in-memory Store implementation for use in tests.
type MockStore struct {
	Applied map[uint64]*clicko.Migration
}

func (s *MockStore) EnsureTable(_ context.Context) error { return nil }
func (s *MockStore) GetAppliedVersions(_ context.Context) (map[uint64]*clicko.Migration, error) {
	out := make(map[uint64]*clicko.Migration, len(s.Applied))
	for k, v := range s.Applied {
		out[k] = v
	}
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
