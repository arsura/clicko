package clicko

import "github.com/ClickHouse/clickhouse-go/v2"

// New creates a Migrator for Go-based migrations.
// It sets up the version-tracking store and Go migration loader internally.
func New(conn clickhouse.Conn, cfg StoreConfig) (*Migrator, error) {
	store, err := NewStore(conn, cfg)
	if err != nil {
		return nil, err
	}

	return NewMigrator(conn, NewGoLoader(), store), nil
}
