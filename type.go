package clicko

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	MigrationDirectionUp   string = "up"
	MigrationDirectionDown string = "down"
)

// GoMigrationFunc is a Go migration function that receives a ClickHouse
// connection. ClickHouse has no transaction support, so the function
// operates directly on the connection.
type GoMigrationFunc func(ctx context.Context, conn clickhouse.Conn) error

// Migration represents a single migration.
type Migration struct {
	Version     uint64
	Description string
	AppliedAt   time.Time
	Source      MigrationSource
}

// MigrationSource holds the migration's executable content — either SQL
// strings loaded from files or Go functions registered programmatically.
// Type selects which pair is executed; content of the other kind is ignored.
type MigrationSource struct {
	Type     MigrationSourceType
	UpSQL    string
	DownSQL  string
	UpFunc   GoMigrationFunc
	DownFunc GoMigrationFunc
}

// HasDown reports whether this migration source has a down (rollback) definition.
// Migrations without a down definition are treated as forward-only: rollback
// operations skip over them — leaving them recorded as applied — and keep
// reverting the migrations underneath.
func (s MigrationSource) HasDown() bool {
	switch s.Type {
	case MigrationSourceTypeGo:
		return s.DownFunc != nil
	case MigrationSourceTypeSQL:
		return s.DownSQL != ""
	}
	return false
}

type MigrationSourceType string

const (
	MigrationSourceTypeSQL MigrationSourceType = "sql"
	MigrationSourceTypeGo  MigrationSourceType = "go"
)
