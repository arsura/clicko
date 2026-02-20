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
	AppliedAt   time.Time // Nil if not applied
	Source      MigrationSource
}

// MigrationSource holds the migration's executable content — either SQL
// strings loaded from files, Go functions registered programmatically, or both.
// When both are set, Go functions take precedence.
type MigrationSource struct {
	Type     MigrationSourceType
	UpSQL    string
	DownSQL  string
	UpFunc   GoMigrationFunc
	DownFunc GoMigrationFunc
}

type MigrationSourceType string

const (
	MigrationSourceTypeSQL MigrationSourceType = "sql"
	MigrationSourceTypeGo  MigrationSourceType = "go"
)
