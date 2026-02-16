package migrator

import (
	"time"
)

const (
	MigrationDirectionUp   string = "up"
	MigrationDirectionDown string = "down"
)

// Migration represents a single migration.
type Migration struct {
	Version     uint64
	Description string
	AppliedAt   time.Time // Nil if not applied
	Source      MigrationSource
}

// MigrationSource holds the actual SQL content.
type MigrationSource struct {
	UpSQL   string
	DownSQL string
}
