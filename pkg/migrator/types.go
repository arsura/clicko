package migrator

import (
	"time"
)

const (
	MigrationDirectionUp     string = "up"
	MigrationDirectionUpTo   string = "up-to"
	MigrationDirectionDown   string = "down"
	MigrationDirectionDownTo string = "down-to"
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
