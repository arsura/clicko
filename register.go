package clicko

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var (
	mu                 sync.Mutex
	globalGoMigrations = make(map[uint64]*Migration)
)

// AddMigration registers a Go migration using the caller's filename to
// derive the version number. The filename must start with a numeric prefix
// (e.g. 20250317141923_create_users.go).
//
// Panics if the version conflicts with an already-registered migration.
func AddMigration(up, down GoMigrationFunc) {
	_, filename, _, _ := runtime.Caller(1)
	AddNamedMigration(filename, up, down)
}

// AddNamedMigration registers a Go migration with an explicit filename.
// The version is parsed from the leading numeric component of the base
// filename (e.g. "20250317141923_create_users.go" → version 20250317141923).
//
// Panics if the version conflicts with an already-registered migration.
func AddNamedMigration(filename string, up, down GoMigrationFunc) {
	version, description := parseFilename(filename)

	mu.Lock()
	defer mu.Unlock()

	if existing, ok := globalGoMigrations[version]; ok {
		panic(fmt.Sprintf(
			"failed to add migration %q: version %d conflicts with %q",
			filename, version, existing.Description,
		))
	}

	globalGoMigrations[version] = &Migration{
		Version:     version,
		Description: description,
		Source: MigrationSource{
			UpFunc:   up,
			DownFunc: down,
		},
	}
}

// parseFilename extracts the numeric version and human-readable description
// from a migration filename. It handles full paths by taking the base name.
//
// Examples:
//
//	"/path/to/20250317141923_create_users.go" → (20250317141923, "create users")
//	"00001_add_column.go"                     → (1, "add column")
func parseFilename(filename string) (uint64, string) {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	parts := strings.SplitN(name, "_", 2)

	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse version from filename %q: %v", filename, err))
	}

	description := ""
	if len(parts) > 1 {
		description = strings.ReplaceAll(parts[1], "_", " ")
	}

	return version, description
}

// getGlobalGoMigrations returns a snapshot of the current global Go
// migration registry. The returned map is a shallow copy safe for
// concurrent reads.
func getGlobalGoMigrations() map[uint64]*Migration {
	mu.Lock()
	defer mu.Unlock()

	snapshot := make(map[uint64]*Migration, len(globalGoMigrations))
	for k, v := range globalGoMigrations {
		snapshot[k] = v
	}
	return snapshot
}

// ResetGlobalMigrations clears all registered Go migrations.
// Intended for use in tests.
func ResetGlobalMigrations() {
	mu.Lock()
	defer mu.Unlock()
	globalGoMigrations = make(map[uint64]*Migration)
}
