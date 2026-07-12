package clicko

import (
	"fmt"
	"maps"
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

// RegisterMigration registers a Go migration, deriving the version from the
// caller's filename (e.g. 20250317141923_create_users.go). Panics if the
// version conflicts with an already-registered migration.
func RegisterMigration(up, down GoMigrationFunc) {
	_, filename, _, _ := runtime.Caller(1)
	RegisterNamedMigration(filename, up, down)
}

// RegisterNamedMigration registers a Go migration with an explicit filename,
// parsing the version from its leading numeric component (e.g.
// "20250317141923_create_users.go" → 20250317141923). Panics if the version
// conflicts with an already-registered migration.
func RegisterNamedMigration(filename string, up, down GoMigrationFunc) {
	if up == nil {
		panic(fmt.Sprintf("failed to add migration %q: up function must not be nil", filename))
	}
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
			Type:     MigrationSourceTypeGo,
			UpFunc:   up,
			DownFunc: down,
		},
	}
}

// parseFilename extracts the numeric version and human-readable description
// from a migration filename (full paths are reduced to the base name), e.g.
// "20250317141923_create_users.go" → (20250317141923, "create users").
func parseFilename(filename string) (uint64, string) {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	parts := strings.SplitN(name, "_", 2)

	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("failed to parse version from filename %q: %v", filename, err))
	}

	// Version 0 is reserved: up/down use target=0 to mean "no bound", so a
	// version-0 migration could never be targeted by up-to or kept by down-to.
	if version == 0 {
		panic(fmt.Sprintf("failed to parse version from filename %q: version 0 is reserved, versions must start at 1", filename))
	}

	description := ""
	if len(parts) > 1 {
		description = strings.ReplaceAll(parts[1], "_", " ")
	}

	return version, description
}

// getGlobalGoMigrations returns a snapshot of the current global Go
// migration registry. The returned map is a shallow copy: the values still
// point into the registry, so callers must copy a Migration before mutating it.
func getGlobalGoMigrations() map[uint64]*Migration {
	mu.Lock()
	defer mu.Unlock()

	snapshot := make(map[uint64]*Migration, len(globalGoMigrations))
	maps.Copy(snapshot, globalGoMigrations)
	return snapshot
}

// ResetGlobalMigrations clears all registered Go migrations.
// Intended for use in tests.
func ResetGlobalMigrations() {
	mu.Lock()
	defer mu.Unlock()
	globalGoMigrations = make(map[uint64]*Migration)
}
