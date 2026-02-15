package migrator

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Loader loads migration definitions from a source.
type Loader interface {
	Load() ([]*Migration, error)
}

type fileLoader struct {
	dir string
}

// NewFileLoader returns a Loader that reads SQL migration files from dir.
func NewFileLoader(dir string) Loader {
	return &fileLoader{dir: dir}
}

// Load reads .sql files from the configured directory and returns migrations
// sorted by version in ascending order.
//
// Files must follow the naming convention:
//
//	<version>_<description>.<up|down>.sql
//
// For example:
//
//	00001_create_users.up.sql
//	00001_create_users.down.sql
//
// Validation rules:
//   - Every .sql file must match the naming convention exactly.
//   - Every version must have an .up.sql file; .down.sql is optional.
//   - The up and down files for the same version must share the same description.
func (l *fileLoader) Load() ([]*Migration, error) {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %q: %w", l.dir, err)
	}

	migrationsMap := make(map[uint64]*Migration)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		parts := strings.Split(name, ".")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid migration filename %q: expected <version>_<description>.<up|down>.sql", name)
		}

		directionStr := parts[1]
		if directionStr != MigrationDirectionUp && directionStr != MigrationDirectionDown {
			return nil, fmt.Errorf("invalid migration filename %q: direction must be \"up\" or \"down\", got %q", name, directionStr)
		}

		baseName := parts[0]
		versionParts := strings.SplitN(baseName, "_", 2)

		version, err := strconv.ParseUint(versionParts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid migration filename %q: version %q is not a valid number", name, versionParts[0])
		}

		description := ""
		if len(versionParts) > 1 {
			description = strings.ReplaceAll(versionParts[1], "_", " ")
		}

		m, exists := migrationsMap[version]
		if !exists {
			m = &Migration{
				Version:     version,
				Description: description,
			}
			migrationsMap[version] = m
		} else if m.Description != description {
			return nil, fmt.Errorf(
				"conflicting files for migration version %d: description %q does not match %q"+
					" (all files for the same version must share the same name, or this may be an unintended version collision)",
				version, m.Description, description,
			)
		}

		content, err := os.ReadFile(filepath.Join(l.dir, name))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %q: %w", name, err)
		}

		switch directionStr {
		case MigrationDirectionUp:
			m.Source.UpSQL = string(content)
		case MigrationDirectionDown:
			m.Source.DownSQL = string(content)
		}
	}

	for version, m := range migrationsMap {
		if m.Source.UpSQL == "" {
			return nil, fmt.Errorf("migration version %d (%s) has no .up.sql file", version, m.Description)
		}
	}

	migrations := make([]*Migration, 0, len(migrationsMap))
	for _, m := range migrationsMap {
		migrations = append(migrations, m)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
