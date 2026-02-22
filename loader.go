package clicko

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

type sqlLoader struct {
	dir string
}

// NewSQLLoader returns a clicko.Loader that reads SQL migration files from dir.
func NewSQLLoader(dir string) Loader {
	return &sqlLoader{dir: dir}
}

type sqlFileInfo struct {
	version     uint64
	description string
	direction   string
}

// parseSQLFilename parses a SQL migration filename into its components.
// Expected format: <version>_<description>.<up|down>.sql
func parseSQLFilename(name string) (sqlFileInfo, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return sqlFileInfo{}, fmt.Errorf("invalid migration filename %q: expected <version>_<description>.<up|down>.sql", name)
	}

	direction := parts[1]
	if direction != MigrationDirectionUp && direction != MigrationDirectionDown {
		return sqlFileInfo{}, fmt.Errorf("invalid migration filename %q: direction must be \"up\" or \"down\", got %q", name, direction)
	}

	versionParts := strings.SplitN(parts[0], "_", 2)
	version, err := strconv.ParseUint(versionParts[0], 10, 64)
	if err != nil {
		return sqlFileInfo{}, fmt.Errorf("invalid migration filename %q: version %q is not a valid number", name, versionParts[0])
	}

	description := ""
	if len(versionParts) > 1 {
		description = strings.ReplaceAll(versionParts[1], "_", " ")
	}

	return sqlFileInfo{version: version, description: description, direction: direction}, nil
}

// validateUpFilesExist returns an error if any migration version has no .up.sql file.
func validateUpFilesExist(migrationsMap map[uint64]*Migration) error {
	for version, m := range migrationsMap {
		if m.Source.UpSQL == "" {
			return fmt.Errorf("migration version %d (%s) has no .up.sql file", version, m.Description)
		}
	}
	return nil
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
func (l *sqlLoader) Load() ([]*Migration, error) {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %q: %w", l.dir, err)
	}

	migrationsMap := make(map[uint64]*Migration)

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		name := file.Name()
		info, err := parseSQLFilename(name)
		if err != nil {
			return nil, err
		}

		m, exists := migrationsMap[info.version]
		if !exists {
			m = &Migration{Version: info.version, Description: info.description}
			migrationsMap[info.version] = m
		} else if m.Description != info.description {
			return nil, fmt.Errorf(
				"conflicting files for migration version %d: description %q does not match %q"+
					" (all files for the same version must share the same name, or this may be an unintended version collision)",
				info.version, m.Description, info.description,
			)
		}

		content, err := os.ReadFile(filepath.Join(l.dir, name))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %q: %w", name, err)
		}

		switch info.direction {
		case MigrationDirectionUp:
			m.Source.UpSQL = string(content)
		case MigrationDirectionDown:
			m.Source.DownSQL = string(content)
		}
	}

	if err := validateUpFilesExist(migrationsMap); err != nil {
		return nil, err
	}

	migrations := make([]*Migration, 0, len(migrationsMap))
	for _, m := range migrationsMap {
		m.Source.Type = MigrationSourceTypeSQL
		migrations = append(migrations, m)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

type goLoader struct{}

// NewGoLoader returns a Loader that reads migrations from the global Go
// migration registry populated by RegisterMigration / RegisterNamedMigration.
func NewGoLoader() Loader {
	return &goLoader{}
}

// Load returns all registered Go migrations sorted by version in ascending order.
func (l *goLoader) Load() ([]*Migration, error) {
	registered := getGlobalGoMigrations()

	migrations := make([]*Migration, 0, len(registered))
	for _, m := range registered {
		m.Source.Type = MigrationSourceTypeGo
		migrations = append(migrations, m)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
