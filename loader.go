package clicko

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
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
//   - Every .sql file must match the naming convention exactly. The ".sql"
//     extension is matched case-insensitively; the direction must be
//     lowercase "up" or "down".
//   - Every version must have an .up.sql file; .down.sql is optional.
//   - The up and down files for the same version must share the same description.
//   - Each version may have at most one file per direction. Versions are compared
//     numerically, so "1_x.up.sql" and "01_x.up.sql" collide.
func (l *sqlLoader) Load() ([]*Migration, error) {
	files, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %q: %w", l.dir, err)
	}

	migrationsMap := make(map[uint64]*Migration)
	// seenFiles maps "<version>.<direction>" to the filename that claimed it,
	// so a second file for the same version and direction is rejected instead
	// of silently overwriting the first.
	seenFiles := make(map[string]string)

	for _, file := range files {
		name := file.Name()

		// Case-insensitive so a ".SQL" file isn't silently dropped.
		if !strings.EqualFold(filepath.Ext(name), ".sql") {
			continue
		}

		// Stat follows symlinks, so a symlinked directory is skipped too
		// (file.IsDir() alone would report false for it).
		stat, err := os.Stat(filepath.Join(l.dir, name))
		if err != nil {
			// Dangling symlink (e.g. an editor lockfile): skip, don't fail the load.
			if file.Type()&fs.ModeSymlink != 0 && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("failed to stat %q: %w", name, err)
		}
		if stat.IsDir() {
			continue
		}

		info, err := parseSQLFilename(name)
		if err != nil {
			return nil, err
		}

		key := fmt.Sprintf("%d.%s", info.version, info.direction)
		if prev, dup := seenFiles[key]; dup {
			return nil, fmt.Errorf(
				"conflicting files for migration version %d: %q and %q both define the %q direction"+
					" (versions are compared numerically, so e.g. \"1\" and \"01\" collide)",
				info.version, prev, name, info.direction,
			)
		}
		seenFiles[key] = name

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

		if isEffectivelyEmptySQL(string(content)) {
			return nil, fmt.Errorf("migration file %q is empty (or contains only SQL comments/semicolons, which is not a valid no-op — ClickHouse rejects an empty query)", name)
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

	// Version 0 is reserved: up/down use target=0 to mean "no bound", so a
	// version-0 migration could never be targeted by up-to or kept by down-to.
	if version == 0 {
		return sqlFileInfo{}, fmt.Errorf("invalid migration filename %q: version 0 is reserved, versions must start at 1", name)
	}

	description := ""
	if len(versionParts) > 1 {
		description = strings.ReplaceAll(versionParts[1], "_", " ")
	}

	return sqlFileInfo{version: version, description: description, direction: direction}, nil
}

// blockCommentRegex and lineCommentRegex strip SQL comments so
// isEffectivelyEmptySQL can catch a file with no executable statement.
var (
	blockCommentRegex = regexp.MustCompile(`(?s)/\*.*?\*/`)
	lineCommentRegex  = regexp.MustCompile(`--[^\n]*`)
)

// isEffectivelyEmptySQL reports whether content has no executable statement
// once comments and bare semicolons are stripped. Without this, a .down.sql
// containing only "-- noop" or ";" would pass a plain TrimSpace check and
// fail server-side with ClickHouse's "Empty query" error mid-rollback
// instead of at load time.
func isEffectivelyEmptySQL(content string) bool {
	s := blockCommentRegex.ReplaceAllString(content, "")
	s = lineCommentRegex.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, ";", "")
	return strings.TrimSpace(s) == ""
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
		c := *m
		migrations = append(migrations, &c)
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}
