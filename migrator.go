package clicko

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Migrator orchestrates loading migrations (SQL files or Go functions),
// comparing them against the applied state in ClickHouse, and executing them.
type Migrator struct {
	loader Loader
	store  Store
	conn   clickhouse.Conn
	dryRun bool
}

// NewMigrator creates a Migrator with the given connection, loader, and store.
// For most use cases, prefer New, which wires up the store and Go loader automatically.
func NewMigrator(conn clickhouse.Conn, loader Loader, store Store) *Migrator {
	return &Migrator{
		conn:   conn,
		loader: loader,
		store:  store,
	}
}

// SetDryRun enables or disables dry-run mode. When enabled, commands print
// the SQL each migration would execute instead of running it.
func (m *Migrator) SetDryRun(enabled bool) {
	m.dryRun = enabled
}

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) error {
	return m.up(ctx, 0)
}

// up is the shared implementation for Up and UpTo.
// target=0 means apply all pending migrations without an upper bound.
func (m *Migrator) up(ctx context.Context, target uint64) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	appliedCount := 0
	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; ok {
			continue
		}

		if target > 0 && migration.Version > target {
			break
		}

		if m.dryRun {
			m.printMigrationSQL(ctx, migration, MigrationDirectionUp)
		} else {
			log.Printf("Applying migration %d: %s", migration.Version, migration.Description)
			start := time.Now()

			if err := m.applyUp(ctx, migration); err != nil {
				return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
			}

			log.Printf("OK (%v)", time.Since(start))
		}
		appliedCount++
	}

	if appliedCount == 0 {
		log.Println("No pending migrations to apply")
	}

	return nil
}

// loadState ensures the tracking table exists, then returns all known migrations
// from the loader alongside a map of already-applied versions keyed by version number.
func (m *Migrator) loadState(ctx context.Context) ([]*Migration, map[uint64]*Migration, error) {
	if err := m.store.EnsureTable(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ensure migration table: %w", err)
	}

	migrations, err := m.loader.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load migrations: %w", err)
	}

	applied, err := m.store.GetAppliedVersions(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get applied versions: %w", err)
	}

	return migrations, applied, nil
}

// printMigrationSQL prints the SQL a migration would execute in the given
// direction. For Go migrations the function is invoked against a no-op
// connection that captures every Exec/Query call, so dynamically-built SQL
// is shown in its final form.
func (m *Migrator) printMigrationSQL(ctx context.Context, migration *Migration, direction string) {
	fmt.Printf("=== Version %d: %s (%s) ===\n", migration.Version, migration.Description, migration.Source.Type)

	switch migration.Source.Type {
	case MigrationSourceTypeSQL:
		var sql string
		if direction == MigrationDirectionUp {
			sql = migration.Source.UpSQL
		} else {
			sql = migration.Source.DownSQL
		}
		fmt.Println(strings.TrimSpace(sql))
	case MigrationSourceTypeGo:
		dc := &dryRunConn{}
		var fn GoMigrationFunc
		if direction == MigrationDirectionUp {
			fn = migration.Source.UpFunc
		} else {
			fn = migration.Source.DownFunc
		}
		if fn == nil {
			fmt.Println("-- no function defined")
		} else if err := fn(ctx, dc); err != nil {
			fmt.Printf("-- dry-run error: %v\n", err)
		}
		for i, stmt := range dc.statements {
			if i > 0 {
				fmt.Println()
			}
			fmt.Println(stmt)
		}
	}

	fmt.Println()
}

// applyUp executes the up direction of a migration and records it as applied in the store.
func (m *Migrator) applyUp(ctx context.Context, migration *Migration) error {
	switch migration.Source.Type {
	case MigrationSourceTypeGo:
		if err := migration.Source.UpFunc(ctx, m.conn); err != nil {
			return err
		}
	case MigrationSourceTypeSQL:
		if err := m.conn.Exec(ctx, migration.Source.UpSQL); err != nil {
			return err
		}
	default:
		return fmt.Errorf("migration %d has unknown source type: %s", migration.Version, migration.Source.Type)
	}

	return m.store.Add(ctx, migration.Version, migration.Description)
}

// UpTo applies pending migrations up to and including the target version.
func (m *Migrator) UpTo(ctx context.Context, target uint64) error {
	return m.up(ctx, target)
}

// Down reverts the last applied migration.
func (m *Migrator) Down(ctx context.Context) error {
	return m.down(ctx, 0, 1)
}

// down is the shared implementation for Down and DownTo.
// target=0 means no lower bound. limit=0 means no limit on how many to revert.
func (m *Migrator) down(ctx context.Context, target uint64, limit int) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	// Loader returns ascending order; reverse to revert newest first.
	slices.Reverse(migrations)

	revertedCount := 0
	for _, migration := range migrations {
		// Skip versions that aren't applied.
		if _, ok := applied[migration.Version]; !ok {
			continue
		}

		// Migrations are now descending. Once we reach the target version
		// (or below), stop — the target itself should remain applied.
		if target > 0 && migration.Version <= target {
			break
		}

		if !migration.Source.HasDown() {
			log.Printf("Skipping migration %d: %s (forward-only, no down defined)", migration.Version, migration.Description)
			if limit > 0 {
				break
			}
			continue
		}

		if m.dryRun {
			m.printMigrationSQL(ctx, migration, MigrationDirectionDown)
		} else {
			log.Printf("Reverting migration %d: %s", migration.Version, migration.Description)
			start := time.Now()

			if err := m.applyDown(ctx, migration); err != nil {
				return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
			}

			log.Printf("OK (%v)", time.Since(start))
		}
		revertedCount++

		if limit > 0 && revertedCount >= limit {
			break
		}
	}

	if revertedCount == 0 {
		log.Println("No migrations to revert")
	}

	return nil
}

// applyDown executes the down direction of a migration and removes it from the store.
func (m *Migrator) applyDown(ctx context.Context, migration *Migration) error {
	switch migration.Source.Type {
	case MigrationSourceTypeGo:
		if err := migration.Source.DownFunc(ctx, m.conn); err != nil {
			return err
		}
	case MigrationSourceTypeSQL:
		if err := m.conn.Exec(ctx, migration.Source.DownSQL); err != nil {
			return err
		}
	default:
		return fmt.Errorf("migration %d has unknown source type: %s", migration.Version, migration.Source.Type)
	}

	return m.store.Remove(ctx, migration.Version)
}

// DownTo reverts all applied migrations down to (but not including) the target version.
func (m *Migrator) DownTo(ctx context.Context, target uint64) error {
	return m.down(ctx, target, 0)
}

// Reset reverts all applied migrations.
func (m *Migrator) Reset(ctx context.Context) error {
	return m.down(ctx, 0, 0)
}

// Status prints a table showing each migration's version, description,
// status, and when it was applied.
func (m *Migrator) Status(ctx context.Context) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("%-10s %-25s %-10s %s\n", "Version", "Description", "Status", "Applied At")
	fmt.Println(strings.Repeat("-", 70))

	for _, mig := range migrations {
		status := "Pending"
		appliedAt := ""
		if val, ok := applied[mig.Version]; ok {
			status = "Applied"
			appliedAt = val.AppliedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-10d %-25s %-10s %s\n", mig.Version, mig.Description, status, appliedAt)
	}

	return nil
}
