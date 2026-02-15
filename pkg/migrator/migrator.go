package migrator

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TODO: ClickHouse has no transactions, so migration SQL and version tracking
// cannot be atomic. If store.Add/Remove fails after SQL succeeds, the state
// becomes inconsistent. Planned improvements:
//   - Retry store operations on transient errors.
//   - Log a clear, actionable error message on permanent failure.
//   - Add "force-add" / "force-remove" CLI commands for manual recovery.

// Migrator orchestrates loading migration files, comparing them against
// the applied state in ClickHouse, and executing the appropriate SQL.
type Migrator struct {
	loader Loader
	store  Store
	conn   clickhouse.Conn
}

func NewMigrator(conn clickhouse.Conn, loader Loader, store Store) *Migrator {
	return &Migrator{
		conn:   conn,
		loader: loader,
		store:  store,
	}
}

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) error {
	return m.up(ctx, 0)
}

// UpTo applies pending migrations up to and including the target version.
func (m *Migrator) UpTo(ctx context.Context, target uint64) error {
	return m.up(ctx, target)
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
		// Skip already-applied versions.
		if _, ok := applied[migration.Version]; ok {
			continue
		}

		// Migrations are sorted ascending by version (from loader).
		// If we've passed the target, everything after is also beyond it.
		if target > 0 && migration.Version > target {
			break
		}

		log.Printf("Applying migration %d: %s", migration.Version, migration.Description)
		start := time.Now()

		if err := m.applyUp(ctx, migration); err != nil {
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		log.Printf("OK (%v)", time.Since(start))
		appliedCount++
	}

	if appliedCount == 0 {
		log.Println("No pending migrations to apply")
	}

	return nil
}

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

func (m *Migrator) applyUp(ctx context.Context, migration *Migration) error {
	if migration.Source.UpSQL == "" {
		return fmt.Errorf("migration %d has no up.sql file", migration.Version)
	}

	if err := m.conn.Exec(ctx, migration.Source.UpSQL); err != nil {
		return err
	}

	return m.store.Add(ctx, migration.Version, migration.Description)
}

// Down reverts the last applied migration.
func (m *Migrator) Down(ctx context.Context) error {
	return m.down(ctx, 0, 1)
}

// DownTo reverts all applied migrations down to (but not including) the target version.
func (m *Migrator) DownTo(ctx context.Context, target uint64) error {
	return m.down(ctx, target, 0)
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

		log.Printf("Reverting migration %d: %s", migration.Version, migration.Description)
		start := time.Now()

		if err := m.applyDown(ctx, migration); err != nil {
			return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
		}

		log.Printf("OK (%v)", time.Since(start))
		revertedCount++

		// Used by Down() to revert only one migration.
		if limit > 0 && revertedCount >= limit {
			break
		}
	}

	if revertedCount == 0 {
		log.Println("No migrations to revert")
	}

	return nil
}

// applyDown executes the down.sql and removes the version from the tracking table.
// Returns an error if the migration has no down.sql file.
func (m *Migrator) applyDown(ctx context.Context, migration *Migration) error {
	if migration.Source.DownSQL == "" {
		return fmt.Errorf("migration %d has no down.sql file, cannot rollback", migration.Version)
	}

	if err := m.conn.Exec(ctx, migration.Source.DownSQL); err != nil {
		return err
	}

	return m.store.Remove(ctx, migration.Version)
}

// Status prints a table showing each migration's version, description,
// and whether it has been applied.
func (m *Migrator) Status(ctx context.Context) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("%-15s %-40s %-20s\n", "Version", "Description", "Status")
	fmt.Println(strings.Repeat("-", 80))

	for _, mig := range migrations {
		status := "Pending"
		if val, ok := applied[mig.Version]; ok {
			status = fmt.Sprintf("Applied (%s)", val.AppliedAt.Format(time.RFC3339))
		}
		fmt.Printf("%-15d %-40s %-20s\n", mig.Version, mig.Description, status)
	}

	return nil
}
