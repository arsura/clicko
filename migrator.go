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
	loader          Loader
	store           Store
	conn            clickhouse.Conn
	dryRun          bool
	allowOutOfOrder bool
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

// SetAllowOutOfOrder allows a pending migration with a lower version than
// the highest applied one to run instead of erroring. Off by default; enable
// only when the migration is independent of anything already applied.
func (m *Migrator) SetAllowOutOfOrder(enabled bool) {
	m.allowOutOfOrder = enabled
}

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) error {
	return m.up(ctx, 0)
}

// up is the shared implementation for Up and UpTo. target=0 means no upper bound.
func (m *Migrator) up(ctx context.Context, target uint64) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	err = m.checkOutOfOrder(migrations, applied, target)
	if err != nil {
		return err
	}

	warnUnknownTarget(migrations, target)

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

// loadState returns all known migrations from the loader alongside a map of
// already-applied versions keyed by version number.
func (m *Migrator) loadState(ctx context.Context) ([]*Migration, map[uint64]*Migration, error) {
	applied, err := m.loadAppliedVersions(ctx)
	if err != nil {
		return nil, nil, err
	}

	migrations, err := m.loader.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load migrations: %w", err)
	}

	return migrations, applied, nil
}

// loadAppliedVersions creates the tracking table if missing, then reads
// applied migrations. Dry-run must not write, so it delegates to
// dryRunAppliedVersions instead.
func (m *Migrator) loadAppliedVersions(ctx context.Context) (map[uint64]*Migration, error) {
	if m.dryRun {
		return m.dryRunAppliedVersions(ctx)
	}

	if err := m.store.EnsureTable(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure migration table: %w", err)
	}

	applied, err := m.store.GetAppliedVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied versions: %w", err)
	}

	return applied, nil
}

// dryRunAppliedVersions is the read-only variant of loadAppliedVersions: it
// checks table existence instead of creating it, and previews the CREATE DDL
// whenever an apply would actually run it (table missing on any replica).
func (m *Migrator) dryRunAppliedVersions(ctx context.Context) (map[uint64]*Migration, error) {
	existsEverywhere, err := m.store.TableExistsEverywhere(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check migration table: %w", err)
	}

	if !existsEverywhere {
		fmt.Println("=== Migration tracking table (would be created on apply) ===")
		fmt.Println(m.store.GetCreateTableDDL())
		fmt.Println()

		exists, err := m.store.TableExists(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to check migration table: %w", err)
		}
		if !exists {
			return make(map[uint64]*Migration), nil
		}
	}

	applied, err := m.store.GetAppliedVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied versions: %w", err)
	}

	return applied, nil
}

// printMigrationSQL prints the SQL a migration would execute in the given
// direction. Go migrations run against a no-op connection that captures
// every Exec/Query call, so dynamically-built SQL is shown in its final form.
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

// applyUp executes the up direction of a migration and records it as applied.
func (m *Migrator) applyUp(ctx context.Context, migration *Migration) error {
	switch migration.Source.Type {
	case MigrationSourceTypeGo:
		if migration.Source.UpFunc == nil {
			return fmt.Errorf("migration %d has no up function", migration.Version)
		}
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
// Target 0 is rejected: it's reserved, and up() uses 0 as its internal
// "no bound" sentinel, so a 0 target would silently behave like Up.
func (m *Migrator) UpTo(ctx context.Context, target uint64) error {
	if target == 0 {
		return fmt.Errorf("target version 0 is reserved (migration versions start at 1); to apply all pending migrations use \"up\" (CLI) or Up (Go)")
	}
	return m.up(ctx, target)
}

// Down reverts the last applied migration that has a down direction defined,
// skipping over (and leaving applied) any forward-only migrations above it.
func (m *Migrator) Down(ctx context.Context) error {
	return m.down(ctx, 0, 1)
}

// down is the shared implementation for Down and DownTo. target=0 means no
// lower bound; limit=0 means no limit on how many to revert.
func (m *Migrator) down(ctx context.Context, target uint64, limit int) error {
	migrations, applied, err := m.loadState(ctx)
	if err != nil {
		return err
	}

	warnUnknownTarget(migrations, target)

	migrations = slices.Clone(migrations)
	slices.Reverse(migrations)

	revertedCount := 0
	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; !ok {
			continue
		}

		// Descending order now; the target itself stays applied.
		if target > 0 && migration.Version <= target {
			break
		}

		// No SQL/Go function to unwind a forward-only migration. Leave it
		// applied and keep reverting the independent migrations underneath.
		if !migration.Source.HasDown() {
			log.Printf("Skipping migration %d: %s (forward-only, no down defined; left applied)", migration.Version, migration.Description)
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

// DownTo reverts applied migrations down to (but not including) the target
// version, skipping forward-only migrations along the way. Target 0 is
// rejected the same way as UpTo, since down() uses 0 as its "no bound"
// sentinel and 0 would silently behave like Reset.
func (m *Migrator) DownTo(ctx context.Context, target uint64) error {
	if target == 0 {
		return fmt.Errorf("target version 0 is reserved (migration versions start at 1); to revert all applied migrations use \"reset\" (CLI) or Reset (Go)")
	}
	return m.down(ctx, target, 0)
}

// Reset reverts all applied migrations, skipping forward-only migrations
// along the way.
func (m *Migrator) Reset(ctx context.Context) error {
	return m.down(ctx, 0, 0)
}

// warnUnknownTarget warns when an up-to/down-to target matches no known
// migration — likely a typo (e.g. a mistyped timestamp) rather than an
// intentional bound. target=0 is the internal sentinel and is exempt.
func warnUnknownTarget(migrations []*Migration, target uint64) {
	if target == 0 {
		return
	}
	known := slices.ContainsFunc(migrations, func(m *Migration) bool {
		return m.Version == target
	})
	if !known {
		log.Printf("Warning: target version %d does not match any known migration", target)
	}
}

// checkOutOfOrder detects pending migrations, up to target (0 = no bound),
// with a version lower than the highest applied one. Errors listing every
// offender unless allowOutOfOrder is set, in which case it warns instead.
func (m *Migrator) checkOutOfOrder(migrations []*Migration, applied map[uint64]*Migration, target uint64) error {
	if len(applied) == 0 {
		return nil
	}

	var maxApplied uint64
	for v := range applied {
		if v > maxApplied {
			maxApplied = v
		}
	}

	var outOfOrder []uint64
	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; ok {
			continue
		}
		if target > 0 && migration.Version > target {
			break
		}
		if migration.Version < maxApplied {
			outOfOrder = append(outOfOrder, migration.Version)
		}
	}

	if len(outOfOrder) == 0 {
		return nil
	}

	if !m.allowOutOfOrder {
		return fmt.Errorf(
			"out-of-order migration detected: version(s) %v are pending but version %d is already applied; verify that the migration is independent of any previously applied changes before proceeding — if intentional, use --allow-out-of-order (CLI) or SetAllowOutOfOrder(true) (Go) to apply anyway",
			outOfOrder, maxApplied,
		)
	}

	for _, v := range outOfOrder {
		log.Printf("Warning: applying out-of-order migration %d (version %d is already applied)", v, maxApplied)
	}
	return nil
}

// Status prints each migration's version, description, status, and applied
// time. Read-only — never creates the tracking table — so it works with
// credentials that lack DDL permissions.
func (m *Migrator) Status(ctx context.Context) error {
	applied, err := m.readAppliedVersions(ctx)
	if err != nil {
		return err
	}

	migrations, err := m.loader.Load()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
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

// readAppliedVersions reads applied migrations without executing DDL: an
// empty map (everything pending) if the tracking table doesn't exist on the
// connected node. Deliberately local, not cluster-wide — rows here are
// authoritative even if another replica is still missing the table.
func (m *Migrator) readAppliedVersions(ctx context.Context) (map[uint64]*Migration, error) {
	exists, err := m.store.TableExists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check migration table: %w", err)
	}

	if !exists {
		return make(map[uint64]*Migration), nil
	}

	applied, err := m.store.GetAppliedVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get applied versions: %w", err)
	}

	return applied, nil
}
