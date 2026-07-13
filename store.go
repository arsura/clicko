package clicko

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	DefaultTableName       = "migration_versions"
	defaultClusterEngine   = "ReplicatedMergeTree('/clickhouse/{cluster}/table/{shard}/{database}/{table}', '{replica}')"
	defaultMergeTreeEngine = "MergeTree()"
)

var (
	// identRegex matches a single unquoted ClickHouse identifier.
	identRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// tableNameRegex matches a table name with an optional database qualifier (db.table).
	tableNameRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

	// managedEngineClauses are CREATE TABLE clauses clicko appends itself (e.g.
	// ORDER BY); a CustomEngine containing one would produce malformed DDL.
	// Matched with \s+ so any whitespace between words is caught.
	managedEngineClauses = []struct {
		name string
		re   *regexp.Regexp
	}{
		{"order by", regexp.MustCompile(`(?i)\border\s+by\b`)},
		{"partition by", regexp.MustCompile(`(?i)\bpartition\s+by\b`)},
		{"primary key", regexp.MustCompile(`(?i)\bprimary\s+key\b`)},
		{"sample by", regexp.MustCompile(`(?i)\bsample\s+by\b`)},
		{"settings", regexp.MustCompile(`(?i)\bsettings\b`)},
	}
)

// StoreConfig holds configuration for the migration state store.
//
// TableName, Cluster, and CustomEngine are interpolated directly into SQL
// (identifiers and engine clauses can't use bound parameters), so NewStore
// validates them against injection. CustomEngine is still an unrestricted
// SQL fragment by design — only set it from trusted, operator-controlled
// config, never from untrusted input.
type StoreConfig struct {
	TableName    string
	Cluster      string
	CustomEngine string
	// InsertQuorum sets insert_quorum for migration writes in cluster mode:
	// the number of replicas that must acknowledge a write before it's
	// considered applied. Without it, a node that missed a write may re-run
	// an already-applied migration. Accepts a positive integer or "auto";
	// requires Cluster to be set.
	// https://clickhouse.com/docs/operations/settings/settings#insert_quorum
	InsertQuorum string
}

func (c StoreConfig) IsCluster() bool {
	return c.Cluster != ""
}

// quotedTableName returns TableName backtick-quoted (db and table parts
// separately), so reserved words like "order" work as names.
func (c StoreConfig) quotedTableName() string {
	db, table, qualified := strings.Cut(c.TableName, ".")
	if !qualified {
		return quoteIdent(db)
	}
	return quoteIdent(db) + "." + quoteIdent(table)
}

// quoteIdent wraps an identifier in backticks, doubling any embedded
// backtick. Defense-in-depth: Cluster is already restricted to a plain
// identifier by validate().
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// ResolveEngine returns the engine clause for the tracking table.
// Priority: CustomEngine > ReplicatedMergeTree (cluster) > MergeTree.
func (c StoreConfig) ResolveEngine() string {
	if c.CustomEngine != "" {
		return c.CustomEngine
	}
	if c.IsCluster() {
		return defaultClusterEngine
	}
	return defaultMergeTreeEngine
}

// validate checks every field interpolated into SQL. Assumes defaulting
// (e.g. TableName) already ran.
func (c StoreConfig) validate() error {
	if !tableNameRegex.MatchString(c.TableName) {
		return fmt.Errorf("invalid table name %q: must be a plain identifier (letters, digits, underscores) with an optional database prefix, e.g. \"migrations\" or \"mydb.migrations\"", c.TableName)
	}

	if c.Cluster != "" && !identRegex.MatchString(c.Cluster) {
		return fmt.Errorf("invalid cluster name %q: must be a plain identifier (letters, digits, underscores)", c.Cluster)
	}

	if err := c.validateCustomEngine(); err != nil {
		return err
	}

	return c.validateInsertQuorum()
}

// validateCustomEngine rejects an engine that could break out of the DDL or
// duplicate a clause clicko manages itself.
func (c StoreConfig) validateCustomEngine() error {
	if c.CustomEngine == "" {
		return nil
	}

	if strings.Contains(c.CustomEngine, ";") || strings.Contains(c.CustomEngine, "--") || strings.Contains(c.CustomEngine, "/*") {
		return fmt.Errorf("invalid custom engine %q: must not contain statement terminators (\";\") or comment sequences (\"--\", \"/*\")", c.CustomEngine)
	}

	for _, clause := range managedEngineClauses {
		if clause.re.MatchString(c.CustomEngine) {
			return fmt.Errorf("invalid custom engine %q: must contain only the engine expression; the %q clause is managed by clicko and must not be included", c.CustomEngine, clause.name)
		}
	}

	return nil
}

// validateInsertQuorum requires a positive integer or "auto", and only
// alongside a cluster — Add only applies it in cluster mode.
func (c StoreConfig) validateInsertQuorum() error {
	if c.InsertQuorum == "" {
		return nil
	}

	// 0 disables quorum entirely, defeating the guarantee this flag exists for.
	if c.InsertQuorum != "auto" {
		if q, err := strconv.ParseUint(c.InsertQuorum, 10, 64); err != nil || q < 1 {
			return fmt.Errorf("invalid insert quorum %q: must be a positive integer (>= 1) or \"auto\"", c.InsertQuorum)
		}
	}

	if !c.IsCluster() {
		return fmt.Errorf("insert quorum %q has no effect without a cluster: set Cluster (Go) or --cluster (CLI), or unset the quorum", c.InsertQuorum)
	}

	return nil
}

// Store provides read/write access to the migration state stored in ClickHouse.
type Store interface {
	EnsureTable(ctx context.Context) error
	GetCreateTableDDL() string
	TableExistsEverywhere(ctx context.Context) (bool, error)
	TableExists(ctx context.Context) (bool, error)
	GetAppliedVersions(ctx context.Context) (map[uint64]*Migration, error)
	Add(ctx context.Context, version uint64, description string) error
	Remove(ctx context.Context, version uint64) error
}

type store struct {
	conn   clickhouse.Conn
	config StoreConfig
}

// NewStore creates a Store backed by the given ClickHouse connection.
func NewStore(conn clickhouse.Conn, config StoreConfig) (Store, error) {
	if config.TableName == "" {
		config.TableName = DefaultTableName
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	// Without a quorum, a node that missed a write may treat the migration as
	// unapplied and re-run it.
	if config.IsCluster() && config.InsertQuorum == "" {
		log.Printf("Warning: cluster mode without insert quorum; a node that misses a migration write may re-run the migration — set InsertQuorum (Go) or --insert-quorum (CLI) to the total number of nodes (shards × replicas) or \"auto\"")
	}

	// Warn here, not in ResolveEngine, so a long-lived Migrator doesn't repeat
	// it on every apply/dry-run.
	if config.IsCluster() && config.CustomEngine == "" {
		log.Printf("Warning: no custom engine specified for cluster mode; falling back to the default engine whose ZooKeeper path includes {shard}, which may result in separate replication groups per shard and inconsistent migration state across nodes — set a custom engine with a unified ZooKeeper path to avoid this")
	}

	return &store{
		conn:   conn,
		config: config,
	}, nil
}

// EnsureTable creates the tracking table if it doesn't exist. Checks
// existence first so a run with nothing to create stays read-only — in
// cluster mode the CREATE is an ON CLUSTER DDL that blocks until every host
// responds, which would break every command if a replica is unreachable even
// when the table already exists everywhere.
func (s *store) EnsureTable(ctx context.Context) error {
	exists, err := s.TableExistsEverywhere(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.conn.Exec(ctx, s.GetCreateTableDDL())
}

// TableExistsEverywhere reports whether the tracking table exists on every
// replica (same as TableExists in standalone mode). Gates DDL: a connection
// through a load balancer only sees one node, so anything less than full
// presence means the CREATE DDL still needs to run.
func (s *store) TableExistsEverywhere(ctx context.Context) (bool, error) {
	if s.config.IsCluster() {
		return s.tableExistsOnCluster(ctx)
	}
	return s.TableExists(ctx)
}

// GetCreateTableDDL builds the CREATE TABLE statement for the tracking table.
func (s *store) GetCreateTableDDL() string {
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", s.config.quotedTableName())

	if s.config.IsCluster() {
		createStmt += " ON CLUSTER " + quoteIdent(s.config.Cluster)
	}

	createStmt += fmt.Sprintf(" (\n"+
		"    version UInt64,\n"+
		"    description String,\n"+
		"    applied_at DateTime64(6) DEFAULT now64(6)\n"+
		") ENGINE = %s ORDER BY version", s.config.ResolveEngine())

	return createStmt
}

// tableExistsOnCluster checks every replica via clusterAllReplicas and
// reports true only if all of them have the table.
func (s *store) tableExistsOnCluster(ctx context.Context) (bool, error) {
	db, table, qualified := strings.Cut(s.config.TableName, ".")
	if !qualified {
		table = db
		// currentDatabase() inside the subquery would resolve on the remote
		// nodes, whose default database may differ, so resolve it locally.
		if err := s.conn.QueryRow(ctx, "SELECT currentDatabase()").Scan(&db); err != nil {
			return false, err
		}
	}

	cluster := quoteIdent(s.config.Cluster)
	query := fmt.Sprintf(
		"SELECT (SELECT count() FROM clusterAllReplicas(%s, system.tables) WHERE database = ? AND name = ?)"+
			" = (SELECT count() FROM clusterAllReplicas(%s, system.one))",
		cluster, cluster,
	)

	var existsEverywhere uint8
	if err := s.conn.QueryRow(ctx, query, db, table).Scan(&existsEverywhere); err != nil {
		return false, err
	}
	return existsEverywhere == 1, nil
}

// TableExists reports whether the tracking table exists on the connected
// node. Read-only, unlike EnsureTable. Rows on this node are authoritative
// for applied state even if another replica is still missing the table, so
// this deliberately doesn't check cluster-wide (use TableExistsEverywhere
// for that).
func (s *store) TableExists(ctx context.Context) (bool, error) {
	var exists uint8
	query := fmt.Sprintf("EXISTS TABLE %s", s.config.quotedTableName())
	if err := s.conn.QueryRow(ctx, query).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

// GetAppliedVersions returns all applied migrations keyed by version.
// select_sequential_consistency=1 ensures a load-balanced connection reads
// the latest data in cluster mode.
func (s *store) GetAppliedVersions(ctx context.Context) (map[uint64]*Migration, error) {
	if s.config.IsCluster() {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"select_sequential_consistency": 1,
		}))
	}

	query := fmt.Sprintf("SELECT version, description, applied_at FROM %s ORDER BY version DESC", s.config.quotedTableName())
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[uint64]*Migration)
	for rows.Next() {
		var m Migration
		var appliedAt time.Time
		if err := rows.Scan(&m.Version, &m.Description, &appliedAt); err != nil {
			return nil, err
		}
		m.AppliedAt = appliedAt
		applied[m.Version] = &m
	}

	// A mid-stream error stops Next early with a partial result; without this
	// check those missing rows would look unapplied and get re-run.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return applied, nil
}

// Add records a migration version as applied. insert_quorum is passed via
// context settings since the native driver has no inline SETTINGS for INSERT.
func (s *store) Add(ctx context.Context, version uint64, description string) error {
	if s.config.IsCluster() && s.config.InsertQuorum != "" {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"insert_quorum":          s.config.InsertQuorum,
			"insert_quorum_parallel": 0,
		}))
	}

	insertStmt := fmt.Sprintf("INSERT INTO %s (version, description) VALUES (?, ?)", s.config.quotedTableName())
	return s.conn.Exec(ctx, insertStmt, version, description)
}

// Remove deletes a migration version record. mutations_sync=2 waits for the
// mutation to complete on all replicas.
func (s *store) Remove(ctx context.Context, version uint64) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"mutations_sync": 2,
	}))

	deleteStmt := fmt.Sprintf("ALTER TABLE %s", s.config.quotedTableName())
	if s.config.IsCluster() {
		deleteStmt += " ON CLUSTER " + quoteIdent(s.config.Cluster)
	}

	deleteStmt += fmt.Sprintf(" DELETE WHERE version = %d", version)

	return s.conn.Exec(ctx, deleteStmt)
}
