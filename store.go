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
)

// StoreConfig holds configuration for the migration state store.
//
// Several fields are interpolated directly into SQL statements (identifiers and
// engine clauses cannot use bound parameters). NewStore validates them to prevent
// SQL injection:
//   - TableName must be a plain identifier with an optional database prefix (db.table).
//   - Cluster must be a plain identifier.
//   - CustomEngine must not contain statement-terminating or comment sequences.
//   - InsertQuorum must be a positive integer or "auto".
//
// Even so, CustomEngine is an unrestricted SQL fragment by design and should only
// ever be set from trusted, operator-controlled configuration — never from
// untrusted or end-user input.
type StoreConfig struct {
	TableName    string
	Cluster      string
	CustomEngine string
	// InsertQuorum controls the insert_quorum setting for migration writes in cluster mode.
	// Set this to the total number of nodes in the cluster (shards × replicas per shard)
	// so every node must acknowledge the write before it is considered successful.
	// This is necessary because the migration table is replicated across all nodes via a single
	// ZooKeeper path — a node that missed the write would report the migration as not applied.
	// Accepts a positive integer (e.g. "6" for 3 shards × 2 replicas) or "auto".
	// Requires Cluster to be set: NewStore rejects a quorum without a cluster,
	// since it would otherwise be silently ignored.
	// https://clickhouse.com/docs/operations/settings/settings#insert_quorum
	InsertQuorum string
}

func (c StoreConfig) IsCluster() bool {
	return c.Cluster != ""
}

// quotedTableName returns TableName with each identifier backtick-quoted
// (quoting the database and table parts separately), so reserved words like
// "order" or "table" work as names instead of failing with a server-side
// syntax error.
func (c StoreConfig) quotedTableName() string {
	db, table, qualified := strings.Cut(c.TableName, ".")
	if !qualified {
		return quoteIdent(db)
	}
	return quoteIdent(db) + "." + quoteIdent(table)
}

// quoteIdent wraps a ClickHouse identifier in backticks, escaping any embedded
// backtick by doubling it. NewStore already restricts Cluster to a plain
// identifier, so this is defense-in-depth for the SQL built from it: it keeps
// the value from breaking out of the backtick quoting even if that invariant
// were ever weakened.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// ResolveEngine returns the engine clause to use when creating the migration table.
// Priority: CustomEngine > ReplicatedMergeTree (cluster, with warning) > MergeTree.
func (c StoreConfig) ResolveEngine() string {
	if c.CustomEngine != "" {
		return c.CustomEngine
	}
	if c.IsCluster() {
		log.Printf("Warning: no custom engine specified for cluster mode; falling back to the default engine whose ZooKeeper path includes {shard}, which may result in separate replication groups per shard and inconsistent migration state across nodes — set a custom engine with a unified ZooKeeper path to avoid this")
		return defaultClusterEngine
	}
	return defaultMergeTreeEngine
}

// validate checks every field that is interpolated into SQL. It assumes any
// defaulting (e.g. TableName) has already been applied by the caller.
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

// validateCustomEngine rejects a CustomEngine that could break out of the DDL
// or that includes a clause clicko manages itself.
func (c StoreConfig) validateCustomEngine() error {
	if c.CustomEngine == "" {
		return nil
	}

	if strings.Contains(c.CustomEngine, ";") || strings.Contains(c.CustomEngine, "--") || strings.Contains(c.CustomEngine, "/*") {
		return fmt.Errorf("invalid custom engine %q: must not contain statement terminators (\";\") or comment sequences (\"--\", \"/*\")", c.CustomEngine)
	}

	// managedEngineClauses are CREATE TABLE clauses that clicko appends or controls
	// itself. They must not appear in a CustomEngine, which is only the engine
	// expression — otherwise the generated DDL would be malformed (e.g. a duplicate
	// ORDER BY, or a SETTINGS clause placed before the appended ORDER BY).
	managedEngineClauses := []string{"order by", "partition by", "primary key", "sample by", "settings"}

	// CustomEngine must be the engine expression only. clicko controls the
	// tracking table's schema and appends "ORDER BY version" itself, so any
	// managed clause here would produce a malformed DDL. Reject it upfront with a
	// clear message instead of surfacing a confusing server-side parse error.
	lowerEngine := strings.ToLower(c.CustomEngine)
	for _, clause := range managedEngineClauses {
		if strings.Contains(lowerEngine, clause) {
			return fmt.Errorf("invalid custom engine %q: must contain only the engine expression; the %q clause is managed by clicko and must not be included", c.CustomEngine, clause)
		}
	}

	return nil
}

// validateInsertQuorum ensures the quorum is a positive integer or "auto",
// and that it is only set alongside a cluster — Add only applies it in
// cluster mode, so accepting it without one would silently do nothing.
func (c StoreConfig) validateInsertQuorum() error {
	if c.InsertQuorum == "" {
		return nil
	}

	// insert_quorum=0 disables quorum entirely, which silently defeats the
	// consistency guarantee this flag exists to provide, so require >= 1.
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
// Returns an error if any config value fails validation.
func NewStore(conn clickhouse.Conn, config StoreConfig) (Store, error) {
	if config.TableName == "" {
		config.TableName = DefaultTableName
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	// Without a quorum, a migration write acknowledged by one replica can be
	// missing on another; a node that missed it would consider the migration
	// unapplied and re-run it. That silently defeats the consistency the
	// tracking table exists to provide, so call it out.
	if config.IsCluster() && config.InsertQuorum == "" {
		log.Printf("Warning: cluster mode without insert quorum; a node that misses a migration write may re-run the migration — set InsertQuorum (Go) or --insert-quorum (CLI) to the total number of nodes (shards × replicas) or \"auto\"")
	}

	return &store{
		conn:   conn,
		config: config,
	}, nil
}

// EnsureTable creates the migration tracking table if it does not exist.
// Engine selection: CustomEngine > ReplicatedMergeTree (when cluster is set) > MergeTree.
//
// The existence check runs first so that a run with nothing to create stays
// read-only. In cluster mode the CREATE is an ON CLUSTER DDL that queues a
// ZooKeeper task and waits for every host — with one replica unreachable it
// blocks until distributed_ddl_task_timeout and then fails, which would break
// every command even when the table already exists everywhere.
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
// replica of the cluster; in standalone mode it is the same as TableExists.
// It is the gate for DDL: a connection through a load balancer only sees one
// node, and anything less than full presence means an apply would still need
// to run the ON CLUSTER CREATE DDL to converge the cluster.
func (s *store) TableExistsEverywhere(ctx context.Context) (bool, error) {
	if s.config.IsCluster() {
		return s.tableExistsOnCluster(ctx)
	}
	return s.TableExists(ctx)
}

// GetCreateTableDDL builds the CREATE TABLE statement for the tracking table.
// EnsureTable executes it; dry-run mode prints it as a preview.
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

// tableExistsOnCluster checks every replica via clusterAllReplicas and reports
// true only when the tracking table is present on all of them.
func (s *store) tableExistsOnCluster(ctx context.Context) (bool, error) {
	db, table, qualified := strings.Cut(s.config.TableName, ".")
	if !qualified {
		table = db
		// Resolve the connection's database locally: currentDatabase() inside
		// a clusterAllReplicas subquery would evaluate on the remote nodes,
		// whose default database may differ from this session's.
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
// node. It is read-only — unlike EnsureTable, it never executes DDL. This is
// the gate for reading applied state: rows recorded on the connected node are
// authoritative even when another replica is still missing the table, so
// answering cluster-wide here would misreport applied migrations as pending.
// Use TableExistsEverywhere to decide whether the CREATE DDL must run.
func (s *store) TableExists(ctx context.Context) (bool, error) {
	var exists uint8
	query := fmt.Sprintf("EXISTS TABLE %s", s.config.quotedTableName())
	if err := s.conn.QueryRow(ctx, query).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

// GetAppliedVersions returns all applied migrations keyed by version number.
// In cluster mode, select_sequential_consistency=1 ensures we read the latest complete
// data when connecting through a load balancer to arbitrary replicas.
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

	// A mid-stream error makes Next return false with a partial result; without
	// this check the migrator would treat missing rows as unapplied and re-run
	// already-applied migrations.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return applied, nil
}

// Add records a migration version as applied.
// For cluster mode, insert_quorum is passed via context settings
// because the native ClickHouse driver does not support inline SETTINGS in INSERT.
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

// Remove deletes a migration version record.
// mutations_sync=2 waits for the mutation to complete on all replicas.
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
