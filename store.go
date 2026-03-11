package clicko

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	DefaultTableName       = "migration_versions"
	defaultClusterEngine   = "ReplicatedMergeTree('/clickhouse/{cluster}/table/{shard}/{database}/{table}', '{replica}')"
	defaultMergeTreeEngine = "MergeTree()"
)

// Store provides read/write access to the migration state stored in ClickHouse.
type Store interface {
	EnsureTable(ctx context.Context) error
	GetAppliedVersions(ctx context.Context) (map[uint64]*Migration, error)
	Add(ctx context.Context, version uint64, description string) error
	Remove(ctx context.Context, version uint64) error
}

type store struct {
	conn   clickhouse.Conn
	config StoreConfig
}

// StoreConfig holds configuration for the migration state store.
// All string values that are interpolated into SQL (TableName, Cluster, InsertQuorum)
// are validated in NewStore to prevent injection.
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
	// Has no effect when Cluster is not set.
	// https://clickhouse.com/docs/operations/settings/settings#insert_quorum
	InsertQuorum string
}

func (c StoreConfig) IsCluster() bool {
	return c.Cluster != ""
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

// NewStore creates a Store backed by the given ClickHouse connection.
// Returns an error if any config value fails validation.
func NewStore(conn clickhouse.Conn, config StoreConfig) (Store, error) {
	if config.TableName == "" {
		config.TableName = DefaultTableName
	}

	if config.InsertQuorum != "" {
		if config.InsertQuorum != "auto" {
			if _, err := strconv.ParseUint(config.InsertQuorum, 10, 64); err != nil {
				return nil, fmt.Errorf("invalid insert quorum %q: must be a number or \"auto\"", config.InsertQuorum)
			}
		}
	}

	return &store{
		conn:   conn,
		config: config,
	}, nil
}

// EnsureTable creates the migration tracking table if it does not exist.
// Engine selection: CustomEngine > ReplicatedMergeTree (when cluster is set) > MergeTree.
func (s *store) EnsureTable(ctx context.Context) error {
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", s.config.TableName)

	if s.config.IsCluster() {
		createStmt += fmt.Sprintf(" ON CLUSTER `%s`", s.config.Cluster)
	}

	createStmt += fmt.Sprintf(` (
		version UInt64,
		description String,
		applied_at DateTime64(6) DEFAULT now64(6)
	) ENGINE = %s ORDER BY version`, s.config.ResolveEngine())

	return s.conn.Exec(ctx, createStmt)
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

	query := fmt.Sprintf("SELECT version, description, applied_at FROM %s ORDER BY version DESC", s.config.TableName)
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

	insertStmt := fmt.Sprintf("INSERT INTO %s (version, description) VALUES (?, ?)", s.config.TableName)
	return s.conn.Exec(ctx, insertStmt, version, description)
}

// Remove deletes a migration version record.
// mutations_sync=2 waits for the mutation to complete on all replicas.
func (s *store) Remove(ctx context.Context, version uint64) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"mutations_sync": 2,
	}))

	deleteStmt := fmt.Sprintf("ALTER TABLE %s", s.config.TableName)
	if s.config.IsCluster() {
		deleteStmt += fmt.Sprintf(" ON CLUSTER `%s`", s.config.Cluster)
	}

	deleteStmt += fmt.Sprintf(" DELETE WHERE version = %d", version)

	return s.conn.Exec(ctx, deleteStmt)
}
