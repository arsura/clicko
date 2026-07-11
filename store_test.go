package clicko

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreConfig_ResolveEngine(t *testing.T) {
	tests := []struct {
		name     string
		config   StoreConfig
		expected string
	}{
		{
			name:     "no cluster no custom engine uses MergeTree",
			config:   StoreConfig{},
			expected: defaultMergeTreeEngine,
		},
		{
			name:     "cluster without custom engine uses ReplicatedMergeTree with warning",
			config:   StoreConfig{Cluster: "my_cluster"},
			expected: defaultClusterEngine,
		},
		{
			name:     "cluster with custom engine uses custom engine",
			config:   StoreConfig{Cluster: "my_cluster", CustomEngine: "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')"},
			expected: "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')",
		},
		{
			name:     "no cluster with custom engine uses custom engine",
			config:   StoreConfig{CustomEngine: "ReplacingMergeTree()"},
			expected: "ReplacingMergeTree()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.ResolveEngine())
		})
	}
}

func TestNewStore_ValidatesConfigValid(t *testing.T) {
	tests := []struct {
		name   string
		config StoreConfig
	}{
		// --- TableName ---
		{
			name:   "empty table name defaults and is valid",
			config: StoreConfig{},
		},
		{
			name:   "plain table name is valid",
			config: StoreConfig{TableName: "migration_versions"},
		},
		{
			name:   "single character table name is valid",
			config: StoreConfig{TableName: "t"},
		},
		{
			name:   "leading underscore table name is valid",
			config: StoreConfig{TableName: "_migrations"},
		},
		{
			name:   "mixed case with digits table name is valid",
			config: StoreConfig{TableName: "Migrations_v2"},
		},
		{
			name:   "table name with database prefix is valid",
			config: StoreConfig{TableName: "mydb.migrations"},
		},
		// --- Cluster ---
		{
			name:   "empty cluster is valid",
			config: StoreConfig{Cluster: ""},
		},
		{
			name:   "plain cluster name is valid",
			config: StoreConfig{Cluster: "my_cluster"},
		},
		// --- CustomEngine ---
		{
			name:   "empty custom engine is valid",
			config: StoreConfig{CustomEngine: ""},
		},
		{
			name:   "custom engine with legitimate syntax is valid",
			config: StoreConfig{CustomEngine: "ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}')"},
		},
		{
			name:   "custom engine with settings clause is valid",
			config: StoreConfig{CustomEngine: "MergeTree() SETTINGS index_granularity = 8192"},
		},
		// --- InsertQuorum ---
		{
			name:   "numeric insert quorum is valid",
			config: StoreConfig{InsertQuorum: "6"},
		},
		{
			name:   "auto insert quorum is valid",
			config: StoreConfig{InsertQuorum: "auto"},
		},
		// --- Combined ---
		{
			name: "fully valid cluster config passes",
			config: StoreConfig{
				TableName:    "mydb.migration_versions",
				Cluster:      "prod_cluster",
				CustomEngine: "ReplicatedMergeTree('/clickhouse/{cluster}/table/{database}/{table}', '{replica}')",
				InsertQuorum: "auto",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewStore(nil, tt.config)
			require.NoError(t, err)
			assert.NotNil(t, s)
		})
	}
}

func TestNewStore_ValidatesConfigInvalid(t *testing.T) {
	tests := []struct {
		name string
		// errContains is the substring the returned error must contain,
		// pinning down which validator rejected the config.
		config      StoreConfig
		errContains string
	}{
		// --- TableName ---
		{
			name:        "table name with injection payload is rejected",
			config:      StoreConfig{TableName: "t DELETE WHERE 1=1 --"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with backtick is rejected",
			config:      StoreConfig{TableName: "t`; DROP TABLE users --"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with space is rejected",
			config:      StoreConfig{TableName: "my table"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with subquery payload is rejected",
			config:      StoreConfig{TableName: "(SELECT * FROM system.users)"},
			errContains: "invalid table name",
		},
		{
			name:        "table name starting with a digit is rejected",
			config:      StoreConfig{TableName: "1migrations"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with a hyphen is rejected",
			config:      StoreConfig{TableName: "my-migrations"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with leading dot is rejected",
			config:      StoreConfig{TableName: ".migrations"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with trailing dot is rejected",
			config:      StoreConfig{TableName: "mydb."},
			errContains: "invalid table name",
		},
		{
			name:        "table name with double dot is rejected",
			config:      StoreConfig{TableName: "mydb..migrations"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with three qualifiers is rejected",
			config:      StoreConfig{TableName: "a.b.c"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with non-ascii characters is rejected",
			config:      StoreConfig{TableName: "migré"},
			errContains: "invalid table name",
		},
		{
			name:        "table name with trailing newline is rejected",
			config:      StoreConfig{TableName: "migrations\n"},
			errContains: "invalid table name",
		},
		// --- Cluster ---
		{
			name:        "cluster name with backtick breakout is rejected",
			config:      StoreConfig{Cluster: "c` ON CLUSTER x --"},
			errContains: "invalid cluster name",
		},
		{
			name:        "cluster name starting with a digit is rejected",
			config:      StoreConfig{Cluster: "1cluster"},
			errContains: "invalid cluster name",
		},
		{
			name:        "cluster name with a dot is rejected",
			config:      StoreConfig{Cluster: "my.cluster"},
			errContains: "invalid cluster name",
		},
		{
			name:        "cluster name with a hyphen is rejected",
			config:      StoreConfig{Cluster: "my-cluster"},
			errContains: "invalid cluster name",
		},
		// --- CustomEngine ---
		{
			name:        "custom engine with statement terminator is rejected",
			config:      StoreConfig{CustomEngine: "MergeTree(); DROP TABLE users"},
			errContains: "invalid custom engine",
		},
		{
			name:        "custom engine with line comment is rejected",
			config:      StoreConfig{CustomEngine: "MergeTree() -- ORDER BY 1"},
			errContains: "invalid custom engine",
		},
		{
			name:        "custom engine with block comment is rejected",
			config:      StoreConfig{CustomEngine: "MergeTree() /* injected */"},
			errContains: "invalid custom engine",
		},
		// --- InsertQuorum ---
		{
			name:        "non-numeric insert quorum is rejected",
			config:      StoreConfig{InsertQuorum: "not-a-number"},
			errContains: "invalid insert quorum",
		},
		{
			name:        "negative insert quorum is rejected",
			config:      StoreConfig{InsertQuorum: "-1"},
			errContains: "invalid insert quorum",
		},
		{
			name:        "insert quorum with surrounding spaces is rejected",
			config:      StoreConfig{InsertQuorum: " 6 "},
			errContains: "invalid insert quorum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewStore(nil, tt.config)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Nil(t, s)
		})
	}
}

// fixedRowsConn is a clickhouse.Conn whose Query always returns the given
// rows. It embeds dryRunConn for the remaining Conn methods.
type fixedRowsConn struct {
	dryRunConn
	rows driver.Rows
}

func (c *fixedRowsConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	return c.rows, nil
}

// errRows is a driver.Rows that yields no rows and reports err from Err,
// simulating a result stream that was cut off mid-iteration.
type errRows struct {
	emptyRows
	err error
}

func (r *errRows) Err() error { return r.err }

func TestStore_GetAppliedVersions_StreamError(t *testing.T) {
	streamErr := errors.New("read: connection reset by peer")
	conn := &fixedRowsConn{rows: &errRows{err: streamErr}}

	s, err := NewStore(conn, StoreConfig{})
	require.NoError(t, err)

	applied, err := s.GetAppliedVersions(context.Background())
	require.ErrorIs(t, err, streamErr, "a mid-stream error must not be swallowed as an empty result")
	assert.Nil(t, applied)
}

func TestStore_GetAppliedVersions_EmptyResult(t *testing.T) {
	conn := &fixedRowsConn{rows: &errRows{}}

	s, err := NewStore(conn, StoreConfig{})
	require.NoError(t, err)

	applied, err := s.GetAppliedVersions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, applied)
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "plain identifier", input: "my_cluster", expected: "`my_cluster`"},
		{name: "empty string", input: "", expected: "``"},
		{name: "embedded backtick is doubled", input: "a`b", expected: "`a``b`"},
		{name: "trailing backtick breakout is neutralized", input: "c` ON CLUSTER x", expected: "`c`` ON CLUSTER x`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, quoteIdent(tt.input))
		})
	}
}
