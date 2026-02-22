package clicko

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
