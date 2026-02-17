ALTER TABLE test_cluster_migration ON CLUSTER dev ADD COLUMN IF NOT EXISTS email String DEFAULT '';
