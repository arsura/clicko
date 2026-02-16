ALTER TABLE test_cli_migration ON CLUSTER dev ADD COLUMN IF NOT EXISTS email String DEFAULT '';
