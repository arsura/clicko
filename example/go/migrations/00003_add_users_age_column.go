package migrations

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

func init() {
	clicko.RegisterMigration(upAddUsersAge, downAddUsersAge)
}

func upAddUsersAge(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `ALTER TABLE users ON CLUSTER dev ADD COLUMN IF NOT EXISTS age UInt8`)
}

func downAddUsersAge(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `ALTER TABLE users ON CLUSTER dev DROP COLUMN IF EXISTS age`)
}
