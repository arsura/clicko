package migrations

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

func init() {
	clicko.AddMigration(upAddUsersAge, nil)
}

func upAddUsersAge(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `ALTER TABLE users ADD COLUMN IF NOT EXISTS age UInt8`)
}
