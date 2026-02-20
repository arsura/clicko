package migrations

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

func init() {
	clicko.AddMigration(upCreateUsers, downCreateUsers)
}

func upCreateUsers(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id          UInt64,
			name        String,
			email       String,
			created_at  DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY id
	`)
}

func downCreateUsers(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `DROP TABLE IF EXISTS users`)
}
