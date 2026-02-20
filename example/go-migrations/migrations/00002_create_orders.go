package migrations

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arsura/clicko"
)

func init() {
	clicko.AddMigration(upCreateOrders, downCreateOrders)
}

func upCreateOrders(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id          UInt64,
			user_id     UInt64,
			amount      Decimal64(2),
			status      LowCardinality(String),
			created_at  DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (user_id, id)
	`)
}

func downCreateOrders(ctx context.Context, conn clickhouse.Conn) error {
	return conn.Exec(ctx, `DROP TABLE IF EXISTS orders`)
}
