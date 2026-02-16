package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Dial parses the URI, opens a connection, and verifies it with a ping.
func Dial(ctx context.Context, uri string) (clickhouse.Conn, func() error, error) {
	opts, err := clickhouse.ParseDSN(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse URI: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, func() error {
		return conn.Close()
	}, nil
}
