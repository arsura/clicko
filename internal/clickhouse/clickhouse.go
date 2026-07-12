package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Dial parses the URI, opens a connection, and verifies it with a ping.
func Dial(ctx context.Context, uri string) (clickhouse.Conn, func() error, error) {
	opts, err := clickhouse.ParseDSN(uri)
	if err != nil {
		// The parse error echoes the raw URI, which may contain credentials.
		// Redacting the echoed text proved brittle (each malformed-password
		// shape needs its own handling), so return a fixed message instead —
		// it keeps credentials out of stderr, CI logs, and shell history,
		// the whole reason the CLI steers users toward the CLICKO_URI env var.
		return nil, nil, errors.New("failed to parse URI: please recheck the URI (expected format: clickhouse://user:pass@host:9000/db)")
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
