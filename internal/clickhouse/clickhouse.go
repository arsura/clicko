package clickhouse

import (
	"context"
	"fmt"
	"regexp"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Dial parses the URI, opens a connection, and verifies it with a ping.
func Dial(ctx context.Context, uri string) (clickhouse.Conn, func() error, error) {
	opts, err := clickhouse.ParseDSN(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse URI: %s", redactCredentials(err.Error()))
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

// credentialRegex matches the password segment of a DSN userinfo
// (scheme://user:PASSWORD@host) so it can be stripped before the value is
// surfaced in an error message or log line.
var credentialRegex = regexp.MustCompile(`(://[^:/?#@\s]*:)[^@/?#\s]+(@)`)

// redactCredentials replaces the password in any DSN-shaped substring with a
// placeholder. ParseDSN embeds the raw URI in its error, so redacting the
// error text keeps credentials out of stderr, CI logs, and shell history —
// the whole reason the CLI steers users toward the CLICKO_URI env var.
func redactCredentials(s string) string {
	return credentialRegex.ReplaceAllString(s, "${1}xxxxx${2}")
}
