package clicko

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var _ clickhouse.Conn = (*dryRunConn)(nil)

// dryRunConn implements clickhouse.Conn but captures SQL statements
// instead of executing them. Used by dry-run mode to reveal the actual SQL
// that Go migration functions would send to ClickHouse.
type dryRunConn struct {
	statements []string
}

func (c *dryRunConn) capture(query string, args []any) {
	s := strings.TrimSpace(query)
	if len(args) > 0 {
		s += fmt.Sprintf("\n-- args: %v", args)
	}
	c.statements = append(c.statements, s)
}

func (c *dryRunConn) Exec(_ context.Context, query string, args ...any) error {
	c.capture(query, args)
	return nil
}

func (c *dryRunConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.capture(query, args)
	return &emptyRows{}, nil
}

func (c *dryRunConn) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	c.capture(query, args)
	return &emptyRow{}
}

func (c *dryRunConn) Select(_ context.Context, _ any, query string, args ...any) error {
	c.capture(query, args)
	return nil
}

func (c *dryRunConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.capture(query, nil)
	return nil, fmt.Errorf("dry-run: PrepareBatch is not supported")
}

func (c *dryRunConn) AsyncInsert(_ context.Context, query string, _ bool, args ...any) error {
	c.capture(query, args)
	return nil
}

func (c *dryRunConn) Ping(_ context.Context) error                  { return nil }
func (c *dryRunConn) Close() error                                  { return nil }
func (c *dryRunConn) Stats() driver.Stats                           { return driver.Stats{} }
func (c *dryRunConn) Contributors() []string                        { return nil }
func (c *dryRunConn) ServerVersion() (*driver.ServerVersion, error) { return nil, nil }

// emptyRows implements driver.Rows returning no data.
// Query/Select calls in dry-run mode return this so the migration
// function can proceed without panicking, though any logic depending
// on query results will see zero rows.
type emptyRows struct{}

func (r *emptyRows) Next() bool                       { return false }
func (r *emptyRows) Scan(_ ...any) error              { return nil }
func (r *emptyRows) ScanStruct(_ any) error           { return nil }
func (r *emptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *emptyRows) Totals(_ ...any) error            { return nil }
func (r *emptyRows) Columns() []string                { return nil }
func (r *emptyRows) Close() error                     { return nil }
func (r *emptyRows) Err() error                       { return nil }

// emptyRow implements driver.Row returning no data.
type emptyRow struct{}

func (r *emptyRow) Err() error             { return nil }
func (r *emptyRow) Scan(_ ...any) error    { return fmt.Errorf("dry-run: no data available") }
func (r *emptyRow) ScanStruct(_ any) error { return fmt.Errorf("dry-run: no data available") }
