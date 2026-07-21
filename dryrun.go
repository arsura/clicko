package clicko

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
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
	c.statements = append(c.statements, FormatDryRunStatement(query, args))
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
	return &dryRunBatch{}, nil
}

func (c *dryRunConn) AsyncInsert(_ context.Context, query string, _ bool, args ...any) error {
	c.capture(query, args)
	return nil
}

func (c *dryRunConn) Ping(_ context.Context) error { return nil }
func (c *dryRunConn) Close() error                 { return nil }
func (c *dryRunConn) Stats() driver.Stats          { return driver.Stats{} }
func (c *dryRunConn) Contributors() []string       { return nil }
func (c *dryRunConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}

// emptyRows implements driver.Rows returning no data, so a Query/Select call
// in dry-run mode can proceed without panicking (any logic depending on
// results will just see zero rows).
type emptyRows struct{}

var _ driver.Rows = (*emptyRows)(nil)

func (r *emptyRows) Next() bool                       { return false }
func (r *emptyRows) HasData() bool                    { return false }
func (r *emptyRows) Scan(_ ...any) error              { return nil }
func (r *emptyRows) ScanStruct(_ any) error           { return nil }
func (r *emptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *emptyRows) Totals(_ ...any) error            { return nil }
func (r *emptyRows) Columns() []string                { return nil }
func (r *emptyRows) Close() error                     { return nil }
func (r *emptyRows) Err() error                       { return nil }

// dryRunBatch implements driver.Batch as a no-op so PrepareBatch callers can
// proceed in dry-run mode; appended rows are discarded (the INSERT statement
// itself is already captured by PrepareBatch).
type dryRunBatch struct{}

var _ driver.Batch = (*dryRunBatch)(nil)

func (b *dryRunBatch) Abort() error                    { return nil }
func (b *dryRunBatch) Append(_ ...any) error           { return nil }
func (b *dryRunBatch) AppendStruct(_ any) error        { return nil }
func (b *dryRunBatch) Column(_ int) driver.BatchColumn { return &dryRunBatchColumn{} }
func (b *dryRunBatch) Flush() error                    { return nil }
func (b *dryRunBatch) Send() error                     { return nil }
func (b *dryRunBatch) IsSent() bool                    { return false }
func (b *dryRunBatch) Rows() int                       { return 0 }
func (b *dryRunBatch) Columns() []column.Interface     { return nil }
func (b *dryRunBatch) Close() error                    { return nil }

// dryRunBatchColumn implements driver.BatchColumn as a no-op, discarding any
// values appended via the column-oriented batch API.
type dryRunBatchColumn struct{}

var _ driver.BatchColumn = (*dryRunBatchColumn)(nil)

func (c *dryRunBatchColumn) Append(_ any) error    { return nil }
func (c *dryRunBatchColumn) AppendRow(_ any) error { return nil }

// errDryRunNoData is returned by the dry-run row scanners: dry-run captures SQL
// without executing it, so no result rows are ever available to scan.
var errDryRunNoData = errors.New("dry-run: no data available")

// emptyRow implements driver.Row returning no data.
type emptyRow struct{}

func (r *emptyRow) Err() error             { return nil }
func (r *emptyRow) Scan(_ ...any) error    { return errDryRunNoData }
func (r *emptyRow) ScanStruct(_ any) error { return errDryRunNoData }

// sensitiveSQLPattern matches SQL that likely embeds credentials. Dry-run omits
// matching statements instead of printing them.
var sensitiveSQLPattern = regexp.MustCompile(`(?i)\b(?:identified\s+by|aws_secret_access_key|secret_access_key|kafka_sasl_password|sasl_password|password)\s`)

const dryRunOmittedStatement = "-- statement omitted (matched credential pattern)"

// FormatDryRunSQL returns sql trimmed for dry-run output, or an omission marker
// when the statement likely contains credentials.
func FormatDryRunSQL(sql string) string {
	if sensitiveSQLPattern.MatchString(sql) {
		return dryRunOmittedStatement
	}
	return strings.TrimSpace(sql)
}

// FormatDryRunStatement formats a captured dry-run query and its bound args.
func FormatDryRunStatement(query string, args []any) string {
	if sensitiveSQLPattern.MatchString(query) {
		return dryRunOmittedStatement
	}
	s := strings.TrimSpace(query)
	if len(args) == 0 {
		return s
	}
	s += fmt.Sprintf("\n-- args: %v", args)
	return s
}
