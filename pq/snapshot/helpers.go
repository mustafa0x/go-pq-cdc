package snapshot

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
)

const (
	postgresTimestampFormat       = "2006-01-02 15:04:05"
	postgresTimestampFormatMicros = "2006-01-02 15:04:05.999999"
)

func withTransaction(ctx context.Context, conn pq.Connection, fn func(pq.Connection) error) error {
	if err := pq.ExecSQL(ctx, conn, "BEGIN"); err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), snapshotCleanupTimeout)
		defer cancel()
		_ = pq.ExecSQL(rollbackCtx, conn, "ROLLBACK")
	}()

	if err := fn(conn); err != nil {
		return err
	}
	if err := pq.ExecSQL(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *Snapshotter) getSnapshotColumns(schema, name string) []string {
	schema = normalizeSchema(schema)
	for _, table := range s.tables {
		if normalizeSchema(table.Schema) == schema && table.Name == name {
			return table.Columns
		}
	}
	return nil
}

func normalizeSchema(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}

func nullableInt64SQL(value *int64) string {
	if value == nil {
		return "NULL"
	}
	return strconv.FormatInt(*value, 10)
}

func parseNullableInt64(value []byte) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parsePostgresBool(value []byte) (bool, error) {
	switch string(value) {
	case "t", "true":
		return true, nil
	case "f", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid PostgreSQL boolean %q", value)
	}
}

func selectSnapshotColumns(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = pq.QuoteIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}
