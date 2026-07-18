package pq

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// ExecQuery executes SQL and always closes its result reader.
func ExecQuery(ctx context.Context, conn Connection, sql string) ([]*pgconn.Result, error) {
	reader := conn.Exec(ctx, sql)
	results, readErr := reader.ReadAll()
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	return results, closeErr
}

// ExecSQL executes a SQL statement without returning results.
func ExecSQL(ctx context.Context, conn Connection, sql string) error {
	_, err := ExecQuery(ctx, conn, sql)
	return err
}

// ExecExistsQuery executes a SELECT EXISTS query and returns the boolean result.
func ExecExistsQuery(ctx context.Context, conn Connection, query string) (bool, error) {
	results, err := ExecQuery(ctx, conn, query)
	if err != nil {
		return false, err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, fmt.Errorf("no result returned from existence check")
	}

	return string(results[0].Rows[0][0]) == "t", nil
}

// TableExists checks if a table exists in the given schema using information_schema.
func TableExists(ctx context.Context, conn Connection, schema, table string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = %s
			AND table_name = %s
		)`, QuoteLiteral(schema), QuoteLiteral(table))

	return ExecExistsQuery(ctx, conn, query)
}

// ColumnExists checks whether a table column exists in the given schema.
func ColumnExists(ctx context.Context, conn Connection, schema, table, column string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = %s
			AND table_name = %s
			AND column_name = %s
		)`, QuoteLiteral(schema), QuoteLiteral(table), QuoteLiteral(column))

	return ExecExistsQuery(ctx, conn, query)
}

// IndexExists checks if an index exists in the given schema using pg_indexes.
func IndexExists(ctx context.Context, conn Connection, schema, index string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = %s
			AND indexname = %s
		)`, QuoteLiteral(schema), QuoteLiteral(index))

	return ExecExistsQuery(ctx, conn, query)
}
