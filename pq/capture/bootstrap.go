package capture

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

// SlotSnapshot is the boundary returned by CREATE_REPLICATION_SLOT with an
// exported snapshot. The snapshot must be imported before the exporting
// session executes another command or closes.
type SlotSnapshot struct {
	ConsistentPoint pq.LSN
	SnapshotName    string
	OutputPlugin    string
}

// Batch is one bounded relation-scoped snapshot batch.
type Batch struct {
	Schema string
	Table  string
	Rows   []map[string]any
}

// SetTextFormat fixes the PostgreSQL text representation shared by snapshot
// scanning and pgoutput text decoding. It must run before slot creation or the
// imported snapshot transaction begins.
func SetTextFormat(ctx context.Context, conn pq.Connection) error {
	for _, statement := range []string{
		"SET TIME ZONE 'UTC'",
		"SET DateStyle TO 'ISO, YMD'",
		"SET IntervalStyle TO 'postgres'",
		"SET extra_float_digits TO 3",
		"SET bytea_output TO 'hex'",
	} {
		if err := pq.ExecSQL(ctx, conn, statement); err != nil {
			return fmt.Errorf("configure capture text format: %w", err)
		}
	}
	return nil
}

// CreateSlotSnapshot creates a persistent pgoutput slot and exports its
// matching snapshot on the caller-owned replication session.
func CreateSlotSnapshot(ctx context.Context, conn pq.Connection, slotName string) (SlotSnapshot, error) {
	query := fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (SNAPSHOT 'export', TWO_PHASE false)",
		pq.QuoteIdentifier(slotName),
	)
	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return SlotSnapshot{}, fmt.Errorf("create replication slot with exported snapshot: %w", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 {
		return SlotSnapshot{}, fmt.Errorf("create replication slot returned an unexpected result")
	}
	result := results[0]
	row := result.Rows[0]
	if len(row) != len(result.FieldDescriptions) {
		return SlotSnapshot{}, fmt.Errorf("create replication slot result shape is invalid")
	}

	values := make(map[string]string, len(row))
	for i, field := range result.FieldDescriptions {
		values[field.Name] = string(row[i])
	}
	consistentPoint, err := pq.ParseLSN(values["consistent_point"])
	if err != nil {
		return SlotSnapshot{}, fmt.Errorf("parse slot consistent point: %w", err)
	}
	boundary := SlotSnapshot{
		ConsistentPoint: consistentPoint,
		SnapshotName:    values["snapshot_name"],
		OutputPlugin:    values["output_plugin"],
	}
	if boundary.SnapshotName == "" || boundary.OutputPlugin != "pgoutput" {
		return SlotSnapshot{}, fmt.Errorf("create replication slot returned invalid snapshot/plugin identity")
	}
	return boundary, nil
}

// DropSlot abandons a bootstrap slot before streaming begins.
func DropSlot(ctx context.Context, conn pq.Connection, slotName string) error {
	if slotName == "" {
		return fmt.Errorf("slot name cannot be empty")
	}
	if err := pq.ExecSQL(ctx, conn, "DROP_REPLICATION_SLOT "+pq.QuoteIdentifier(slotName)); err != nil {
		return fmt.Errorf("drop replication slot: %w", err)
	}
	return nil
}

// ImportSnapshot begins one read-only repeatable-read transaction and imports
// the slot-exported snapshot as its first query-like operation.
func ImportSnapshot(ctx context.Context, conn pq.Connection, snapshotName string) error {
	if snapshotName == "" {
		return fmt.Errorf("snapshot name cannot be empty")
	}
	if err := SetTextFormat(ctx, conn); err != nil {
		return err
	}
	if err := pq.ExecSQL(ctx, conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return fmt.Errorf("begin imported snapshot transaction: %w", err)
	}
	if err := pq.ExecSQL(ctx, conn, "SET TRANSACTION SNAPSHOT "+pq.QuoteLiteral(snapshotName)); err != nil {
		_ = pq.ExecSQL(context.WithoutCancel(ctx), conn, "ROLLBACK")
		return fmt.Errorf("import slot snapshot: %w", err)
	}
	return nil
}

// FinishSnapshot closes the imported snapshot transaction.
func FinishSnapshot(ctx context.Context, conn pq.Connection) error {
	if err := pq.ExecSQL(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("commit imported snapshot transaction: %w", err)
	}
	return nil
}

// AbortSnapshot rolls back an imported snapshot transaction.
func AbortSnapshot(ctx context.Context, conn pq.Connection) {
	_ = pq.ExecSQL(context.WithoutCancel(ctx), conn, "ROLLBACK")
}

// Scan reads every compiled relation sequentially through one server-side
// cursor. Row order is intentionally not part of correctness.
func (p *Plan) Scan(ctx context.Context, conn pq.Connection, batchSize int, handle func(Batch) error) error {
	if !p.scanAllowed {
		return fmt.Errorf("capture plan is not valid for an unfiltered snapshot scan")
	}
	if batchSize < 1 {
		return fmt.Errorf("capture scan batch size must be positive")
	}
	for i, table := range p.tables {
		cursor := "go_pq_cdc_capture_" + strconv.Itoa(i)
		columns := make([]string, len(table.Columns))
		for j, column := range table.Columns {
			columns[j] = pq.QuoteIdentifier(column)
		}
		from := "ONLY " + pq.QuoteQualifiedName(table.Schema, table.Name)
		if table.Partitioned {
			from = pq.QuoteQualifiedName(table.Schema, table.Name)
		}
		query := fmt.Sprintf(
			"DECLARE %s NO SCROLL CURSOR FOR SELECT %s FROM %s",
			pq.QuoteIdentifier(cursor),
			joinColumns(columns),
			from,
		)
		if err := pq.ExecSQL(ctx, conn, query); err != nil {
			return fmt.Errorf("declare capture cursor for %s.%s: %w", table.Schema, table.Name, err)
		}

		for {
			results, err := pq.ExecQuery(ctx, conn, fmt.Sprintf("FETCH FORWARD %d FROM %s", batchSize, pq.QuoteIdentifier(cursor)))
			if err != nil {
				return fmt.Errorf("fetch capture rows for %s.%s: %w", table.Schema, table.Name, err)
			}
			if len(results) != 1 {
				return fmt.Errorf("capture fetch for %s.%s returned an unexpected result", table.Schema, table.Name)
			}
			result := results[0]
			rows := make([]map[string]any, 0, len(result.Rows))
			for _, row := range result.Rows {
				decoded, err := decodeScanRow(result.FieldDescriptions, row)
				if err != nil {
					return fmt.Errorf("decode capture row for %s.%s: %w", table.Schema, table.Name, err)
				}
				rows = append(rows, decoded)
			}
			if len(rows) > 0 {
				if err := handle(Batch{Schema: table.Schema, Table: table.Name, Rows: rows}); err != nil {
					return fmt.Errorf("handle capture batch for %s.%s: %w", table.Schema, table.Name, err)
				}
			}
			if len(result.Rows) < batchSize {
				break
			}
		}
		if err := pq.ExecSQL(ctx, conn, "CLOSE "+pq.QuoteIdentifier(cursor)); err != nil {
			return fmt.Errorf("close capture cursor for %s.%s: %w", table.Schema, table.Name, err)
		}
	}
	return nil
}

func joinColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	result := columns[0]
	for _, column := range columns[1:] {
		result += ", " + column
	}
	return result
}

func decodeScanRow(fields []pgconn.FieldDescription, row [][]byte) (map[string]any, error) {
	if len(fields) != len(row) {
		return nil, fmt.Errorf("row has %d values for %d fields", len(row), len(fields))
	}
	decoded := make(map[string]any, len(fields))
	for i, field := range fields {
		if row[i] == nil {
			decoded[field.Name] = nil
			continue
		}
		decoded[field.Name] = string(row[i])
	}
	return decoded, nil
}
