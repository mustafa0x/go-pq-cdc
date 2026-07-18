package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

// exportSnapshot exports the current read-only transaction for worker connections.
func (s *Snapshotter) exportSnapshot(ctx context.Context, conn pq.Connection) (string, error) {
	results, err := pq.ExecQuery(ctx, conn, "SELECT pg_export_snapshot()")
	if err != nil {
		if strings.Contains(err.Error(), "permission denied") {
			return "", errors.New("pg_export_snapshot requires REPLICATION privilege. Run: ALTER USER your_user WITH REPLICATION")
		}
		if strings.Contains(err.Error(), "wal_level") {
			return "", errors.New("pg_export_snapshot requires wal_level='logical'. Set in postgresql.conf and restart")
		}
		return "", fmt.Errorf("export snapshot: %w", err)
	}
	return parseExportedSnapshotID(results)
}

func parseExportedSnapshotID(results []*pgconn.Result) (string, error) {
	value, err := singleValue(results, "exported snapshot ID")
	if err != nil {
		return "", err
	}
	if len(value) == 0 {
		return "", errors.New("empty snapshot ID returned")
	}
	return string(value), nil
}

func (s *Snapshotter) setTransactionSnapshot(ctx context.Context, conn pq.Connection, snapshotID string) error {
	if snapshotID == "" {
		return errors.New("snapshot ID cannot be empty")
	}
	if err := pq.ExecSQL(ctx, conn, "SET TRANSACTION SNAPSHOT "+pq.QuoteLiteral(snapshotID)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "22023" && strings.Contains(strings.ToLower(pgErr.Message), "invalid snapshot identifier") {
			return ErrSnapshotInvalidated
		}
		return fmt.Errorf("set transaction snapshot: %w", err)
	}
	logger.Debug("[worker] transaction snapshot set", "snapshotID", snapshotID)
	return nil
}
