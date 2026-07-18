package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

const snapshotTransactionBeginSQL = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"

// workerProcess claims and processes chunks until the job is complete.
func (s *Snapshotter) workerProcess(ctx context.Context, instanceID string, job *Job, handler Handler) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		chunk, err := s.claimNextChunk(ctx, job, instanceID, s.config.ClaimTimeout)
		if err != nil {
			return fmt.Errorf("claim next chunk: %w", err)
		}
		if chunk != nil {
			logArgs := []any{
				"instanceID", instanceID,
				"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
				"chunkIndex", chunk.ChunkIndex,
				"chunkStart", chunk.ChunkStart,
				"chunkSize", chunk.ChunkSize,
			}
			if chunk.hasRangeBounds() {
				logArgs = append(logArgs, "rangeStart", *chunk.RangeStart, "rangeEnd", *chunk.RangeEnd)
			}
			logger.Debug("[worker] processing chunk", logArgs...)
			if err := s.executeChunkProcessing(ctx, instanceID, job, handler, chunk); err != nil {
				return err
			}
			continue
		}

		completed, err := s.checkJobCompleted(ctx, job)
		if err != nil {
			return fmt.Errorf("check snapshot completion: %w", err)
		}
		if completed {
			logger.Debug("[worker] snapshot chunks completed", "instanceID", instanceID)
			return nil
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (s *Snapshotter) executeChunkProcessing(ctx context.Context, instanceID string, job *Job, handler Handler, chunk *Chunk) error {
	chunkCtx, cancelChunk := context.WithCancelCause(ctx)
	defer cancelChunk(context.Canceled)

	// Fence stale claimants before emitting any rows. Later renewals keep the
	// same claimed_by ownership check active for the duration of the chunk.
	renewCtx, cancelRenew := context.WithTimeout(chunkCtx, s.config.HeartbeatInterval)
	err := s.updateChunkHeartbeat(renewCtx, chunk.ID, instanceID)
	cancelRenew()
	if err != nil {
		return fmt.Errorf("verify chunk claim: %w", err)
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(chunkCtx)
	heartbeatDone := make(chan struct{})
	go s.heartbeatWorker(heartbeatCtx, cancelChunk, heartbeatDone, chunk.ID, instanceID, s.config.HeartbeatInterval)

	rowsProcessed, err := s.executeInTransaction(chunkCtx, job.SnapshotID, func(conn pq.Connection) (int64, error) {
		return s.processChunk(chunkCtx, conn, chunk, job.SnapshotLSN, handler)
	})
	stopHeartbeat()
	<-heartbeatDone

	if heartbeatErr := context.Cause(chunkCtx); heartbeatErr != nil && context.Cause(ctx) == nil {
		return fmt.Errorf("maintain chunk claim: %w", heartbeatErr)
	}
	if err != nil {
		if errors.Is(err, ErrSnapshotInvalidated) {
			logger.Warn("[worker] exported snapshot invalidated", "snapshotID", job.SnapshotID, "chunkID", chunk.ID)
			return ErrSnapshotInvalidated
		}
		return fmt.Errorf("process chunk: %w", err)
	}
	completedChunks, err := s.markChunkCompleted(chunkCtx, job, instanceID, chunk.ID, rowsProcessed)
	if err != nil {
		return fmt.Errorf("complete chunk: %w", err)
	}
	s.metric.SnapshotRowsIncrement(rowsProcessed)
	s.metric.SetSnapshotCompletedChunks(completedChunks)
	logger.Debug("[worker] chunk completed", "instanceID", instanceID, "chunkID", chunk.ID, "rowsProcessed", rowsProcessed)
	return nil
}

// executeInTransaction executes a function within a read-only exported-snapshot transaction.
func (s *Snapshotter) executeInTransaction(ctx context.Context, snapshotID string, fn func(pq.Connection) (int64, error)) (int64, error) {
	if err := pq.ExecSQL(ctx, s.workerConn, snapshotTransactionBeginSQL); err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer s.rollbackWorkerConnection()

	if err := s.setTransactionSnapshot(ctx, s.workerConn, snapshotID); err != nil {
		return 0, err
	}
	return fn(s.workerConn)
}

func (s *Snapshotter) rollbackWorkerConnection() {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotCleanupTimeout)
	defer cancel()
	if err := pq.ExecSQL(ctx, s.workerConn, "ROLLBACK"); err != nil {
		_ = s.workerConn.Close(ctx)
	}
}

// heartbeatWorker keeps one claimed chunk leased and cancels its work if renewal fails.
func (s *Snapshotter) heartbeatWorker(ctx context.Context, cancelChunk context.CancelCauseFunc, done chan<- struct{}, chunkID int64, instanceID string, interval time.Duration) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, interval)
			err := s.updateChunkHeartbeat(renewCtx, chunkID, instanceID)
			cancel()
			if err != nil {
				cancelChunk(err)
				return
			}
			logger.Debug("[heartbeat] updated", "chunkID", chunkID)
		}
	}
}

func (s *Snapshotter) emitSnapshotMarker(ctx context.Context, job *Job, eventType format.SnapshotEventType, handler Handler) error {
	var column, update string
	switch eventType {
	case format.SnapshotEventTypeBegin:
		column = "begin_emitted"
		update = "begin_emitted = true"
	case format.SnapshotEventTypeEnd:
		column = "end_emitted"
		update = "end_emitted = true, completed = true"
	default:
		return fmt.Errorf("unsupported snapshot marker %q", eventType)
	}

	return withTransaction(ctx, s.workerConn, func(conn pq.Connection) error {
		query := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s FOR UPDATE",
			column,
			jobTableName,
			job.sqlIdentity(""),
		)
		results, err := pq.ExecQuery(ctx, conn, query)
		if err != nil {
			return fmt.Errorf("lock snapshot job: %w", err)
		}
		value, err := singleValue(results, "snapshot marker state")
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSnapshotInvalidated, err)
		}
		emitted, err := parsePostgresBool(value)
		if err != nil {
			return fmt.Errorf("parse snapshot marker state: %w", err)
		}
		if emitted {
			return nil
		}

		if err := handler(&format.Snapshot{
			EventType:  eventType,
			ServerTime: time.Now().UTC(),
			LSN:        job.SnapshotLSN,
		}); err != nil {
			return fmt.Errorf("handle snapshot marker: %w", err)
		}

		query = fmt.Sprintf(
			"UPDATE %s SET %s WHERE %s",
			jobTableName,
			update,
			job.sqlIdentity(""),
		)
		if err := pq.ExecSQL(ctx, conn, query); err != nil {
			return fmt.Errorf("persist snapshot marker: %w", err)
		}
		return nil
	})
}

// claimNextChunk attempts to claim a pending chunk using SELECT FOR UPDATE SKIP LOCKED
func (s *Snapshotter) claimNextChunk(ctx context.Context, job *Job, instanceID string, claimTimeout time.Duration) (*Chunk, error) {
	now := time.Now().UTC()
	results, err := pq.ExecQuery(ctx, s.metadataConn, s.buildClaimChunkQuery(job, instanceID, now, claimTimeout))
	if err != nil {
		return nil, fmt.Errorf("claim chunk: %w", err)
	}
	if len(results) != 1 {
		return nil, errors.New("invalid chunk claim result")
	}
	rows := results[0].Rows
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 {
		return nil, errors.New("chunk claim returned multiple rows")
	}
	return s.parseClaimedChunk(rows[0], job.SlotName, instanceID, now)
}

func (s *Snapshotter) buildClaimChunkQuery(job *Job, instanceID string, now time.Time, claimTimeout time.Duration) string {
	timeoutThreshold := now.Add(-claimTimeout)
	return fmt.Sprintf(`
		WITH available_chunk AS (
			SELECT c.id FROM %s c
			WHERE c.slot_name = %s
			  AND EXISTS (SELECT 1 FROM %s j WHERE %s)
			  AND (
				  status = 'pending'
				  OR (status = 'in_progress' AND COALESCE(heartbeat_at, claimed_at) < %s)
			  )
			ORDER BY chunk_index
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s c
		SET status = 'in_progress',
		    claimed_by = %s,
		    claimed_at = %s,
		    heartbeat_at = %s
		FROM available_chunk
		WHERE c.id = available_chunk.id
		RETURNING c.id, c.table_schema, c.table_name, 
		          c.chunk_index, c.chunk_start, c.chunk_size, 
		          c.range_start, c.range_end, c.block_start, c.block_end,
		          c.is_last_chunk, c.partition_strategy
	`, chunksTableName,
		pq.QuoteLiteral(job.SlotName),
		jobTableName,
		job.sqlIdentity("j"),
		pq.QuoteLiteral(timeoutThreshold.Format(postgresTimestampFormatMicros)),
		chunksTableName,
		pq.QuoteLiteral(instanceID),
		pq.QuoteLiteral(now.Format(postgresTimestampFormatMicros)),
		pq.QuoteLiteral(now.Format(postgresTimestampFormatMicros)),
	)
}

func (s *Snapshotter) parseClaimedChunk(row [][]byte, slotName, instanceID string, now time.Time) (*Chunk, error) {
	if len(row) != 12 {
		return nil, errors.New("invalid chunk row: expected 12 columns")
	}

	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}
	var err error

	chunk.ID, err = strconv.ParseInt(string(row[0]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse chunk ID: %w", err)
	}
	if chunk.ID <= 0 {
		return nil, errors.New("chunk ID must be positive")
	}
	chunk.TableSchema = normalizeSchema(string(row[1]))
	chunk.TableName = string(row[2])
	if chunk.TableName == "" {
		return nil, errors.New("chunk table name is required")
	}
	chunk.ChunkIndex, err = strconv.Atoi(string(row[3]))
	if err != nil {
		return nil, fmt.Errorf("parse chunk index: %w", err)
	}
	chunk.ChunkStart, err = strconv.ParseInt(string(row[4]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse chunk start: %w", err)
	}
	chunk.ChunkSize, err = strconv.ParseInt(string(row[5]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse chunk size: %w", err)
	}

	chunk.RangeStart, err = parseNullableInt64(row[6])
	if err != nil {
		return nil, fmt.Errorf("parse range start: %w", err)
	}
	chunk.RangeEnd, err = parseNullableInt64(row[7])
	if err != nil {
		return nil, fmt.Errorf("parse range end: %w", err)
	}
	chunk.BlockStart, err = parseNullableInt64(row[8])
	if err != nil {
		return nil, fmt.Errorf("parse block start: %w", err)
	}
	chunk.BlockEnd, err = parseNullableInt64(row[9])
	if err != nil {
		return nil, fmt.Errorf("parse block end: %w", err)
	}

	chunk.IsLastChunk, err = parsePostgresBool(row[10])
	if err != nil {
		return nil, fmt.Errorf("parse is_last_chunk: %w", err)
	}

	if len(row[11]) == 0 {
		return nil, errors.New("missing snapshot partition strategy")
	}
	chunk.PartitionStrategy = PartitionStrategy(string(row[11]))
	if err := chunk.validatePartition(); err != nil {
		return nil, fmt.Errorf("validate claimed chunk: %w", err)
	}

	return chunk, nil
}

// updateChunkHeartbeat renews a chunk claim once; failure invalidates the lease.
func (s *Snapshotter) updateChunkHeartbeat(ctx context.Context, chunkID int64, instanceID string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET heartbeat_at = %s
		WHERE id = %d AND status = 'in_progress' AND claimed_by = %s
		RETURNING 1
	`,
		chunksTableName,
		pq.QuoteLiteral(time.Now().UTC().Format(postgresTimestampFormatMicros)),
		chunkID,
		pq.QuoteLiteral(instanceID),
	)

	results, err := pq.ExecQuery(ctx, s.healthcheckConn, query)
	if err != nil {
		return err
	}
	if _, err := singleValue(results, "chunk heartbeat"); err != nil {
		return fmt.Errorf("snapshot chunk %d claim lost: %w", chunkID, err)
	}
	return nil
}

// markChunkCompleted atomically consumes the claim and advances its job.
func (s *Snapshotter) markChunkCompleted(ctx context.Context, job *Job, instanceID string, chunkID, rowsProcessed int64) (completed int, err error) {
	err = withTransaction(ctx, s.metadataConn, func(conn pq.Connection) error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET status = 'completed',
			    completed_at = %s,
			    rows_processed = %d
			WHERE id = %d
			  AND slot_name = %s
			  AND status = 'in_progress'
			  AND claimed_by = %s
			RETURNING 1
		`,
			chunksTableName,
			pq.QuoteLiteral(time.Now().UTC().Format(postgresTimestampFormatMicros)),
			rowsProcessed,
			chunkID,
			pq.QuoteLiteral(job.SlotName),
			pq.QuoteLiteral(instanceID),
		)
		results, err := pq.ExecQuery(ctx, conn, query)
		if err != nil {
			return fmt.Errorf("complete snapshot chunk: %w", err)
		}
		if _, err := singleValue(results, "snapshot chunk completion"); err != nil {
			return err
		}

		query = fmt.Sprintf(`
			UPDATE %s
			SET completed_chunks = completed_chunks + 1
			WHERE %s
			  AND NOT completed
			  AND completed_chunks < total_chunks
			RETURNING completed_chunks
		`, jobTableName, job.sqlIdentity(""))
		results, err = pq.ExecQuery(ctx, conn, query)
		if err != nil {
			return fmt.Errorf("advance snapshot progress: %w", err)
		}
		value, err := singleValue(results, "snapshot progress")
		if err != nil {
			return fmt.Errorf("snapshot job lost: %w: %v", ErrSnapshotInvalidated, err)
		}
		completed, err = strconv.Atoi(string(value))
		if err != nil {
			return fmt.Errorf("parse snapshot progress: %w", err)
		}
		return nil
	})
	return completed, err
}
