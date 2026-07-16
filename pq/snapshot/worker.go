package snapshot

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

const (
	snapshotTransactionBeginSQL = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
	snapshotRollbackTimeout     = 5 * time.Second
)

// waitForCoordinator waits for the coordinator to initialize job and create chunks
// Workers need both job metadata and chunks to be ready before they can start processing
func (s *Snapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	timeout := 5 * time.Minute
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for coordinator to initialize")
		}

		// Check if both job and chunks are ready
		yes, status, err := s.isCoordinatorDidItsJob(ctx, slotName)
		switch {
		case err != nil:
			logger.Debug("[worker] waiting for coordinator", "status", status, "error", err)
		case yes:
			logger.Debug("[worker] coordinator ready, starting work", "status", status)
			return nil
		default:
			logger.Debug("[worker] waiting for coordinator", "status", status)
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(1 * time.Second):
			// Continue waiting
		}
	}
}

// isCoordinatorDidItsJob checks if both job and chunks are ready for processing
// Returns (ready, status_message, error)
func (s *Snapshotter) isCoordinatorDidItsJob(ctx context.Context, slotName string) (bool, string, error) {
	// Step 1: Check if job exists
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return false, "job not found", err
	}
	if job == nil {
		return false, "job not created yet", nil
	}

	// Step 2: Check if snapshot ID is set (coordinator has exported snapshot)
	if job.SnapshotID == "" || job.SnapshotID == "PENDING" {
		return false, "snapshot not exported yet", nil
	}

	// Step 3: Check if chunks are available
	hasChunks, err := s.hasChunksReady(ctx, slotName)
	if err != nil {
		return false, "error checking chunks", err
	}
	if !hasChunks {
		return false, "chunks not created yet", nil
	}

	// Everything is ready!
	return true, fmt.Sprintf("ready (job=%s, chunks=available)", job.SnapshotID), nil
}

// hasChunksReady checks if there are chunks available for processing
func (s *Snapshotter) hasChunksReady(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) > 0
		FROM %s
		WHERE slot_name = %s
	`, chunksTableName, pq.QuoteLiteral(slotName))

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "check chunks ready")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	// Parse boolean result
	hasChunks := string(results[0].Rows[0][0]) == "t"
	return hasChunks, nil
}

// executeWorker sets up metrics, transactions, and processes chunks
func (s *Snapshotter) executeWorker(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, startTime time.Time) error {
	// Set metrics
	s.metric.SetSnapshotInProgress(true)
	s.metric.SetSnapshotTotalTables(len(s.tables))
	s.metric.SetSnapshotTotalChunks(job.TotalChunks)
	defer func() {
		s.metric.SetSnapshotInProgress(false)
		s.metric.SetSnapshotDurationSeconds(time.Since(startTime).Seconds())
	}()

	if err := s.emitSnapshotMarker(ctx, slotName, format.SnapshotEventTypeBegin, job.SnapshotLSN, handler); err != nil {
		return errors.Wrap(err, "emit snapshot begin")
	}

	// Process chunks (each chunk will have its own transaction)
	if err := s.workerProcess(ctx, slotName, instanceID, job, handler); err != nil {
		return fmt.Errorf("worker process: %w", err)
	}

	return nil
}

// workerProcess processes chunks as a worker
func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) error {
	for {
		processed, err := s.processNextChunk(ctx, slotName, instanceID, job, handler)
		if err != nil {
			return err
		}
		if processed {
			continue
		}

		completed, err := s.checkJobCompleted(ctx, slotName)
		if err != nil {
			return errors.Wrap(err, "check snapshot completion")
		}
		if completed {
			logger.Debug("[worker] snapshot chunks completed", "instanceID", instanceID)
			return nil
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(time.Second):
		}
	}
}

// processNextChunk claims and processes a single chunk
// Returns (hasMore, error) where hasMore indicates if there are more chunks to process
func (s *Snapshotter) processNextChunk(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) (bool, error) {
	// Check context cancellation
	if ctx.Err() != nil {
		return false, context.Cause(ctx)
	}

	// Claim next chunk
	chunk, err := s.claimNextChunk(ctx, slotName, instanceID, s.config.ClaimTimeout)
	if err != nil {
		return false, errors.Wrap(err, "claim next chunk")
	}
	if chunk == nil {
		return false, nil // No more chunks available
	}

	s.logChunkStart(instanceID, chunk)
	return s.executeChunkProcessing(ctx, slotName, instanceID, job, handler, chunk)
}

// executeChunkProcessing processes a chunk and handles errors appropriately
func (s *Snapshotter) executeChunkProcessing(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunk *Chunk) (bool, error) {
	chunkCtx, cancelChunk := context.WithCancelCause(ctx)
	defer cancelChunk(context.Canceled)

	// Fence stale claimants before emitting any rows. Later renewals keep the
	// same claimed_by ownership check active for the duration of the chunk.
	renewCtx, cancelRenew := context.WithTimeout(chunkCtx, s.config.HeartbeatInterval)
	err := s.updateChunkHeartbeat(renewCtx, chunk.ID, instanceID)
	cancelRenew()
	if err != nil {
		return false, fmt.Errorf("verify chunk claim: %w", err)
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
		return false, errors.Wrap(heartbeatErr, "maintain chunk claim")
	}
	if err != nil {
		err = errors.Wrap(err, "process chunk")
		if isInvalidSnapshotError(err) {
			return s.handleInvalidSnapshot(ctx, instanceID, chunk, job.SnapshotID)
		}
		return false, err
	}
	if err := s.completeChunk(chunkCtx, slotName, instanceID, chunk, rowsProcessed); err != nil {
		return false, errors.Wrap(err, "complete chunk")
	}
	return true, nil // More chunks may be available
}

// handleInvalidSnapshot handles the case when snapshot becomes invalid
// Returns (false, error) to stop worker loop and trigger retry
func (s *Snapshotter) handleInvalidSnapshot(ctx context.Context, instanceID string, chunk *Chunk, snapshotID string) (bool, error) {
	logger.Warn("[worker] invalid snapshot detected, coordinator likely restarted",
		"chunkID", chunk.ID,
		"snapshotID", snapshotID,
		"instanceID", instanceID)

	// Release chunk back to pending so it can be reprocessed
	if err := s.releaseChunk(ctx, chunk.ID, instanceID); err != nil {
		logger.Error("[worker] failed to release chunk after invalid snapshot",
			"chunkID", chunk.ID,
			"error", err)
	} else {
		logger.Info("[worker] chunk released back to pending", "chunkID", chunk.ID)
	}

	// Signal worker to stop and trigger retry at connector level
	logger.Info("[worker] stopping worker due to invalid snapshot, will restart", "instanceID", instanceID)
	return false, ErrSnapshotInvalidated
}

// logChunkStart logs the start of chunk processing
func (s *Snapshotter) logChunkStart(instanceID string, chunk *Chunk) {
	args := []any{
		"instanceID", instanceID,
		"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
		"chunkIndex", chunk.ChunkIndex,
		"chunkStart", chunk.ChunkStart,
		"chunkSize", chunk.ChunkSize,
	}

	if hasRange := chunk.hasRangeBounds(); hasRange {
		args = append(args,
			"rangeStart", *chunk.RangeStart,
			"rangeEnd", *chunk.RangeEnd,
		)
	}

	logger.Debug("[worker] processing chunk", args...)
}

// completeChunk marks chunk as completed and updates metrics.
func (s *Snapshotter) completeChunk(ctx context.Context, slotName, instanceID string, chunk *Chunk, rowsProcessed int64) error {
	if err := s.markChunkCompleted(ctx, slotName, instanceID, chunk.ID, rowsProcessed); err != nil {
		return errors.Wrap(err, "mark chunk completed")
	}

	// Update metrics
	s.metric.SnapshotRowsIncrement(rowsProcessed)
	s.updateCompletedChunksMetric(ctx, slotName)

	// Log completion
	logger.Debug("[worker] chunk completed",
		"instanceID", instanceID,
		"chunkID", chunk.ID,
		"rowsProcessed", rowsProcessed)
	return nil
}

// updateCompletedChunksMetric updates the completed chunks metric
func (s *Snapshotter) updateCompletedChunksMetric(ctx context.Context, slotName string) {
	if job, _ := s.loadJob(ctx, slotName); job != nil {
		s.metric.SetSnapshotCompletedChunks(job.CompletedChunks)
	}
}

// executeInTransaction executes a function within a read-only exported-snapshot transaction.
func (s *Snapshotter) executeInTransaction(ctx context.Context, snapshotID string, fn func(pq.Connection) (int64, error)) (int64, error) {
	if err := s.execSQL(ctx, s.workerConn, snapshotTransactionBeginSQL); err != nil {
		return 0, errors.Wrap(err, "begin transaction")
	}
	defer s.rollbackWorkerConnection()

	if err := s.setTransactionSnapshot(ctx, s.workerConn, snapshotID); err != nil {
		return 0, errors.Wrap(err, "set transaction snapshot")
	}
	rows, err := fn(s.workerConn)
	if err != nil {
		return 0, errors.Wrap(err, "execute function")
	}
	return rows, nil
}

func (s *Snapshotter) rollbackWorkerConnection() {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotRollbackTimeout)
	defer cancel()
	if err := s.execSQL(ctx, s.workerConn, "ROLLBACK"); err != nil {
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

func (s *Snapshotter) emitSnapshotMarker(ctx context.Context, slotName string, eventType format.SnapshotEventType, lsn pq.LSN, handler Handler) error {
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

	if err := s.execSQL(ctx, s.workerConn, "BEGIN"); err != nil {
		return errors.Wrap(err, "begin marker transaction")
	}
	committed := false
	defer func() {
		if !committed {
			s.rollbackWorkerConnection()
		}
	}()

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE slot_name = %s FOR UPDATE",
		column,
		jobTableName,
		pq.QuoteLiteral(slotName),
	)
	results, err := s.execQuery(ctx, s.workerConn, query)
	if err != nil {
		return errors.Wrap(err, "lock snapshot job")
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return errors.New("snapshot job not found")
	}
	if value := string(results[0].Rows[0][0]); value == "t" || value == "true" {
		return nil
	}

	if err := handler(&format.Snapshot{
		EventType:  eventType,
		ServerTime: time.Now().UTC(),
		LSN:        lsn,
	}); err != nil {
		return errors.Wrap(err, "handle snapshot marker")
	}

	query = fmt.Sprintf(
		"UPDATE %s SET %s WHERE slot_name = %s",
		jobTableName,
		update,
		pq.QuoteLiteral(slotName),
	)
	if err := s.execSQL(ctx, s.workerConn, query); err != nil {
		return errors.Wrap(err, "persist snapshot marker")
	}
	if err := s.execSQL(ctx, s.workerConn, "COMMIT"); err != nil {
		return errors.Wrap(err, "commit marker transaction")
	}
	committed = true
	return nil
}

// claimNextChunk attempts to claim a pending chunk using SELECT FOR UPDATE SKIP LOCKED
func (s *Snapshotter) claimNextChunk(ctx context.Context, slotName, instanceID string, claimTimeout time.Duration) (*Chunk, error) {
	now := time.Now().UTC()
	results, err := s.execQuery(ctx, s.metadataConn, s.buildClaimChunkQuery(slotName, instanceID, now, claimTimeout))
	if err != nil {
		return nil, errors.Wrap(err, "claim chunk")
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}
	return s.parseClaimedChunk(results[0].Rows[0], slotName, instanceID, now)
}

// buildClaimChunkQuery builds the SQL query for claiming a chunk
func (s *Snapshotter) buildClaimChunkQuery(slotName, instanceID string, now time.Time, claimTimeout time.Duration) string {
	timeoutThreshold := now.Add(-claimTimeout)
	return fmt.Sprintf(`
		WITH available_chunk AS (
			SELECT id FROM %s
			WHERE slot_name = %s
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
		          c.is_last_chunk, c.partition_strategy, c.rows_processed
	`, chunksTableName,
		pq.QuoteLiteral(slotName),
		pq.QuoteLiteral(timeoutThreshold.Format(postgresTimestampFormat)),
		chunksTableName,
		pq.QuoteLiteral(instanceID),
		pq.QuoteLiteral(now.Format(postgresTimestampFormat)),
		pq.QuoteLiteral(now.Format(postgresTimestampFormat)),
	)
}

// parseClaimedChunk parses the chunk row data
func (s *Snapshotter) parseClaimedChunk(row [][]byte, slotName, instanceID string, now time.Time) (*Chunk, error) {
	if len(row) < 13 {
		return nil, errors.New("invalid chunk row: expected 13 columns")
	}

	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}

	// Parse each field with validation
	if _, err := fmt.Sscanf(string(row[0]), "%d", &chunk.ID); err != nil {
		return nil, errors.Wrap(err, "parse chunk ID")
	}
	chunk.TableSchema = string(row[1])
	chunk.TableName = string(row[2])
	if _, err := fmt.Sscanf(string(row[3]), "%d", &chunk.ChunkIndex); err != nil {
		return nil, errors.Wrap(err, "parse chunk index")
	}
	if _, err := fmt.Sscanf(string(row[4]), "%d", &chunk.ChunkStart); err != nil {
		return nil, errors.Wrap(err, "parse chunk start")
	}
	if _, err := fmt.Sscanf(string(row[5]), "%d", &chunk.ChunkSize); err != nil {
		return nil, errors.Wrap(err, "parse chunk size")
	}

	// Parse nullable int64 fields for range
	rangeStart, err := parseNullableInt64(row[6])
	if err != nil {
		return nil, errors.Wrap(err, "parse range start")
	}
	rangeEnd, err := parseNullableInt64(row[7])
	if err != nil {
		return nil, errors.Wrap(err, "parse range end")
	}
	chunk.RangeStart = rangeStart
	chunk.RangeEnd = rangeEnd

	// Parse CTID block fields
	blockStart, err := parseNullableInt64(row[8])
	if err != nil {
		return nil, errors.Wrap(err, "parse block start")
	}
	blockEnd, err := parseNullableInt64(row[9])
	if err != nil {
		return nil, errors.Wrap(err, "parse block end")
	}
	chunk.BlockStart = blockStart
	chunk.BlockEnd = blockEnd

	// Parse is_last_chunk (boolean)
	if row[10] != nil && len(row[10]) > 0 {
		chunk.IsLastChunk = string(row[10]) == "t" || string(row[10]) == "true"
	}

	// Parse partition strategy
	if row[11] != nil && len(row[11]) > 0 {
		chunk.PartitionStrategy = PartitionStrategy(string(row[11]))
	} else {
		chunk.PartitionStrategy = PartitionStrategyOffset
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
		pq.QuoteLiteral(time.Now().UTC().Format(postgresTimestampFormat)),
		chunkID,
		pq.QuoteLiteral(instanceID),
	)

	results, err := s.execQuery(ctx, s.healthcheckConn, query)
	if err != nil {
		return err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return fmt.Errorf("snapshot chunk %d claim lost", chunkID)
	}
	return nil
}

// markChunkCompleted marks a chunk as completed and atomically increments completed_chunks
// NOTE: Uses metadataConn (not workerConn) to avoid serialization conflicts
// workerConn is in REPEATABLE READ snapshot transaction, metadata updates should be separate
func (s *Snapshotter) markChunkCompleted(ctx context.Context, slotName, instanceID string, chunkID, rowsProcessed int64) error {
	query := fmt.Sprintf(`
		WITH completed AS (
			UPDATE %s c
			SET status = 'completed',
			    completed_at = %s,
			    rows_processed = %d
			FROM %s j
			WHERE c.id = %d
			  AND c.slot_name = %s
			  AND c.status = 'in_progress'
			  AND c.claimed_by = %s
			  AND j.slot_name = c.slot_name
			RETURNING 1
		), progress AS (
			UPDATE %s j
			SET completed_chunks = completed_chunks + 1
			FROM completed
			WHERE j.slot_name = %s
			RETURNING 1
		)
		SELECT COUNT(*) FROM progress
	`,
		chunksTableName,
		pq.QuoteLiteral(time.Now().UTC().Format(postgresTimestampFormat)),
		rowsProcessed,
		jobTableName,
		chunkID,
		pq.QuoteLiteral(slotName),
		pq.QuoteLiteral(instanceID),
		jobTableName,
		pq.QuoteLiteral(slotName),
	)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return errors.New("complete chunk returned no result")
	}
	completed, err := strconv.ParseInt(string(results[0].Rows[0][0]), 10, 64)
	if err != nil {
		return errors.Wrap(err, "parse completed chunk count")
	}
	if completed != 1 {
		return errors.New("snapshot chunk claim or job lost before completion")
	}
	return nil
}

// releaseChunk releases a claimed chunk back to pending status
// This allows other workers to reclaim and process the chunk
func (s *Snapshotter) releaseChunk(ctx context.Context, chunkID int64, instanceID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET status = 'pending',
			    claimed_by = NULL,
			    claimed_at = NULL,
			    heartbeat_at = NULL
			WHERE id = %d AND status = 'in_progress' AND claimed_by = %s
		`, chunksTableName, chunkID, pq.QuoteLiteral(instanceID))

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "release chunk")
		}

		return nil
	})
}
