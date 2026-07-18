package snapshot

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

type primaryKeyColumn struct {
	Name     string
	DataType string
}

// initializeCoordinator replaces stale metadata, records a conservative CDC
// checkpoint, exports a snapshot, and atomically publishes its work plan.
func (s *Snapshotter) initializeCoordinator(ctx context.Context, slotName string, existingJob *Job) error {
	logger.Debug("[coordinator] initializing job", "slotName", slotName)

	if existingJob != nil {
		logger.Warn("[coordinator] incomplete job found, restarting from scratch")
	}
	// Capture the checkpoint before acquiring the exported snapshot. Transactions
	// committed between these operations may be replayed after appearing in the
	// snapshot, but none can be absent from both the snapshot and CDC.
	results, err := pq.ExecQuery(ctx, s.metadataConn, "SELECT pg_current_wal_lsn()")
	if err != nil {
		return fmt.Errorf("read snapshot checkpoint: %w", err)
	}
	snapshotLSN, err := parseSnapshotCheckpoint(results)
	if err != nil {
		return err
	}

	exportConn, snapshotID, err := s.exportSnapshotTransaction(ctx)
	if err != nil {
		return fmt.Errorf("export snapshot: %w", err)
	}

	// Create metadata and chunks using the snapshot connection. This guarantees
	// chunk boundaries match the exported snapshot.
	if err := s.createMetadata(ctx, exportConn, slotName, snapshotID, snapshotLSN); err != nil {
		s.closeExportSnapshotConnection()
		return fmt.Errorf("create metadata: %w", err)
	}
	s.startSnapshotKeepalive(ctx, exportConn)

	logger.Debug("[coordinator] initialization complete")
	return nil
}

func parseSnapshotCheckpoint(results []*pgconn.Result) (pq.LSN, error) {
	value, err := singleValue(results, "snapshot checkpoint")
	if err != nil {
		return 0, err
	}
	lsn, err := pq.ParseLSN(string(value))
	if err != nil {
		return 0, fmt.Errorf("parse snapshot checkpoint: %w", err)
	}
	if lsn == 0 {
		return 0, errors.New("snapshot checkpoint must not be 0/0")
	}
	return lsn, nil
}

func (s *Snapshotter) createMetadata(ctx context.Context, exportConn pq.Connection, slotName, snapshotID string, snapshotLSN pq.LSN) error {
	logger.Info("[coordinator] creating metadata and chunks")

	job := &Job{
		SlotName:    slotName,
		SnapshotID:  snapshotID,
		SnapshotLSN: snapshotLSN,
		StartedAt:   time.Now().UTC(),
	}
	if s.config.Resnapshot {
		job.ResnapshotID = s.config.ResnapshotID
	}

	var chunks []*Chunk
	for _, table := range s.tables {
		tableChunks, err := s.createTableChunks(ctx, exportConn, slotName, table)
		if err != nil {
			return fmt.Errorf("plan chunks for %s.%s: %w", table.Schema, table.Name, err)
		}
		if len(tableChunks) == 0 {
			return fmt.Errorf("snapshot planner returned no chunks for %s.%s", table.Schema, table.Name)
		}
		chunks = append(chunks, tableChunks...)
		logger.Info("[coordinator] chunks created",
			"table", fmt.Sprintf("%s.%s", table.Schema, table.Name),
			"chunks", len(tableChunks))
	}

	if err := s.persistMetadata(ctx, job, chunks); err != nil {
		return err
	}

	logger.Info("[coordinator] metadata committed", "totalChunks", len(chunks), "lsn", snapshotLSN.String())
	return nil
}

func (s *Snapshotter) persistMetadata(ctx context.Context, job *Job, chunks []*Chunk) error {
	job.TotalChunks = len(chunks)
	if err := job.validate(); err != nil {
		return fmt.Errorf("validate snapshot job: %w", err)
	}
	return withTransaction(ctx, s.metadataConn, func(conn pq.Connection) error {
		if err := s.deleteMetadata(ctx, conn, job.SlotName); err != nil {
			return fmt.Errorf("replace previous metadata: %w", err)
		}
		if err := s.saveChunksBatch(ctx, conn, chunks); err != nil {
			return fmt.Errorf("save chunks: %w", err)
		}
		if err := s.saveJob(ctx, conn, job); err != nil {
			return fmt.Errorf("save job: %w", err)
		}
		if job.ResnapshotID != "" {
			query := fmt.Sprintf(
				"INSERT INTO %s (slot_name, resnapshot_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
				requestTableName,
				pq.QuoteLiteral(job.SlotName),
				pq.QuoteLiteral(job.ResnapshotID),
			)
			if err := pq.ExecSQL(ctx, conn, query); err != nil {
				return fmt.Errorf("save resnapshot request: %w", err)
			}
		}
		return nil
	})
}

// exportSnapshotTransaction begins a REPEATABLE READ transaction and exports snapshot ID
// This transaction is kept OPEN for workers to use the same snapshot
func (s *Snapshotter) exportSnapshotTransaction(ctx context.Context) (_ pq.Connection, _ string, err error) {
	s.closeExportSnapshotConnection()

	exportSnapshotConn, err := pq.NewConnection(ctx, s.dsn)
	if err != nil {
		return nil, "", fmt.Errorf("create pg export snapshot connection: %w", err)
	}

	s.keepaliveMu.Lock()
	s.exportSnapshotConn = exportSnapshotConn
	s.keepaliveMu.Unlock()
	defer func() {
		if err != nil {
			s.closeExportSnapshotConnection()
		}
	}()

	logger.Info("[coordinator] exporting snapshot")

	// Disable timeouts for snapshot transaction
	if err := pq.ExecSQL(ctx, exportSnapshotConn, "SET idle_in_transaction_session_timeout = 0"); err != nil {
		return nil, "", fmt.Errorf("set idle timeout: %w", err)
	}

	if err := pq.ExecSQL(ctx, exportSnapshotConn, "SET statement_timeout = 0"); err != nil {
		return nil, "", fmt.Errorf("set statement timeout: %w", err)
	}

	// Start transaction on snapshot connection (will stay open)
	if err := pq.ExecSQL(ctx, exportSnapshotConn, snapshotTransactionBeginSQL); err != nil {
		return nil, "", fmt.Errorf("begin snapshot transaction: %w", err)
	}

	snapshotID, err := s.exportSnapshot(ctx, exportSnapshotConn)
	if err != nil {
		return nil, "", fmt.Errorf("export snapshot: %w", err)
	}

	logger.Info("[coordinator] snapshot exported", "snapshotID", snapshotID)
	return exportSnapshotConn, snapshotID, nil
}

func (s *Snapshotter) snapshotTransactionKeepalive(ctx context.Context, conn pq.Connection, done chan<- struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = pq.ExecSQL(pingCtx, conn, "SELECT 1")
			cancel()
		}
	}
}

// ReconcileResnapshot elects one owner for a resnapshot request and reports
// whether this instance should execute its generation.
func (s *Snapshotter) ReconcileResnapshot(ctx context.Context, slotName string) (bool, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		acquired, err := s.tryAcquireCoordinatorLock(ctx, slotName)
		if err != nil {
			return false, fmt.Errorf("acquire coordinator lock for cleanup: %w", err)
		}
		if acquired {
			job, err := s.loadJob(ctx, slotName)
			if err != nil {
				return false, fmt.Errorf("load snapshot job during cleanup: %w", err)
			}
			if job != nil && job.ResnapshotID == s.config.ResnapshotID {
				if err := s.releaseCoordinatorLock(ctx, slotName); err != nil {
					return false, err
				}
				return !job.Completed, nil
			}
			consumed, err := s.resnapshotRequestExists(ctx, slotName, s.config.ResnapshotID)
			if err != nil {
				return false, err
			}
			if consumed {
				if err := s.releaseCoordinatorLock(ctx, slotName); err != nil {
					return false, err
				}
				return false, nil
			}
			if err := s.cleanupJob(ctx, slotName); err != nil {
				return false, err
			}
			return true, nil
		}

		job, err := s.loadJob(ctx, slotName)
		if err != nil {
			return false, fmt.Errorf("load snapshot job while waiting for cleanup: %w", err)
		}
		if job != nil && job.ResnapshotID == s.config.ResnapshotID {
			return !job.Completed, nil
		}
		consumed, err := s.resnapshotRequestExists(ctx, slotName, s.config.ResnapshotID)
		if err != nil {
			return false, err
		}
		if consumed {
			return false, nil
		}

		select {
		case <-ctx.Done():
			return false, context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (s *Snapshotter) resnapshotRequestExists(ctx context.Context, slotName, resnapshotID string) (bool, error) {
	query := fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM %s WHERE slot_name = %s AND resnapshot_id = %s)",
		requestTableName,
		pq.QuoteLiteral(slotName),
		pq.QuoteLiteral(resnapshotID),
	)
	exists, err := pq.ExecExistsQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("check resnapshot request: %w", err)
	}
	return exists, nil
}

func (s *Snapshotter) cleanupJob(ctx context.Context, slotName string) error {
	if err := withTransaction(ctx, s.metadataConn, func(conn pq.Connection) error {
		return s.deleteMetadata(ctx, conn, slotName)
	}); err != nil {
		return fmt.Errorf("delete snapshot metadata: %w", err)
	}
	logger.Info("[metadata] job cleaned up", "slotName", slotName)
	return nil
}

func (s *Snapshotter) deleteMetadata(ctx context.Context, conn pq.Connection, slotName string) error {
	quotedSlot := pq.QuoteLiteral(slotName)
	if err := pq.ExecSQL(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE slot_name = %s", chunksTableName, quotedSlot)); err != nil {
		return err
	}
	return pq.ExecSQL(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE slot_name = %s", jobTableName, quotedSlot))
}

// setupJob elects a coordinator or joins the atomically published work plan.
func (s *Snapshotter) setupJob(ctx context.Context, slotName, instanceID string) error {
	if err := s.initTables(ctx); err != nil {
		return fmt.Errorf("initialize tables: %w", err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var invalidSnapshotID string
	for {
		job, err := s.loadJob(ctx, slotName)
		if err != nil {
			return fmt.Errorf("load snapshot job: %w", err)
		}
		if job != nil {
			if err := s.validateJobRequest(job); err != nil {
				return err
			}
		}
		if job != nil && job.Completed {
			return nil
		}

		lockAcquired, err := s.tryAcquireCoordinatorLock(ctx, slotName)
		if err != nil {
			return fmt.Errorf("acquire coordinator lock: %w", err)
		}
		if lockAcquired {
			// The job can complete between the optimistic read above and lock
			// acquisition. Re-read under coordinator ownership before replacing it.
			job, err = s.loadJob(ctx, slotName)
			if err != nil {
				return fmt.Errorf("reload snapshot job: %w", err)
			}
			if job != nil {
				if err := s.validateJobRequest(job); err != nil {
					return err
				}
			}
			if job != nil && job.Completed {
				return nil
			}
			logger.Debug("[snapshot] elected as coordinator", "instanceID", instanceID)
			if err := s.initializeCoordinator(ctx, slotName, job); err != nil {
				return fmt.Errorf("initialize coordinator: %w", err)
			}
			return nil
		}
		// The lock holder may be replacing stale metadata. Join only after
		// PostgreSQL proves the published snapshot is still importable.
		if job != nil && job.SnapshotID != invalidSnapshotID {
			_, err := s.executeInTransaction(ctx, job.SnapshotID, func(pq.Connection) (int64, error) {
				return 0, nil
			})
			if err == nil {
				logger.Debug("[snapshot] joining as worker", "instanceID", instanceID, "snapshotID", job.SnapshotID)
				return nil
			}
			if !errors.Is(err, ErrSnapshotInvalidated) {
				return fmt.Errorf("validate snapshot generation: %w", err)
			}
			invalidSnapshotID = job.SnapshotID
			logger.Debug("[snapshot] waiting for replacement generation", "instanceID", instanceID, "snapshotID", job.SnapshotID)
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (s *Snapshotter) initTables(ctx context.Context) error {
	err := withTransaction(ctx, s.metadataConn, func(conn pq.Connection) error {
		if err := pq.ExecSQL(ctx, conn, "SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext('go-pq-cdc:snapshot-schema'))"); err != nil {
			return fmt.Errorf("lock snapshot metadata schema: %w", err)
		}

		tables := []struct {
			name string
			sql  string
		}{
			{jobTableName, fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					slot_name TEXT PRIMARY KEY,
					snapshot_id TEXT NOT NULL,
					resnapshot_id TEXT NOT NULL DEFAULT '',
					snapshot_lsn TEXT NOT NULL,
					started_at TIMESTAMP NOT NULL,
					completed BOOLEAN DEFAULT FALSE,
					begin_emitted BOOLEAN DEFAULT FALSE,
					end_emitted BOOLEAN DEFAULT FALSE,
					total_chunks INT NOT NULL DEFAULT 0,
					completed_chunks INT NOT NULL DEFAULT 0
				)
			`, jobTableName)},
			{chunksTableName, fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					id SERIAL PRIMARY KEY,
					slot_name TEXT NOT NULL,
					table_schema TEXT NOT NULL,
					table_name TEXT NOT NULL,
					chunk_index INT NOT NULL,
					chunk_start BIGINT NOT NULL,
					chunk_size BIGINT NOT NULL,
					range_start BIGINT,
					range_end BIGINT,
					block_start BIGINT,
					block_end BIGINT,
					is_last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
					partition_strategy TEXT NOT NULL DEFAULT 'offset',
					status TEXT NOT NULL DEFAULT 'pending',
					claimed_by TEXT,
					claimed_at TIMESTAMP,
					heartbeat_at TIMESTAMP,
					completed_at TIMESTAMP,
					rows_processed BIGINT DEFAULT 0,
					UNIQUE(slot_name, table_schema, table_name, chunk_index)
				)
			`, chunksTableName)},
			{requestTableName, fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					slot_name TEXT NOT NULL,
					resnapshot_id TEXT NOT NULL,
					PRIMARY KEY (slot_name, resnapshot_id)
				)
			`, requestTableName)},
		}
		for _, table := range tables {
			exists, err := pq.TableExists(ctx, conn, "public", table.name)
			if err != nil {
				return fmt.Errorf("check snapshot table %s: %w", table.name, err)
			}
			if !exists {
				if err := pq.ExecSQL(ctx, conn, table.sql); err != nil {
					return fmt.Errorf("create snapshot table %s: %w", table.name, err)
				}
			}
		}

		indexes := []struct {
			name string
			sql  string
		}{
			{"idx_chunks_claim", fmt.Sprintf(`CREATE INDEX idx_chunks_claim ON %s(slot_name, status, claimed_at) WHERE status IN ('pending', 'in_progress')`, chunksTableName)},
			{"idx_chunks_status", fmt.Sprintf(`CREATE INDEX idx_chunks_status ON %s(slot_name, status)`, chunksTableName)},
		}
		for _, index := range indexes {
			exists, err := pq.IndexExists(ctx, conn, "public", index.name)
			if err != nil {
				return fmt.Errorf("check snapshot index %s: %w", index.name, err)
			}
			if !exists {
				if err := pq.ExecSQL(ctx, conn, index.sql); err != nil {
					return fmt.Errorf("create snapshot index %s: %w", index.name, err)
				}
			}
		}

		columns := []struct {
			table      string
			name       string
			definition string
		}{
			{jobTableName, "begin_emitted", "BOOLEAN DEFAULT FALSE"},
			{jobTableName, "end_emitted", "BOOLEAN DEFAULT FALSE"},
			{jobTableName, "resnapshot_id", "TEXT NOT NULL DEFAULT ''"},
			{chunksTableName, "block_start", "BIGINT"},
			{chunksTableName, "block_end", "BIGINT"},
			{chunksTableName, "is_last_chunk", "BOOLEAN DEFAULT FALSE"},
			{chunksTableName, "partition_strategy", "TEXT DEFAULT 'offset'"},
			{chunksTableName, "range_start", "BIGINT"},
			{chunksTableName, "range_end", "BIGINT"},
		}
		for _, column := range columns {
			exists, err := pq.ColumnExists(ctx, conn, "public", column.table, column.name)
			if err != nil {
				return fmt.Errorf("check snapshot column %s.%s: %w", column.table, column.name, err)
			}
			if !exists {
				statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", pq.QuoteIdentifier(column.table), pq.QuoteIdentifier(column.name), column.definition)
				if err := pq.ExecSQL(ctx, conn, statement); err != nil {
					return fmt.Errorf("add snapshot column %s.%s: %w", column.table, column.name, err)
				}
			}
		}
		return nil
	})
	if err == nil {
		logger.Debug("[metadata] snapshot tables initialized")
	}
	return err
}

// EnsureMetadataTables creates or migrates snapshot metadata before callers
// inspect whether a job already exists.
func (s *Snapshotter) EnsureMetadataTables(ctx context.Context) error {
	return s.initTables(ctx)
}

func (s *Snapshotter) processChunk(ctx context.Context, conn pq.Connection, chunk *Chunk, lsn pq.LSN, handler Handler) (int64, error) {
	// Resolve the configured projection columns for this table.
	chunk.TableColumns = s.getSnapshotColumns(chunk.TableSchema, chunk.TableName)

	var orderByClause string
	var pkColumns []string
	if chunk.PartitionStrategy != PartitionStrategyCTIDBlock {
		table := publication.Table{
			Schema:  chunk.TableSchema,
			Name:    chunk.TableName,
			Columns: chunk.TableColumns,
		}
		var err error
		orderByClause, pkColumns, err = s.getOrderByClause(ctx, conn, table)
		if err != nil {
			return 0, fmt.Errorf("get order by clause: %w", err)
		}
	}

	query, err := s.buildChunkQuery(chunk, orderByClause, pkColumns, s.getQueryCondition(chunk.TableSchema, chunk.TableName))
	if err != nil {
		return 0, fmt.Errorf("build chunk query: %w", err)
	}

	logger.Debug("[chunk] executing query", "query", query)

	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return 0, fmt.Errorf("execute chunk query: %w", err)
	}

	if len(results) != 1 {
		return 0, errors.New("invalid chunk query result")
	}
	result := results[0]
	chunkTime := time.Now().UTC()

	for i, row := range result.Rows {
		if err := context.Cause(ctx); err != nil {
			return 0, err
		}
		rowData, err := s.parseRow(result.FieldDescriptions, row)
		if err != nil {
			return 0, fmt.Errorf("decode snapshot row: %w", err)
		}

		// Send data event. A failed handler must leave the chunk incomplete so it
		// can be delivered again after the caller recovers.
		if err := handler(&format.Snapshot{
			EventType:  format.SnapshotEventTypeData,
			Table:      chunk.TableName,
			Schema:     chunk.TableSchema,
			Data:       rowData,
			ServerTime: chunkTime,
			LSN:        lsn,
			IsLast:     i == len(result.Rows)-1,
		}); err != nil {
			return 0, fmt.Errorf("handle snapshot row: %w", err)
		}
	}

	return int64(len(result.Rows)), nil
}

func (s *Snapshotter) getQueryCondition(tableSchema, tableName string) string {
	tableSchema = normalizeSchema(tableSchema)
	for _, table := range s.tables {
		if normalizeSchema(table.Schema) == tableSchema && table.Name == tableName && table.QueryCondition != "" {
			return table.QueryCondition
		}
	}

	return s.config.QueryCondition
}

func andCondition(existing, extra string) string {
	if extra == "" {
		return existing
	}
	if existing == "" {
		return "(" + extra + ")"
	}
	return existing + " AND (" + extra + ")"
}

func (s *Snapshotter) buildChunkQuery(chunk *Chunk, orderByClause string, pkColumns []string, queryCondition string) (string, error) {
	if err := chunk.validatePartition(); err != nil {
		return "", err
	}

	switch chunk.PartitionStrategy {
	case PartitionStrategyIntegerRange:
		return s.buildIntegerRangeQuery(chunk, orderByClause, pkColumns, queryCondition)
	case PartitionStrategyCTIDBlock:
		return s.buildCTIDBlockQuery(chunk, queryCondition), nil
	case PartitionStrategyOffset:
		if orderByClause == "" {
			return "", errors.New("offset chunk requires an order-by clause")
		}
		return s.buildOffsetQuery(chunk, orderByClause, queryCondition), nil
	default:
		return "", fmt.Errorf("unknown snapshot partition strategy %q", chunk.PartitionStrategy)
	}
}

func (s *Snapshotter) buildIntegerRangeQuery(chunk *Chunk, orderByClause string, pkColumns []string, queryCondition string) (string, error) {
	if !chunk.hasRangeBounds() {
		return fmt.Sprintf(
			"SELECT %s FROM %s WHERE FALSE",
			selectSnapshotColumns(chunk.TableColumns),
			pq.QuoteQualifiedName(chunk.TableSchema, chunk.TableName),
		), nil
	}
	if orderByClause == "" {
		return "", errors.New("integer-range chunk requires an order-by clause")
	}
	if len(pkColumns) != 1 {
		return "", fmt.Errorf("integer-range chunk requires exactly one primary-key column, got %d", len(pkColumns))
	}

	pkColumn := pq.QuoteIdentifier(pkColumns[0])
	whereClause := fmt.Sprintf("%s >= %d AND %s <= %d", pkColumn, *chunk.RangeStart, pkColumn, *chunk.RangeEnd)
	whereClause = andCondition(whereClause, queryCondition)
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		selectSnapshotColumns(chunk.TableColumns),
		pq.QuoteQualifiedName(chunk.TableSchema, chunk.TableName),
		whereClause,
		orderByClause,
		chunk.ChunkSize,
	), nil
}

func (s *Snapshotter) buildCTIDBlockQuery(chunk *Chunk, queryCondition string) string {
	whereClause := ""
	if chunk.BlockStart != nil {
		whereClause = fmt.Sprintf("ctid >= '(%d,0)'::tid", *chunk.BlockStart)
		if !chunk.IsLastChunk {
			whereClause += fmt.Sprintf(" AND ctid < '(%d,0)'::tid", *chunk.BlockEnd)
		}
	}
	whereClause = andCondition(whereClause, queryCondition)

	query := fmt.Sprintf("SELECT %s FROM %s", selectSnapshotColumns(chunk.TableColumns), pq.QuoteQualifiedName(chunk.TableSchema, chunk.TableName))
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	return query
}

func (s *Snapshotter) buildOffsetQuery(chunk *Chunk, orderByClause string, queryCondition string) string {
	cols := selectSnapshotColumns(chunk.TableColumns)
	tableName := pq.QuoteQualifiedName(chunk.TableSchema, chunk.TableName)
	where := ""
	if queryCondition != "" {
		where = " WHERE (" + queryCondition + ")"
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT %d OFFSET %d",
		cols,
		tableName,
		where,
		orderByClause,
		chunk.ChunkSize,
		chunk.ChunkStart,
	)
}

func (s *Snapshotter) getOrderByClause(ctx context.Context, conn pq.Connection, table publication.Table) (string, []string, error) {
	if entry, ok := s.loadOrderByCache(table); ok {
		return entry.clause, entry.columns, nil
	}

	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, conn, table)
	if err != nil {
		return "", nil, err
	}

	if len(columns) > 0 {
		columnNames := make([]string, len(columns))
		orderByColumns := make([]string, len(columns))
		for i, column := range columns {
			columnNames[i] = column.Name
			orderByColumns[i] = pq.QuoteIdentifier(column.Name)
		}
		orderBy := strings.Join(orderByColumns, ", ")
		logger.Debug("[chunk] using primary key for ordering", "table", table.Name, "orderBy", orderBy)
		s.storeOrderByCache(table, orderBy, columnNames)
		return orderBy, columnNames, nil
	}

	// No primary key, use ctid (PostgreSQL internal row identifier)
	logger.Debug("[chunk] no primary key, using ctid", "table", table.Name)
	s.storeOrderByCache(table, "ctid", nil)
	return "ctid", nil, nil
}

func (s *Snapshotter) loadOrderByCache(table publication.Table) (orderByCacheEntry, bool) {
	key := s.orderByCacheKey(table)

	s.orderByMu.RLock()
	entry, ok := s.orderByCache[key]
	s.orderByMu.RUnlock()

	if !ok {
		return orderByCacheEntry{}, false
	}

	return orderByCacheEntry{
		clause:  entry.clause,
		columns: slices.Clone(entry.columns),
	}, true
}

func (s *Snapshotter) storeOrderByCache(table publication.Table, clause string, columns []string) {
	key := s.orderByCacheKey(table)

	s.orderByMu.Lock()
	s.orderByCache[key] = orderByCacheEntry{
		clause:  clause,
		columns: slices.Clone(columns),
	}
	s.orderByMu.Unlock()
}

func (s *Snapshotter) orderByCacheKey(table publication.Table) string {
	return normalizeSchema(table.Schema) + "." + table.Name
}

func (s *Snapshotter) newChunk(slotName string, table publication.Table, strategy PartitionStrategy) *Chunk {
	return &Chunk{
		SlotName:          slotName,
		TableSchema:       normalizeSchema(table.Schema),
		TableName:         table.Name,
		ChunkSize:         s.config.ChunkSize,
		Status:            ChunkStatusPending,
		PartitionStrategy: strategy,
	}
}

// createTableChunks plans one table entirely from the exported snapshot.
func (s *Snapshotter) createTableChunks(ctx context.Context, conn pq.Connection, slotName string, table publication.Table) ([]*Chunk, error) {
	strategy := table.SnapshotPartitionStrategy
	var pkColumn string
	if strategy == publication.SnapshotPartitionStrategyAuto || strategy == publication.SnapshotPartitionStrategyIntegerRange {
		column, ok, err := s.getSingleIntegerPrimaryKey(ctx, conn, table)
		if err != nil {
			return nil, fmt.Errorf("inspect primary key: %w", err)
		}
		if strategy == publication.SnapshotPartitionStrategyAuto {
			if ok {
				strategy = publication.SnapshotPartitionStrategyIntegerRange
			} else {
				strategy = publication.SnapshotPartitionStrategyCTIDBlock
			}
		} else if !ok {
			return nil, errors.New("integer_range requires exactly one integer primary-key column")
		}
		pkColumn = column
	}

	logger.Info("[chunk] using partition strategy", "table", table.Name, "strategy", strategy)
	switch strategy {
	case publication.SnapshotPartitionStrategyIntegerRange:
		return s.createRangeChunks(ctx, conn, slotName, table, pkColumn)
	case publication.SnapshotPartitionStrategyCTIDBlock:
		return s.createCTIDBlockChunks(ctx, conn, slotName, table)
	case publication.SnapshotPartitionStrategyOffset:
		return s.createOffsetChunks(ctx, conn, slotName, table)
	default:
		return nil, fmt.Errorf("unknown snapshot partition strategy %q", strategy)
	}
}

func (s *Snapshotter) createRangeChunks(ctx context.Context, conn pq.Connection, slotName string, table publication.Table, pkColumn string) ([]*Chunk, error) {
	minValue, maxValue, hasBounds, err := s.getPrimaryKeyBounds(ctx, conn, table, pkColumn)
	if err != nil {
		return nil, err
	}
	if !hasBounds {
		return []*Chunk{s.newChunk(slotName, table, PartitionStrategyIntegerRange)}, nil
	}

	chunkSize := s.config.ChunkSize
	var chunks []*Chunk
	for rangeStart := minValue; ; {
		rangeEnd := rangeStart + chunkSize - 1
		if rangeEnd < rangeStart || rangeEnd > maxValue {
			rangeEnd = maxValue
		}
		startValue, endValue := rangeStart, rangeEnd
		chunk := s.newChunk(slotName, table, PartitionStrategyIntegerRange)
		chunk.ChunkIndex = len(chunks)
		chunk.ChunkStart = int64(chunk.ChunkIndex) * chunkSize
		chunk.RangeStart = &startValue
		chunk.RangeEnd = &endValue
		chunks = append(chunks, chunk)
		if rangeEnd == maxValue {
			break
		}
		rangeStart = rangeEnd + 1
	}

	logger.Info("[chunk] range chunks created",
		"table", table.Name,
		"chunkSize", chunkSize,
		"numChunks", len(chunks),
		"rangeMin", minValue,
		"rangeMax", maxValue,
	)
	return chunks, nil
}

func (s *Snapshotter) createCTIDBlockChunks(ctx context.Context, conn pq.Connection, slotName string, table publication.Table) ([]*Chunk, error) {
	query := fmt.Sprintf(
		"SELECT (pg_relation_size(%s::regclass) / current_setting('block_size')::int)::bigint",
		snapshotRegclassLiteral(table.Schema, table.Name),
	)
	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("query table block count: %w", err)
	}
	value, err := singleValue(results, "table block count")
	if err != nil {
		return nil, err
	}
	totalBlocks, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse table block count: %w", err)
	}
	if totalBlocks < 0 {
		return nil, errors.New("table block count must not be negative")
	}
	if totalBlocks == 0 {
		return []*Chunk{s.newChunk(slotName, table, PartitionStrategyCTIDBlock)}, nil
	}

	estimatedRowsPerBlock, err := s.estimateRowsPerBlock(ctx, conn, table)
	if err != nil {
		return nil, err
	}
	blocksPerChunk := s.config.ChunkSize / estimatedRowsPerBlock
	if blocksPerChunk < 1 {
		blocksPerChunk = 1
	}

	numChunks := 1 + (totalBlocks-1)/blocksPerChunk
	chunks := make([]*Chunk, 0, numChunks)
	for i := int64(0); i < numChunks; i++ {
		blockStart := i * blocksPerChunk
		isLastChunk := i == numChunks-1
		var blockEnd *int64
		if !isLastChunk {
			end := blockStart + blocksPerChunk
			if end < blockStart || end > totalBlocks {
				end = totalBlocks
			}
			blockEnd = &end
		}
		chunk := s.newChunk(slotName, table, PartitionStrategyCTIDBlock)
		chunk.ChunkIndex = int(i)
		chunk.ChunkStart = blockStart
		chunk.BlockStart = &blockStart
		chunk.BlockEnd = blockEnd
		chunk.IsLastChunk = isLastChunk
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] CTID block chunks created",
		"table", table.Name,
		"totalBlocks", totalBlocks,
		"blocksPerChunk", blocksPerChunk,
		"numChunks", len(chunks),
		"estimatedRowsPerBlock", estimatedRowsPerBlock,
	)
	return chunks, nil
}

func (s *Snapshotter) estimateRowsPerBlock(ctx context.Context, conn pq.Connection, table publication.Table) (int64, error) {
	query := fmt.Sprintf(`
		SELECT CASE
			WHEN relpages > 0 THEN GREATEST((reltuples / relpages)::bigint, 1)
			ELSE 100
		END
		FROM pg_class
		WHERE oid = %s::regclass
	`, snapshotRegclassLiteral(table.Schema, table.Name))
	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return 0, fmt.Errorf("estimate rows per block: %w", err)
	}
	value, err := singleValue(results, "rows-per-block estimate")
	if err != nil {
		return 0, err
	}
	rowsPerBlock, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse rows-per-block estimate: %w", err)
	}
	if rowsPerBlock < 1 {
		return 0, errors.New("rows-per-block estimate must be positive")
	}
	return rowsPerBlock, nil
}

func (s *Snapshotter) createOffsetChunks(ctx context.Context, conn pq.Connection, slotName string, table publication.Table) ([]*Chunk, error) {
	rowCount, err := s.getTableRowCount(ctx, conn, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}

	chunkSize := s.config.ChunkSize
	if rowCount == 0 {
		return []*Chunk{s.newChunk(slotName, table, PartitionStrategyOffset)}, nil
	}

	numChunks := 1 + (rowCount-1)/chunkSize
	chunks := make([]*Chunk, 0, numChunks)
	for i := int64(0); i < numChunks; i++ {
		chunk := s.newChunk(slotName, table, PartitionStrategyOffset)
		chunk.ChunkIndex = int(i)
		chunk.ChunkStart = i * chunkSize
		chunks = append(chunks, chunk)
	}

	logger.Info("[chunk] offset chunks created", "table", table.Name, "rowCount", rowCount, "chunkSize", chunkSize, "numChunks", len(chunks))
	return chunks, nil
}

func singleValue(results []*pgconn.Result, name string) ([]byte, error) {
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 1 || results[0].Rows[0][0] == nil {
		return nil, fmt.Errorf("invalid %s result", name)
	}
	return results[0].Rows[0][0], nil
}

func (s *Snapshotter) getPrimaryKeyColumnsDetailed(ctx context.Context, conn pq.Connection, table publication.Table) ([]primaryKeyColumn, error) {
	query := fmt.Sprintf(`
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_index i
		JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY AS key(attnum, position)
		  ON key.position <= i.indnkeyatts
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = key.attnum
		WHERE i.indrelid = %s::regclass AND i.indisprimary
		ORDER BY key.position
	`, snapshotRegclassLiteral(table.Schema, table.Name))

	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return nil, fmt.Errorf("query primary key: %w", err)
	}
	return parsePrimaryKeyColumns(results)
}

func parsePrimaryKeyColumns(results []*pgconn.Result) ([]primaryKeyColumn, error) {
	if len(results) != 1 {
		return nil, errors.New("invalid primary-key metadata result")
	}

	columns := make([]primaryKeyColumn, 0, len(results[0].Rows))
	for _, row := range results[0].Rows {
		if len(row) != 2 || len(row[0]) == 0 || len(row[1]) == 0 {
			return nil, errors.New("invalid primary-key metadata row")
		}
		columns = append(columns, primaryKeyColumn{
			Name:     string(row[0]),
			DataType: strings.ToLower(string(row[1])),
		})
	}
	return columns, nil
}

func (s *Snapshotter) getSingleIntegerPrimaryKey(ctx context.Context, conn pq.Connection, table publication.Table) (string, bool, error) {
	columns, err := s.getPrimaryKeyColumnsDetailed(ctx, conn, table)
	if err != nil {
		return "", false, err
	}

	if len(columns) != 1 || !isIntegerType(columns[0].DataType) {
		return "", false, nil
	}
	return columns[0].Name, true, nil
}

func isIntegerType(dataType string) bool {
	switch dataType {
	case "smallint", "integer", "bigint", "int2", "int4", "int8":
		return true
	default:
		return false
	}
}

func snapshotRegclassLiteral(schema, table string) string {
	return pq.QuoteLiteral(pq.QuoteQualifiedName(schema, table))
}

func (s *Snapshotter) getPrimaryKeyBounds(ctx context.Context, conn pq.Connection, table publication.Table, pkColumn string) (int64, int64, bool, error) {
	quotedPKColumn := pq.QuoteIdentifier(pkColumn)
	tableName := pq.QuoteQualifiedName(table.Schema, table.Name)
	query := fmt.Sprintf(`
		SELECT MIN(%s)::bigint AS min_value, MAX(%s)::bigint AS max_value
		FROM %s
	`, quotedPKColumn, quotedPKColumn, tableName)
	if condition := s.getQueryCondition(table.Schema, table.Name); condition != "" {
		query += " WHERE (" + condition + ")"
	}

	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return 0, 0, false, fmt.Errorf("query primary key bounds: %w", err)
	}

	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 2 {
		return 0, 0, false, errors.New("invalid primary-key bounds result")
	}

	row := results[0].Rows[0]
	if row[0] == nil && row[1] == nil {
		return 0, 0, false, nil
	}
	if row[0] == nil || row[1] == nil {
		return 0, 0, false, errors.New("incomplete primary-key bounds")
	}

	minValue, err := strconv.ParseInt(string(row[0]), 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse min value: %w", err)
	}

	maxValue, err := strconv.ParseInt(string(row[1]), 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse max value: %w", err)
	}
	if minValue > maxValue {
		return 0, 0, false, errors.New("primary-key minimum exceeds maximum")
	}

	return minValue, maxValue, true, nil
}

// parseRow converts PostgreSQL row data to map with proper type conversion
func (s *Snapshotter) parseRow(fields []pgconn.FieldDescription, row [][]byte) (map[string]any, error) {
	if len(row) != len(fields) {
		return nil, errors.New("snapshot row does not match its field descriptions")
	}
	rowData := make(map[string]any, len(fields))

	for i, field := range fields {
		columnName := field.Name
		columnValue := row[i]

		if columnValue == nil {
			rowData[columnName] = nil
			continue
		}

		// Convert to appropriate type using pgtype
		val, err := s.decoderCache.Get(field.DataTypeOID).Decode(s.typeMap, columnValue)
		if err != nil {
			return nil, fmt.Errorf("decode column %s: %w", columnName, err)
		}

		rowData[columnName] = val
	}

	return rowData, nil
}

const chunkBatchSize = 1000

func (s *Snapshotter) saveChunksBatch(ctx context.Context, conn pq.Connection, chunks []*Chunk) error {
	for i := 0; i < len(chunks); i += chunkBatchSize {
		end := min(i+chunkBatchSize, len(chunks))
		batch := chunks[i:end]
		values := make([]string, len(batch))
		for j, chunk := range batch {
			value, err := chunkValueSQL(chunk)
			if err != nil {
				return fmt.Errorf("encode chunk %d: %w", i+j, err)
			}
			values[j] = value
		}

		query := fmt.Sprintf(`
			INSERT INTO %s (
				slot_name, table_schema, table_name, chunk_index,
				chunk_start, chunk_size, range_start, range_end,
				block_start, block_end, is_last_chunk, partition_strategy, status
			) VALUES %s
		`, chunksTableName, strings.Join(values, ","))
		if err := pq.ExecSQL(ctx, conn, query); err != nil {
			return fmt.Errorf("insert chunk batch %d-%d: %w", i, end, err)
		}
	}

	return nil
}

func chunkValueSQL(chunk *Chunk) (string, error) {
	if err := chunk.validatePartition(); err != nil {
		return "", err
	}
	return fmt.Sprintf("(%s, %s, %s, %d, %d, %d, %s, %s, %s, %s, %t, %s, %s)",
		pq.QuoteLiteral(chunk.SlotName),
		pq.QuoteLiteral(chunk.TableSchema),
		pq.QuoteLiteral(chunk.TableName),
		chunk.ChunkIndex,
		chunk.ChunkStart,
		chunk.ChunkSize,
		nullableInt64SQL(chunk.RangeStart),
		nullableInt64SQL(chunk.RangeEnd),
		nullableInt64SQL(chunk.BlockStart),
		nullableInt64SQL(chunk.BlockEnd),
		chunk.IsLastChunk,
		pq.QuoteLiteral(string(chunk.PartitionStrategy)),
		pq.QuoteLiteral(string(chunk.Status)),
	), nil
}

func (s *Snapshotter) getTableRowCount(ctx context.Context, conn pq.Connection, schema, table string) (int64, error) {
	queryCondition := s.getQueryCondition(schema, table)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteQualifiedName(schema, table))
	if queryCondition != "" {
		query += " WHERE (" + queryCondition + ")"
	}

	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return 0, fmt.Errorf("table row count: %w", err)
	}

	value, err := singleValue(results, "table row count")
	if err != nil {
		return 0, err
	}
	count, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse row count: %w", err)
	}
	if count < 0 {
		return 0, errors.New("table row count must not be negative")
	}

	return count, nil
}

func (s *Snapshotter) saveJob(ctx context.Context, conn pq.Connection, job *Job) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			slot_name, snapshot_id, resnapshot_id, snapshot_lsn, started_at,
			completed, total_chunks, completed_chunks
		) VALUES (%s, %s, %s, %s, %s, %t, %d, %d)
	`, jobTableName,
		pq.QuoteLiteral(job.SlotName),
		pq.QuoteLiteral(job.SnapshotID),
		pq.QuoteLiteral(job.ResnapshotID),
		pq.QuoteLiteral(job.SnapshotLSN.String()),
		pq.QuoteLiteral(job.StartedAt.Format(postgresTimestampFormatMicros)),
		job.Completed,
		job.TotalChunks,
		job.CompletedChunks,
	)
	if err := pq.ExecSQL(ctx, conn, query); err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

func (s *Snapshotter) tryAcquireCoordinatorLock(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(
		"SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(%s))",
		pq.QuoteLiteral("go-pq-cdc:snapshot:"+slotName),
	)
	results, err := pq.ExecQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, fmt.Errorf("acquire coordinator lock: %w", err)
	}

	value, err := singleValue(results, "coordinator lock")
	if err != nil {
		return false, err
	}
	acquired, err := parsePostgresBool(value)
	if err != nil {
		return false, fmt.Errorf("parse coordinator lock result: %w", err)
	}
	return acquired, nil
}

func (s *Snapshotter) releaseCoordinatorLock(ctx context.Context, slotName string) error {
	query := fmt.Sprintf(
		"SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(%s))",
		pq.QuoteLiteral("go-pq-cdc:snapshot:"+slotName),
	)
	results, err := pq.ExecQuery(ctx, s.metadataConn, query)
	if err != nil {
		return fmt.Errorf("release coordinator lock: %w", err)
	}
	value, err := singleValue(results, "coordinator unlock")
	if err != nil {
		return err
	}
	released, err := parsePostgresBool(value)
	if err != nil {
		return fmt.Errorf("parse coordinator unlock result: %w", err)
	}
	if !released {
		return errors.New("coordinator lock was not held")
	}
	return nil
}
