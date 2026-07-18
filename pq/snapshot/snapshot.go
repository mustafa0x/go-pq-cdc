package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/jackc/pgx/v5/pgtype"
)

const snapshotCleanupTimeout = 5 * time.Second

var (
	// ErrSnapshotInvalidated indicates that the exported transaction no longer exists.
	ErrSnapshotInvalidated = errors.New("snapshot invalidated by coordinator restart")
	// ErrResnapshotSuperseded prevents an old request from executing a newer generation.
	ErrResnapshotSuperseded = errors.New("resnapshot request was superseded")
)

// Handler consumes snapshot events.
type Handler func(event *format.Snapshot) error

type Snapshotter struct {
	healthcheckConn    pq.Connection
	exportSnapshotConn pq.Connection
	metric             metric.Metric
	metadataConn       pq.Connection
	keepaliveCancel    context.CancelFunc
	typeMap            *pgtype.Map
	decoderCache       *DecoderCache
	workerConn         pq.Connection
	orderByCache       map[string]orderByCacheEntry
	keepaliveDone      chan struct{}
	dsn                string
	tables             publication.Tables
	config             config.SnapshotConfig
	orderByMu          sync.RWMutex
	keepaliveMu        sync.Mutex
	closeOnce          sync.Once
}

type orderByCacheEntry struct {
	clause  string
	columns []string
}

func New(ctx context.Context, snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string, m metric.Metric) (*Snapshotter, error) {
	snapshotter := &Snapshotter{
		dsn:          dsn,
		decoderCache: NewDecoderCache(),
		config:       snapshotConfig,
		tables:       tables,
		typeMap:      pgtype.NewMap(),
		metric:       m,
		orderByCache: make(map[string]orderByCacheEntry),
	}

	var err error
	snapshotter.metadataConn, err = pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create metadata connection: %w", err)
	}

	snapshotter.healthcheckConn, err = pq.NewConnection(ctx, dsn)
	if err != nil {
		snapshotter.Close(ctx)
		return nil, fmt.Errorf("create healthcheck connection: %w", err)
	}

	snapshotter.workerConn, err = pq.NewConnection(ctx, dsn)
	if err != nil {
		snapshotter.Close(ctx)
		return nil, fmt.Errorf("create worker connection: %w", err)
	}

	return snapshotter, nil
}

// Prepare exports a consistent snapshot and persists its work plan. CDC callers
// must establish WAL retention before calling it.
//
// Flow:
//  1. Elect a coordinator.
//  2. Capture the current LSN and export a read-only snapshot.
//  3. Plan all chunks from that snapshot.
//  4. Commit the job and chunks atomically.
func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] preparing", "instanceID", instanceID)
	s.orderByMu.Lock()
	clear(s.orderByCache)
	s.orderByMu.Unlock()

	if err := s.setupJob(ctx, slotName, instanceID); err != nil {
		return fmt.Errorf("setup job: %w", err)
	}
	return nil
}

// Execute performs the actual snapshot data collection
// This should be called AFTER the replication slot is created with the LSN from Prepare()
// Returns when snapshot is complete
func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	_, err := s.ExecuteWithLSN(ctx, handler, slotName)
	return err
}

// ExecuteWithLSN performs the snapshot and returns the exact checkpoint delivered to the handler.
func (s *Snapshotter) ExecuteWithLSN(ctx context.Context, handler Handler, slotName string) (pq.LSN, error) {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return 0, fmt.Errorf("load snapshot job: %w", err)
	}
	if job == nil {
		return 0, fmt.Errorf("snapshot job disappeared: %w", ErrSnapshotInvalidated)
	}
	if err := s.validateJobRequest(job); err != nil {
		return 0, err
	}

	s.metric.SetSnapshotInProgress(true)
	s.metric.SetSnapshotTotalTables(len(s.tables))
	s.metric.SetSnapshotTotalChunks(job.TotalChunks)
	defer func() {
		s.metric.SetSnapshotInProgress(false)
		s.metric.SetSnapshotDurationSeconds(time.Since(startTime).Seconds())
	}()

	if err := s.emitSnapshotMarker(ctx, job, format.SnapshotEventTypeBegin, handler); err != nil {
		return 0, fmt.Errorf("emit snapshot begin: %w", err)
	}
	if err := s.workerProcess(ctx, instanceID, job, handler); err != nil {
		return 0, fmt.Errorf("execute worker: %w", err)
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")
	if err := s.emitSnapshotMarker(ctx, job, format.SnapshotEventTypeEnd, handler); err != nil {
		return 0, fmt.Errorf("emit snapshot end: %w", err)
	}
	s.closeAllConnections()

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return job.SnapshotLSN, nil
}

// closeAllConnections closes every connection owned by the snapshotter.
func (s *Snapshotter) closeAllConnections() {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		logger.Info("[snapshot] closing all connections")
		s.closeExportSnapshotConnection()
		closeConnection := func(name string, conn pq.Connection) {
			if conn != nil {
				if err := conn.Close(ctx); err != nil {
					logger.Warn("[snapshot] error closing connection", "connection", name, "error", err)
				}
			}
		}
		closeConnection("worker", s.workerConn)
		closeConnection("metadata", s.metadataConn)
		closeConnection("healthcheck", s.healthcheckConn)
		logger.Info("[snapshot] all connections closed")
	})
}

// closeExportSnapshotConnection rolls back the read-only exported transaction and closes it.
func (s *Snapshotter) closeExportSnapshotConnection() {
	s.keepaliveMu.Lock()
	s.stopSnapshotKeepaliveLocked()
	exportConn := s.exportSnapshotConn
	s.exportSnapshotConn = nil
	s.keepaliveMu.Unlock()
	if exportConn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), snapshotCleanupTimeout)
	defer cancel()
	if err := pq.ExecSQL(ctx, exportConn, "ROLLBACK"); err != nil {
		logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
	}
	if err := exportConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}
}

// Close releases every resource owned by the snapshotter.
func (s *Snapshotter) Close(context.Context) {
	s.closeAllConnections()
}

func (s *Snapshotter) startSnapshotKeepalive(parentCtx context.Context, conn pq.Connection) {
	s.keepaliveMu.Lock()
	defer s.keepaliveMu.Unlock()

	s.stopSnapshotKeepaliveLocked()
	if s.exportSnapshotConn != conn {
		return
	}
	keepaliveCtx, cancel := context.WithCancel(parentCtx)
	s.keepaliveCancel = cancel
	s.keepaliveDone = make(chan struct{})
	go s.snapshotTransactionKeepalive(keepaliveCtx, conn, s.keepaliveDone)
}

// stopSnapshotKeepaliveLocked stops the keepalive before its connection can be
// replaced or closed. The caller owns keepaliveMu for the whole transition.
func (s *Snapshotter) stopSnapshotKeepaliveLocked() {
	if s.keepaliveCancel == nil {
		return
	}
	s.keepaliveCancel()
	<-s.keepaliveDone
	s.keepaliveCancel = nil
	s.keepaliveDone = nil
}

func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
