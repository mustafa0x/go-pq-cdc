package snapshot

import (
	"context"
	goerrors "errors"
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
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Sentinel errors for snapshot operations
var (
	// ErrSnapshotInvalidated indicates the snapshot transaction was closed (coordinator restart)
	ErrSnapshotInvalidated = goerrors.New("snapshot invalidated by coordinator restart")
)

// Handler SnapshotHandler is a function that handles snapshot events
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
	cachedSnapshotID   string
	tables             publication.Tables
	config             config.SnapshotConfig
	orderByMu          sync.RWMutex
	keepaliveMu        sync.Mutex
	closeOnce          sync.Once
	exportConnClosed   bool
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
		return nil, errors.Wrap(err, "create metadata connection")
	}

	snapshotter.healthcheckConn, err = pq.NewConnection(ctx, dsn)
	if err != nil {
		snapshotter.Close(ctx)
		return nil, errors.Wrap(err, "create healthcheck connection")
	}

	snapshotter.workerConn, err = pq.NewConnection(ctx, dsn)
	if err != nil {
		snapshotter.Close(ctx)
		return nil, errors.Wrap(err, "create worker connection")
	}

	return snapshotter, nil
}

// Prepare sets up snapshot metadata and exports snapshot transaction
// This must be called BEFORE creating the replication slot to avoid data loss
// Returns the snapshot LSN that should be used for replication slot creation
//
// Flow:
//  1. Coordinator election
//  2. Capture current LSN
//  3. Create metadata (job, chunks)
//  4. Export snapshot transaction (keeps transaction OPEN)
//  5. Return LSN for slot creation
//
// IMPORTANT: Replication slot MUST be created immediately after this returns
// to ensure no WAL changes are lost during snapshot execution
func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] preparing", "instanceID", instanceID)

	isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return errors.Wrap(err, "setup job")
	}

	if isCoordinator {
		logger.Debug("[coordinator] snapshot transaction kept OPEN - replication slot must be created NOW")
	}
	return nil
}

// Execute performs the actual snapshot data collection
// This should be called AFTER the replication slot is created with the LSN from Prepare()
// Returns when snapshot is complete
func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	// Load job
	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return errors.New("job not found - Prepare() must be called first")
	}

	// Execute worker processing (ALL instances work, including coordinator)
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return fmt.Errorf("execute worker: %w", err)
	}

	// Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

// finalizeSnapshot checks completion, closes connections, and sends END marker
func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	completed, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}
	if !completed {
		return nil
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")
	defer s.closeAllConnections(ctx)
	return s.emitSnapshotMarker(ctx, slotName, format.SnapshotEventTypeEnd, job.SnapshotLSN, handler)
}

// closeAllConnections closes every connection owned by the snapshotter.
func (s *Snapshotter) closeAllConnections(context.Context) {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		logger.Info("[snapshot] closing all connections")
		s.closeExportSnapshotConnection(ctx)
		connections := []struct {
			name string
			conn pq.Connection
		}{
			{"worker", s.workerConn},
			{"metadata", s.metadataConn},
			{"healthcheck", s.healthcheckConn},
		}
		for _, connection := range connections {
			if connection.conn != nil {
				if err := connection.conn.Close(ctx); err != nil {
					logger.Warn("[snapshot] error closing connection", "connection", connection.name, "error", err)
				}
			}
		}
		logger.Info("[snapshot] all connections closed")
	})
}

// closeExportSnapshotConnection rolls back the read-only exported transaction and closes it.
func (s *Snapshotter) closeExportSnapshotConnection(ctx context.Context) {
	s.stopSnapshotKeepalive()

	s.keepaliveMu.Lock()
	if s.exportConnClosed {
		s.keepaliveMu.Unlock()
		return
	}
	s.exportConnClosed = true
	exportConn := s.exportSnapshotConn
	s.keepaliveMu.Unlock()
	if exportConn == nil {
		return
	}

	if err := s.execSQL(ctx, exportConn, "ROLLBACK"); err != nil {
		logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
	}
	if err := exportConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}
}

// Close releases every resource owned by the snapshotter.
func (s *Snapshotter) Close(ctx context.Context) {
	if s != nil {
		s.closeAllConnections(ctx)
	}
}

func (s *Snapshotter) startSnapshotKeepalive(parentCtx context.Context, conn pq.Connection) {
	keepaliveCtx, keepaliveCancel := context.WithCancel(parentCtx)
	done := make(chan struct{})

	s.keepaliveMu.Lock()
	// Ensure previous keepalive is not left around during retries.
	if s.keepaliveCancel != nil {
		s.keepaliveCancel()
	}
	s.keepaliveCancel = keepaliveCancel
	s.keepaliveDone = done
	s.exportConnClosed = false
	s.keepaliveMu.Unlock()

	go s.snapshotTransactionKeepalive(keepaliveCtx, conn, done)
}

func (s *Snapshotter) stopSnapshotKeepalive() {
	s.keepaliveMu.Lock()
	cancel := s.keepaliveCancel
	done := s.keepaliveDone
	s.keepaliveCancel = nil
	s.keepaliveDone = nil
	s.keepaliveMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// decodeColumnData decodes PostgreSQL column data using cached decoder
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	// Use cached decoder (optimization: avoid reflection overhead)
	decoder := s.decoderCache.Get(dataTypeOID)
	return decoder.Decode(s.typeMap, data)
}

// generateInstanceID generates a unique instance identifier
func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
