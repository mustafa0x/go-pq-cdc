package cdc

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/heartbeat"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"

	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/prometheus/client_golang/prometheus"
)

type Connector interface {
	Start(ctx context.Context) error
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

// runningStreamer is connector-internal so adding lifecycle observation does
// not break external implementations of replication.Streamer.
type runningStreamer interface {
	replication.Streamer
	slot.XLogUpdater
	OpenFromSnapshotLSNAt(pq.LSN)
	Done() <-chan struct{}
	Err() error
}

// ErrConnectorStarted is returned when Start is called more than once.
var ErrConnectorStarted = errors.New("connector already started")

const connectorShutdownTimeout = 30 * time.Second

type connector struct {
	heartbeat          *heartbeat.Heartbeat
	prometheusRegistry metric.Registry
	server             http.Server
	stream             runningStreamer
	timescaleDB        *timescaledb.TimescaleDB
	slot               *slot.Slot
	readyCh            chan struct{}
	cfg                *config.Config
	snapshotter        *snapshot.Snapshotter
	listenerFunc       replication.ListenerFunc
	stopOnce           sync.Once
	cleanupOnce        sync.Once
	runMu              sync.Mutex
	runCancel          context.CancelCauseFunc
	runDone            chan struct{}
	stopCh             chan struct{}
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}
	logger.InitLogger(cfg.Logger.Logger)
	cfg.Print()

	// Snapshot-only mode: minimal setup without CDC components
	if cfg.IsSnapshotOnlyMode() {
		return newSnapshotOnlyConnector(ctx, cfg, listenerFunc)
	}

	// Normal CDC mode: full setup with publication, slot, stream
	// Normal connection for publication setup
	// This uses regular DSN (no replication parameter) to avoid consuming max_wal_senders limit
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), connectorShutdownTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	hb, publicationInfo, err := setupHeartbeatAndPublication(ctx, cfg, conn)
	if err != nil {
		return nil, err
	}

	m := metric.NewMetric(cfg.Slot.Name)

	// Get tables to snapshot (either from snapshot.tables or publication.tables)
	snapshotTables, err := cfg.GetSnapshotTables(publicationInfo)
	if err != nil {
		return nil, fmt.Errorf("get snapshot tables: %w", err)
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	stream, ok := replication.NewStream(cfg.ReplicationDSN(), cfg, m, listenerFunc).(runningStreamer)
	if !ok {
		if snapshotter != nil {
			snapshotter.Close(ctx)
		}
		return nil, errors.New("replication stream does not expose required connector capabilities")
	}

	sl := slot.NewSlot(cfg.ReplicationDSN(), cfg.DSN(), cfg.Slot, m, stream)

	prometheusRegistry := metric.NewRegistry(m)

	c := &connector{
		cfg:                &cfg,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, sl),
		slot:               sl,
		heartbeat:          hb,
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		readyCh:            make(chan struct{}),
		stopCh:             make(chan struct{}),
	}

	c.timescaleDB, err = initializeTimescaleDB(ctx, cfg)
	if err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

// newSnapshotOnlyConnector creates a minimal connector for snapshot-only mode
// without CDC components (publication, slot, replication stream)
func newSnapshotOnlyConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	// Use a dummy metric name since we don't have a slot
	m := metric.NewMetric("snapshot_only")

	// Get tables to snapshot from snapshot.tables
	snapshotTables, err := cfg.GetSnapshotTables(nil) // nil publicationInfo for snapshot_only mode
	if err != nil {
		return nil, fmt.Errorf("get snapshot tables: %w", err)
	}

	// Initialize snapshotter with tables from snapshot config
	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	logger.Info("snapshot-only mode enabled", "tables", len(snapshotTables))

	return &connector{
		cfg:                &cfg,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, nil),
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		readyCh:            make(chan struct{}),
		stopCh:             make(chan struct{}),
		// CDC components left nil: system, stream, slot
	}, nil
}

func setupHeartbeatAndPublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*heartbeat.Heartbeat, *publication.Config, error) {
	var hb *heartbeat.Heartbeat
	if cfg.IsHeartbeatEnabled() {
		hb = heartbeat.New(cfg.DSN(), cfg.Heartbeat)
		if err := hb.EnsureTable(ctx, conn); err != nil {
			return nil, nil, fmt.Errorf("create heartbeat table: %w", err)
		}
	}

	publicationInfo, err := initializePublication(ctx, cfg, conn)
	if err != nil {
		return nil, nil, err
	}
	if err := cfg.ValidateHeartbeatInPublication(publicationInfo); err != nil {
		return nil, nil, err
	}
	logger.Info("publication", "info", publicationInfo)
	return hb, publicationInfo, nil
}

func initializeTimescaleDB(ctx context.Context, cfg config.Config) (*timescaledb.TimescaleDB, error) {
	if !cfg.ExtensionSupport.EnableTimeScaleDB {
		return nil, nil
	}
	tdb, err := timescaledb.NewTimescaleDB(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}
	if _, err = tdb.FindHyperTables(ctx); err != nil {
		tdb.Close(ctx)
		return nil, err
	}
	return tdb, nil
}

// initializePublication sets up and creates the publication
func initializePublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*publication.Config, error) {
	pub := publication.New(cfg.Publication, conn)
	if err := pub.SetReplicaIdentities(ctx); err != nil {
		return nil, err
	}
	return pub.Create(ctx)
}

// initializeSnapshot creates snapshot if enabled
// tables parameter should come from publicationInfo (not from config) to support both scenarios:
// 1. When user provides tables in config (createIfNotExists: true)
// 2. When user uses existing publication without specifying tables (createIfNotExists: false)
func initializeSnapshot(ctx context.Context, cfg config.Config, tables publication.Tables, m metric.Metric) (*snapshot.Snapshotter, error) {
	if !cfg.Snapshot.Enabled {
		return nil, nil
	}
	return snapshot.New(ctx, cfg.Snapshot, tables, cfg.DSN(), m)
}

func (c *connector) Start(parent context.Context) (err error) {
	c.runMu.Lock()
	if c.runCancel != nil {
		c.runMu.Unlock()
		return ErrConnectorStarted
	}
	if c.stopped() {
		c.runMu.Unlock()
		return context.Canceled
	}
	signalCtx, stopSignals := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	ctx, cancel := context.WithCancelCause(signalCtx)
	done := make(chan struct{})
	c.runCancel = cancel
	c.runDone = done
	c.runMu.Unlock()
	defer func() {
		if c.stopped() || parent.Err() == nil && signalCtx.Err() != nil {
			err = nil
		} else if parent.Err() != nil {
			err = context.Cause(parent)
		}
		cancel(err)
		c.stopOnce.Do(func() { close(c.stopCh) })
		c.cleanup()
		close(done)
	}()

	if err := c.errIfStopped(ctx); err != nil {
		logger.Debug("connector start canceled before startup", "error", err)
		return err
	}

	go c.server.Listen()

	if c.cfg.IsSnapshotOnlyMode() {
		slotName := c.cfg.Snapshot.ID
		if slotName == "" {
			slotName = "snapshot_only_" + c.cfg.Database
		}
		takeSnapshot, err := c.shouldTakeSnapshot(ctx, slotName)
		if err != nil {
			return err
		}
		if !takeSnapshot {
			logger.Info("snapshot-only already completed, exiting")
			c.signalReady()
			return nil
		}

		logger.Info("starting snapshot-only execution", "slotName", slotName)
		if err := c.executeSnapshotWithRetry(ctx, slotName); err != nil {
			logger.Error("snapshot-only execution failed", "error", err)
			return fmt.Errorf("run snapshot: %w", err)
		}
		logger.Info("snapshot-only completed successfully, exiting")
		c.signalReady()
		return nil
	}

	takeSnapshot, err := c.shouldTakeSnapshot(ctx, c.cfg.Slot.Name)
	if err != nil {
		return err
	}
	if takeSnapshot {
		if err := c.prepareSnapshotAndSlot(ctx); err != nil {
			logger.Error("snapshot preparation failed", "error", err)
			return err
		}
	} else {
		logger.Info("creating replication slot for CDC")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			logger.Error("slot creation failed", "error", err)
			return err
		}
		logger.Info("slot info", "info", slotInfo)
	}

	if err := c.errIfStopped(ctx); err != nil {
		logger.Debug("connector start canceled before slot connect", "error", err)
		return err
	}

	if err := c.slot.Connect(ctx); err != nil {
		logger.Error("slot connection failed", "error", err)
		return err
	}

	if err := c.CaptureSlot(ctx); err != nil {
		logger.Error("capture slot failed", "error", err)
		return err
	}

	if err := c.errIfStopped(ctx); err != nil {
		logger.Debug("connector start canceled before stream connect", "error", err)
		return err
	}

	if err := c.stream.Connect(ctx); err != nil {
		logger.Error("stream connection failed", "error", err)
		return err
	}

	if err := c.openStream(ctx); err != nil {
		logger.Error("postgres stream open", "error", err)
		return err
	}

	logger.Info("slot captured")
	go c.slot.Metrics(ctx)

	if c.heartbeat != nil {
		go c.heartbeat.Run(ctx)
	}

	if c.timescaleDB != nil {
		go c.timescaleDB.SyncHyperTables(ctx)
	}

	c.signalReady()
	return c.waitUntilStopped(ctx, parent)
}

func (c *connector) waitUntilStopped(ctx, parent context.Context) error {
	if c.stopped() {
		return nil
	}
	select {
	case <-c.stopCh:
		logger.Debug("connector close requested")
		return nil
	case <-ctx.Done():
		if c.stopped() || parent.Err() == nil {
			return nil
		}
		cause := context.Cause(ctx)
		logger.Debug("context canceled", "error", cause)
		return cause
	case <-c.stream.Done():
		if c.stopped() {
			return nil
		}
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if err := c.stream.Err(); err != nil {
			return fmt.Errorf("replication stream stopped: %w", err)
		}
		return errors.New("replication stream stopped")
	}
}

func (c *connector) openStream(ctx context.Context) error {
	for {
		err := c.stream.Open(ctx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, replication.ErrorSlotInUse) {
			return err
		}

		logger.Info("replication slot is active; waiting before retrying stream open")
		if err := c.waitWithContext(ctx, time.Second); err != nil {
			return err
		}
		if err := c.errIfStopped(ctx); err != nil {
			return err
		}
	}
}

func (c *connector) errIfStopped(ctx context.Context) error {
	if c.stopped() {
		return context.Canceled
	}
	return context.Cause(ctx)
}

func (c *connector) stopped() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

func (c *connector) signalReady() {
	close(c.readyCh)
}

func (c *connector) shouldTakeSnapshot(ctx context.Context, slotName string) (bool, error) {
	if !c.cfg.Snapshot.Enabled || c.cfg.Snapshot.Mode == config.SnapshotModeNever {
		return false, nil
	}
	if err := c.snapshotter.EnsureMetadataTables(ctx); err != nil {
		return false, fmt.Errorf("initialize snapshot metadata: %w", err)
	}
	if c.cfg.Snapshot.Resnapshot {
		logger.Info("resnapshot enabled, reconciling requested generation", "slotName", slotName, "resnapshotID", c.cfg.Snapshot.ResnapshotID)
		shouldTakeSnapshot, err := c.snapshotter.ReconcileResnapshot(ctx, slotName)
		if err != nil {
			return false, fmt.Errorf("reconcile resnapshot: %w", err)
		}
		return shouldTakeSnapshot, nil
	}

	job, err := c.snapshotter.LoadJob(ctx, slotName)
	if err != nil {
		return false, fmt.Errorf("load snapshot job: %w", err)
	}
	return job == nil || !job.Completed, nil
}

// prepareSnapshotAndSlot creates the replication slot before exporting and processing the snapshot.
func (c *connector) prepareSnapshotAndSlot(ctx context.Context) error {
	if err := c.errIfStopped(ctx); err != nil {
		return err
	}

	slotInfo, err := c.slot.Create(ctx)
	if err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	logger.Debug("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

	if err := c.executeSnapshotWithRetry(ctx, c.cfg.Slot.Name); err != nil {
		return fmt.Errorf("run snapshot: %w", err)
	}

	logger.Info("snapshot completed successfully")
	return nil
}

// executeSnapshotWithRetry retries only when the exported snapshot was invalidated.
// Every retry prepares a fresh snapshot before replaying snapshot rows.
func (c *connector) executeSnapshotWithRetry(ctx context.Context, slotName string) error {
	const (
		maxRetries   = 5
		initialDelay = 10 * time.Second
		maxDelay     = 60 * time.Second
	)

	retryDelay := initialDelay
	for attempt := 1; ; attempt++ {
		err := c.snapshotter.Prepare(ctx, slotName)
		if err == nil {
			var snapshotLSN pq.LSN
			snapshotLSN, err = c.snapshotter.ExecuteWithLSN(ctx, c.snapshotHandler(ctx), slotName)
			if err == nil && !c.cfg.IsSnapshotOnlyMode() {
				c.stream.OpenFromSnapshotLSNAt(snapshotLSN)
			}
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, snapshot.ErrSnapshotInvalidated) {
			return err
		}
		if attempt == maxRetries {
			return fmt.Errorf("snapshot execution failed after maximum retries: %w", err)
		}

		logger.Warn("[snapshot] snapshot invalidated, retrying",
			"slot", slotName,
			"attempt", attempt,
			"maxRetries", maxRetries,
			"retryIn", retryDelay)
		if err := c.waitWithContext(ctx, retryDelay); err != nil {
			return err
		}
		retryDelay = min(retryDelay*2, maxDelay)
	}
}

// waitWithContext waits for duration or connector cancellation.
func (c *connector) waitWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.stopCh:
		return context.Canceled
	case <-timer.C:
		return nil
	}
}

func (c *connector) snapshotHandler(ctx context.Context) snapshot.Handler {
	return func(event *format.Snapshot) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		c.listenerFunc(&replication.ListenerContext{
			Message: event,
			Ack:     func() error { return nil },
		})
		return context.Cause(ctx)
	}
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	default:
	}

	select {
	case <-c.readyCh:
		return nil
	case <-c.stopCh:
	case <-ctx.Done():
	}

	select {
	case <-c.readyCh:
		return nil
	default:
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return context.Canceled
}

func (c *connector) Close() {
	c.stopOnce.Do(func() { close(c.stopCh) })

	c.runMu.Lock()
	runCancel := c.runCancel
	runDone := c.runDone
	c.runMu.Unlock()
	if runCancel == nil {
		c.cleanup()
		return
	}

	runCancel(context.Canceled)
	select {
	case <-runDone:
	case <-time.After(connectorShutdownTimeout):
		logger.Warn("timed out waiting for connector shutdown")
		c.cleanup()
	}
}

func (c *connector) cleanup() {
	c.cleanupOnce.Do(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), connectorShutdownTimeout)
		defer cancel()

		logger.Debug("[connector] closing connector")
		if c.stream != nil {
			c.stream.Close(cleanupCtx)
		}
		if c.snapshotter != nil {
			c.snapshotter.Close(cleanupCtx)
		}
		if c.heartbeat != nil {
			c.heartbeat.Close(cleanupCtx)
		}
		if c.timescaleDB != nil {
			c.timescaleDB.Close(cleanupCtx)
		}
		if c.slot != nil {
			c.slot.Close(cleanupCtx)
		}
		c.server.Shutdown()
		logger.Info("[connector] connector closed successfully")
	})
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func (c *connector) CaptureSlot(ctx context.Context) error {
	logger.Info("slot capturing...")
	for {
		info, err := c.slot.Info(ctx)
		if err == nil && !info.Active {
			logger.Debug("capture slot", "slotInfo", info)
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if errors.Is(err, slot.ErrorSlotClosed) {
				return nil
			}
			logger.Warn("slot info failed on capture slot", "error", err)
		}
		if err := c.waitWithContext(ctx, time.Second); err != nil {
			return err
		}
	}
}
