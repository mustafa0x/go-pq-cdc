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

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/heartbeat"
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
	slot               *slot.Slot
	readyCh            chan struct{}
	cfg                *config.Config
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

	hb, err := validateHeartbeat(ctx, cfg, conn)
	if err != nil {
		return nil, err
	}

	capturePlan, err := cfg.CapturePlan(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("compile capture plan: %w", err)
	}
	cfg.Publication = capturePlan.PublicationConfig(cfg.Publication.Name, cfg.Publication.CreateIfNotExists)
	publicationInfo, err := publication.New(cfg.Publication, conn).Create(ctx)
	if err != nil {
		return nil, fmt.Errorf("create or inspect publication: %w", err)
	}
	if err := capturePlan.Validate(ctx, conn, cfg.Publication.Name); err != nil {
		return nil, err
	}
	if err := cfg.ValidateHeartbeatInPublication(publicationInfo); err != nil {
		return nil, err
	}
	logger.Info("publication", "info", publicationInfo)

	m := metric.NewMetric(cfg.Slot.Name)

	stream, ok := replication.NewStream(cfg.ReplicationDSN(), cfg, capturePlan, m, listenerFunc).(runningStreamer)
	if !ok {
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
		readyCh:            make(chan struct{}),
		stopCh:             make(chan struct{}),
	}

	return c, nil
}

func validateHeartbeat(ctx context.Context, cfg config.Config, conn pq.Connection) (*heartbeat.Heartbeat, error) {
	if !cfg.IsHeartbeatEnabled() {
		return nil, nil
	}

	hb := heartbeat.New(cfg.DSN(), cfg.Heartbeat)
	if err := hb.ValidateTable(ctx, conn); err != nil {
		return nil, fmt.Errorf("validate heartbeat table: %w", err)
	}
	return hb, nil
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

	logger.Info("creating or adopting replication slot")
	slotInfo, err := c.slot.Create(ctx)
	if err != nil {
		return fmt.Errorf("create replication slot: %w", err)
	}
	logger.Info("slot info", "info", slotInfo)

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
		if c.heartbeat != nil {
			c.heartbeat.Close(cleanupCtx)
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
