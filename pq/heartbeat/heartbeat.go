package heartbeat

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
)

// quoteIdentifier quotes a PostgreSQL identifier (schema, table, column name)
// to handle reserved words, special characters, and embedded quotes safely.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Heartbeat manages periodic heartbeat updates to prevent WAL bloat
type Heartbeat struct {
	conn   pq.Connection
	dsn    string
	cfg    config.HeartbeatConfig
	mu     sync.Mutex
	closed bool
}

// New creates a new Heartbeat instance
func New(dsn string, cfg config.HeartbeatConfig) *Heartbeat {
	return &Heartbeat{
		dsn: dsn,
		cfg: cfg,
	}
}

// ValidateTable verifies the pre-provisioned heartbeat relation without
// mutating the source database. Startup must finish publication and capture
// validation before the heartbeat loop is allowed to write.
func (h *Heartbeat) ValidateTable(ctx context.Context, conn pq.Connection) error {
	exists, err := pq.TableExists(ctx, conn, h.cfg.Table.Schema, h.cfg.Table.Name)
	if err != nil {
		return fmt.Errorf("check heartbeat table existence: %w", err)
	}
	if !exists {
		return fmt.Errorf(
			"heartbeat table %s.%s does not exist; create it before starting the connector",
			h.cfg.Table.Schema,
			h.cfg.Table.Name,
		)
	}

	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	if err := pq.ExecSQL(ctx, conn, "EXPLAIN "+h.query()); err != nil {
		return fmt.Errorf("validate heartbeat table contract: %w", err)
	}

	logger.Info("heartbeat table validated", "table", schema+"."+table)
	return nil
}

// Run starts the heartbeat loop. It blocks until context is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	logger.Debug("heartbeat loop started",
		"interval", h.cfg.Interval,
		"table", h.cfg.Table.Schema+"."+h.cfg.Table.Name)

	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("heartbeat loop stopped", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := h.execute(ctx); err != nil {
				logger.Error("heartbeat execution failed", "error", err)
			} else {
				logger.Debug("heartbeat query executed")
			}
		}
	}
}

// execute runs a single heartbeat update
func (h *Heartbeat) execute(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}

	// Lazily (re)establish connection if needed
	if h.conn == nil {
		conn, err := pq.NewConnection(ctx, h.dsn)
		if err != nil {
			return fmt.Errorf("heartbeat connection (re)establish failed: %w", err)
		}
		h.conn = conn
	}

	query := h.query()
	resultReader := h.conn.Exec(ctx, query)
	if resultReader == nil {
		return fmt.Errorf("heartbeat exec returned nil resultReader")
	}

	_, readErr := resultReader.ReadAll()
	closeErr := resultReader.Close()
	if readErr != nil || closeErr != nil {
		// On error, proactively close and nil the connection so that the next
		// heartbeat tick will try to re-establish it.
		_ = h.conn.Close(ctx)
		h.conn = nil
		if readErr != nil {
			return fmt.Errorf("heartbeat query failed: %w", readErr)
		}
		return fmt.Errorf("close heartbeat result reader: %w", closeErr)
	}

	return nil
}

// query returns the auto-generated UPDATE query for heartbeat
func (h *Heartbeat) query() string {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	return fmt.Sprintf(`INSERT INTO %s.%s (id, last_heartbeat) VALUES (1, NOW())
		ON CONFLICT (id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat`, schema, table)
}

// Close closes the heartbeat connection
func (h *Heartbeat) Close(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.closed = true
	if h.conn != nil {
		_ = h.conn.Close(ctx)
		h.conn = nil
	}
}
