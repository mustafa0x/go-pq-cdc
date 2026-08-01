package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/capture"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

const defaultSchema = "public"

type Config struct {
	Logger      LoggerConfig       `json:"logger" yaml:"logger"`
	Host        string             `json:"host" yaml:"host"`
	Username    string             `json:"username" yaml:"username"`
	Password    string             `json:"password" yaml:"password"`
	Database    string             `json:"database" yaml:"database"`
	Publication publication.Config `json:"publication" yaml:"publication"`
	Heartbeat   HeartbeatConfig    `json:"heartbeat" yaml:"heartbeat"`
	Listener    ListenerConfig     `json:"listener" yaml:"listener"`
	Slot        slot.Config        `json:"slot" yaml:"slot"`
	Port        int                `json:"port" yaml:"port"`
	Metric      MetricConfig       `json:"metric" yaml:"metric"`
	DebugMode   bool               `json:"debugMode" yaml:"debugMode"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	Logger   logger.Logger `json:"-" yaml:"-"`         // custom logger
	LogLevel slog.Level    `json:"level" yaml:"level"` // if custom logger is nil, set the slog log level
}

type HeartbeatConfig struct {
	Table    publication.Table `json:"table" yaml:"table"`
	Interval time.Duration     `json:"interval" yaml:"interval"`
}

type ListenerConfig struct {
	EmitTransactionBoundaries bool `json:"emitTransactionBoundaries" yaml:"emitTransactionBoundaries"`
}

// DSN returns a normal PostgreSQL connection string for regular database operations
// (publication, heartbeat, and source inspection).
func (c *Config) DSN() string {
	return c.buildDSN(false, false)
}

// ReplicationDSN returns a replication connection string for CDC streaming
// This connection counts against max_wal_senders limit
func (c *Config) ReplicationDSN() string {
	return c.buildDSN(true, false)
}

func (c *Config) DSNWithoutSSL() string {
	return c.buildDSN(false, true)
}

func (c *Config) buildDSN(replication, disableSSL bool) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   c.Database,
	}

	q := u.Query()
	if replication {
		q.Set("replication", "database")
	}
	if disableSSL {
		q.Set("sslmode", "disable")
	}
	u.RawQuery = q.Encode()

	return u.String()
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 5432
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}

	// Default heartbeat interval when table is configured
	if c.Heartbeat.Table.Name != "" {
		if c.Heartbeat.Interval == 0 {
			c.Heartbeat.Interval = 100 * time.Millisecond
		}
		if c.Heartbeat.Table.Schema == "" {
			c.Heartbeat.Table.Schema = defaultSchema
		}
	}

	c.Slot.SlotActivityCheckerInterval = normalizeMillisecondsDuration(c.Slot.SlotActivityCheckerInterval, time.Second)

	if c.Slot.ProtoVersion == 0 {
		c.Slot.ProtoVersion = 1
	}

	if c.Logger.Logger == nil {
		c.Logger.Logger = logger.NewSlog(c.Logger.LogLevel)
	}

	// Own configured publication intent before applying defaults so the
	// connector never aliases caller-owned table or column slices.
	tables := make(publication.Tables, len(c.Publication.Tables))
	for i, table := range c.Publication.Tables {
		table.Columns = append([]string(nil), table.Columns...)
		tables[i] = table
	}
	c.Publication.Tables = tables
	c.Publication.Operations = append(publication.Operations(nil), c.Publication.Operations...)

	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" {
			c.Publication.Tables[tableID].Schema = defaultSchema
		}
		if len(table.Columns) > 0 {
			c.Publication.Tables[tableID].ColumnsSpecified = true
		}
		if table.Partitioned {
			c.Publication.PublishViaPartitionRoot = true
		}
	}

}

// IsHeartbeatEnabled returns true if heartbeat table is configured
func (c *Config) IsHeartbeatEnabled() bool {
	return c.Heartbeat.Table.Name != ""
}

// CapturePlan compiles the configured static source contract before any
// publication mutation. The returned immutable plan is then the only value
// used to create or validate the managed publication and to decode CDC.
func (c *Config) CapturePlan(ctx context.Context, conn pq.Connection) (*capture.Plan, error) {
	if c.Publication.AllTables || c.Publication.SchemaTables {
		return nil, errors.New("dynamic FOR ALL TABLES and schema publications are not supported by a static capture plan")
	}
	if len(c.Publication.Tables) == 0 {
		return nil, errors.New("publication has no explicit tables")
	}
	return capture.Compile(ctx, conn, capture.Spec{
		Relations:               capture.RelationsFromConfig(c.Publication.Tables),
		Operations:              append(publication.Operations(nil), c.Publication.Operations...),
		PublishViaPartitionRoot: c.Publication.PublishViaPartitionRoot,
	})
}

func (c *Config) ValidateHeartbeatInPublication(pubInfo *publication.Config) error {
	if !c.IsHeartbeatEnabled() || pubInfo == nil {
		return nil
	}

	if c.Publication.AllTables || pubInfo.AllTables {
		return nil
	}

	schema := c.Heartbeat.Table.Schema
	if schema == "" {
		schema = defaultSchema
	}
	name := c.Heartbeat.Table.Name
	if !pubInfo.Tables.Contains(schema, name) {
		return fmt.Errorf(
			"heartbeat table %s.%s is not included in publication %q; add it to publication.tables so heartbeat changes reach the replication slot",
			schema, name, pubInfo.Name,
		)
	}

	return nil
}

func (c *Config) Validate() error {
	var err error
	if isEmpty(c.Host) {
		err = errors.Join(err, errors.New("host cannot be empty"))
	}

	if isEmpty(c.Username) {
		err = errors.Join(err, errors.New("username cannot be empty"))
	}

	if isEmpty(c.Password) {
		err = errors.Join(err, errors.New("password cannot be empty"))
	}

	if isEmpty(c.Database) {
		err = errors.Join(err, errors.New("database cannot be empty"))
	}

	if cErr := c.Publication.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	slotConfig := c.Slot
	slotConfig.SlotActivityCheckerInterval = normalizeMillisecondsDuration(slotConfig.SlotActivityCheckerInterval, 0)
	if cErr := slotConfig.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	if c.IsHeartbeatEnabled() {
		if c.Heartbeat.Interval <= 0 {
			err = errors.Join(err, errors.New("heartbeat.interval must be greater than 0 when heartbeat table is configured"))
		}
		if !c.Publication.Operations.Contains(publication.OperationUpdate) {
			err = errors.Join(err, errors.New("publication.operations must include UPDATE when heartbeat is configured"))
		}
		if !c.Publication.AllTables && len(c.Publication.Tables) > 0 {
			if hErr := c.ValidateHeartbeatInPublication(&publication.Config{
				Name:   c.Publication.Name,
				Tables: c.Publication.Tables,
			}); hErr != nil {
				err = errors.Join(err, hErr)
			}
		}
	}

	return err
}

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	b, _ := json.Marshal(cfg)
	logger.Info("used config", "config", string(b))
}

func normalizeMillisecondsDuration(value, defaultValue time.Duration) time.Duration {
	if value == 0 {
		return defaultValue
	}

	// Backward compatibility: earlier configs commonly used bare integers as
	// milliseconds because the slot package multiplied this value by time.Millisecond.
	// Duration strings such as "3s" are already parsed as durations and must not be
	// multiplied again.
	if value > 0 && value < time.Millisecond {
		return value * time.Millisecond
	}

	return value
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
