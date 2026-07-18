package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
)

// ChunkStatus represents the status of a chunk
type ChunkStatus string

const (
	ChunkStatusPending    ChunkStatus = "pending"
	ChunkStatusInProgress ChunkStatus = "in_progress"
	ChunkStatusCompleted  ChunkStatus = "completed"
)

// PartitionStrategy defines how a table is partitioned for snapshot
type PartitionStrategy string

const (
	PartitionStrategyIntegerRange PartitionStrategy = "integer_range" // Single integer PK - MIN/MAX range
	PartitionStrategyCTIDBlock    PartitionStrategy = "ctid_block"    // Physical block-based partitioning
	PartitionStrategyOffset       PartitionStrategy = "offset"        // LIMIT/OFFSET
)

// Chunk represents a unit of work for snapshot processing
type Chunk struct {
	ClaimedAt   *time.Time
	HeartbeatAt *time.Time
	CompletedAt *time.Time
	RangeEnd    *int64
	RangeStart  *int64

	// CTID block partitioning fields
	BlockStart *int64
	BlockEnd   *int64 // nil for the final unbounded chunk

	Status            ChunkStatus
	PartitionStrategy PartitionStrategy
	TableName         string
	ClaimedBy         string
	TableSchema       string
	SlotName          string
	TableColumns      []string
	ID                int64
	ChunkIndex        int
	ChunkStart        int64
	ChunkSize         int64
	IsLastChunk       bool
}

func (c *Chunk) hasRangeBounds() bool {
	return c.RangeStart != nil && c.RangeEnd != nil
}

func (c *Chunk) validatePartition() error {
	if c.ChunkSize <= 0 {
		return errors.New("chunk size must be positive")
	}
	if c.ChunkIndex < 0 || c.ChunkStart < 0 {
		return errors.New("chunk position must not be negative")
	}

	switch c.PartitionStrategy {
	case PartitionStrategyIntegerRange:
		if c.BlockStart != nil || c.BlockEnd != nil || c.IsLastChunk {
			return errors.New("integer-range chunk contains CTID metadata")
		}
		if (c.RangeStart == nil) != (c.RangeEnd == nil) {
			return errors.New("integer-range chunk has incomplete bounds")
		}
		if !c.hasRangeBounds() && (c.ChunkIndex != 0 || c.ChunkStart != 0) {
			return errors.New("empty integer-range chunk has inconsistent position")
		}
		if c.hasRangeBounds() && *c.RangeStart > *c.RangeEnd {
			return errors.New("integer-range chunk start exceeds end")
		}
		if c.hasRangeBounds() {
			maxEnd := *c.RangeStart + c.ChunkSize - 1
			if maxEnd >= *c.RangeStart && *c.RangeEnd > maxEnd {
				return errors.New("integer-range chunk exceeds its row limit")
			}
		}
	case PartitionStrategyCTIDBlock:
		if c.RangeStart != nil || c.RangeEnd != nil {
			return errors.New("CTID chunk contains integer-range metadata")
		}
		if c.BlockStart == nil {
			if c.BlockEnd != nil || c.IsLastChunk || c.ChunkIndex != 0 || c.ChunkStart != 0 {
				return errors.New("empty CTID chunk has inconsistent bounds")
			}
			return nil
		}
		if *c.BlockStart < 0 {
			return errors.New("CTID block start must not be negative")
		}
		if c.IsLastChunk {
			if c.BlockEnd != nil {
				return errors.New("last CTID chunk must not have an upper bound")
			}
		} else if c.BlockEnd == nil || *c.BlockEnd <= *c.BlockStart {
			return errors.New("bounded CTID chunk has invalid upper bound")
		}
	case PartitionStrategyOffset:
		if c.RangeStart != nil || c.RangeEnd != nil || c.BlockStart != nil || c.BlockEnd != nil || c.IsLastChunk {
			return errors.New("offset chunk contains range metadata")
		}
	default:
		return fmt.Errorf("unknown snapshot partition strategy %q", c.PartitionStrategy)
	}
	return nil
}

// Job represents the overall snapshot job metadata
type Job struct {
	StartedAt       time.Time
	SlotName        string
	SnapshotID      string
	ResnapshotID    string
	SnapshotLSN     pq.LSN
	TotalChunks     int
	CompletedChunks int
	Completed       bool
}

func (j *Job) sqlIdentity(alias string) string {
	if alias != "" {
		alias += "."
	}
	return fmt.Sprintf(
		"%sslot_name = %s AND %ssnapshot_id = %s",
		alias,
		pq.QuoteLiteral(j.SlotName),
		alias,
		pq.QuoteLiteral(j.SnapshotID),
	)
}

func (s *Snapshotter) validateJobRequest(job *Job) error {
	if s.config.Resnapshot && job.ResnapshotID != s.config.ResnapshotID {
		return fmt.Errorf("%w: expected %q, got %q", ErrResnapshotSuperseded, s.config.ResnapshotID, job.ResnapshotID)
	}
	return nil
}

func (j *Job) validate() error {
	if j.SlotName == "" || j.SnapshotID == "" {
		return errors.New("invalid snapshot job identity")
	}
	if j.SnapshotLSN == 0 {
		return errors.New("snapshot LSN must not be 0/0")
	}
	if j.StartedAt.IsZero() {
		return errors.New("snapshot job start time is required")
	}
	if j.TotalChunks <= 0 || j.CompletedChunks < 0 || j.CompletedChunks > j.TotalChunks {
		return errors.New("invalid snapshot job progress")
	}
	if j.Completed && j.CompletedChunks != j.TotalChunks {
		return errors.New("completed snapshot job has unfinished chunks")
	}
	return nil
}

const (
	jobTableName     = "cdc_snapshot_job"
	chunksTableName  = "cdc_snapshot_chunks"
	requestTableName = "cdc_snapshot_request"
)

func (s *Snapshotter) loadJob(ctx context.Context, slotName string) (*Job, error) {
	query := fmt.Sprintf(`
		SELECT slot_name, snapshot_id, resnapshot_id, snapshot_lsn, started_at,
		       completed, total_chunks, completed_chunks
		FROM %s WHERE slot_name = %s
	`, jobTableName, pq.QuoteLiteral(slotName))
	results, err := pq.ExecQuery(ctx, s.metadataConn, query)
	if err != nil {
		return nil, fmt.Errorf("load job: %w", err)
	}
	if len(results) != 1 {
		return nil, errors.New("invalid snapshot job result")
	}
	rows := results[0].Rows
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 {
		return nil, errors.New("invalid snapshot job row")
	}
	return parseJobRow(rows[0], slotName)
}

func parseJobRow(row [][]byte, slotName string) (*Job, error) {
	if len(row) != 8 {
		return nil, errors.New("invalid snapshot job row")
	}

	job := &Job{SlotName: string(row[0]), SnapshotID: string(row[1]), ResnapshotID: string(row[2])}
	if job.SlotName != slotName {
		return nil, errors.New("invalid snapshot job identity")
	}

	var err error
	job.SnapshotLSN, err = pq.ParseLSN(string(row[3]))
	if err != nil {
		return nil, fmt.Errorf("parse snapshot LSN: %w", err)
	}
	job.StartedAt, err = parseTimestamp(string(row[4]))
	if err != nil {
		return nil, fmt.Errorf("parse started_at timestamp: %w", err)
	}
	job.Completed, err = parsePostgresBool(row[5])
	if err != nil {
		return nil, fmt.Errorf("parse snapshot completion flag: %w", err)
	}
	job.TotalChunks, err = strconv.Atoi(string(row[6]))
	if err != nil {
		return nil, fmt.Errorf("parse total chunks: %w", err)
	}
	job.CompletedChunks, err = strconv.Atoi(string(row[7]))
	if err != nil {
		return nil, fmt.Errorf("parse completed chunks: %w", err)
	}
	if err := job.validate(); err != nil {
		return nil, err
	}
	return job, nil
}

// LoadJob is the public API for connector.
func (s *Snapshotter) LoadJob(ctx context.Context, slotName string) (*Job, error) {
	return s.loadJob(ctx, slotName)
}

func (s *Snapshotter) checkJobCompleted(ctx context.Context, expected *Job) (bool, error) {
	job, err := s.loadJob(ctx, expected.SlotName)
	if err != nil {
		return false, err
	}
	if job == nil {
		return false, ErrSnapshotInvalidated
	}
	if job.SnapshotID != expected.SnapshotID {
		return false, ErrSnapshotInvalidated
	}
	return job.CompletedChunks == job.TotalChunks, nil
}

func parseTimestamp(s string) (time.Time, error) {
	if t, err := time.Parse(postgresTimestampFormatMicros, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(postgresTimestampFormat, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp %q", s)
}
