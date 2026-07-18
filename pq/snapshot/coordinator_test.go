package snapshot

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrInt64(v int64) *int64 { return &v }

func TestAndCondition(t *testing.T) {
	t.Run("both empty returns empty", func(t *testing.T) {
		assert.Empty(t, andCondition("", ""))
	})
	t.Run("only existing returns existing", func(t *testing.T) {
		assert.Equal(t, "a = 1", andCondition("a = 1", ""))
	})
	t.Run("only extra returns parenthesized", func(t *testing.T) {
		assert.Equal(t, "(b = 2)", andCondition("", "b = 2"))
	})
	t.Run("combines with AND and parenthesizes extra for OR precedence", func(t *testing.T) {
		assert.Equal(t, "a = 1 AND (b = 2)", andCondition("a = 1", "b = 2"))
	})
}

func TestGetQueryCondition(t *testing.T) {
	newSnapshotter := func(cfg config.SnapshotConfig, tables publication.Tables) *Snapshotter {
		return &Snapshotter{config: cfg, tables: tables}
	}

	t.Run("returns empty when no conditions defined", func(t *testing.T) {
		s := newSnapshotter(config.SnapshotConfig{}, publication.Tables{
			{Name: "users", Schema: "public"},
		})
		assert.Empty(t, s.getQueryCondition("public", "users"))
	})

	t.Run("returns global condition when no per-table override", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{{Name: "users", Schema: "public"}},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
	})

	t.Run("per-table condition overrides global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: "deleted_at IS NULL"},
			},
		)
		assert.Equal(t, "deleted_at IS NULL", s.getQueryCondition("public", "users"))
	})

	t.Run("empty per-table falls back to global", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "is_active = true"},
			publication.Tables{
				{Name: "users", Schema: "public", QueryCondition: ""},
				{Name: "orders", Schema: "public", QueryCondition: "status <> 'cancelled'"},
			},
		)
		assert.Equal(t, "is_active = true", s.getQueryCondition("public", "users"))
		assert.Equal(t, "status <> 'cancelled'", s.getQueryCondition("public", "orders"))
	})

	t.Run("empty schema is normalized to public", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{},
			publication.Tables{
				{Name: "users", Schema: "", QueryCondition: "active"},
			},
		)
		assert.Equal(t, "active", s.getQueryCondition("", "users"))
		assert.Equal(t, "active", s.getQueryCondition("public", "users"))
	})

	t.Run("different schema does not match", func(t *testing.T) {
		s := newSnapshotter(
			config.SnapshotConfig{QueryCondition: "global_cond"},
			publication.Tables{
				{Name: "users", Schema: "tenant_a", QueryCondition: "table_cond"},
			},
		)
		assert.Equal(t, "global_cond", s.getQueryCondition("public", "users"))
		assert.Equal(t, "table_cond", s.getQueryCondition("tenant_a", "users"))
	})
}

func requireChunkQuery(t *testing.T, s *Snapshotter, chunk *Chunk, orderBy string, pkColumns []string, condition string) string {
	t.Helper()
	query, err := s.buildChunkQuery(chunk, orderBy, pkColumns, condition)
	require.NoError(t, err)
	return query
}

func TestBuildChunkQueryWithCondition(t *testing.T) {
	s := &Snapshotter{}

	t.Run("integer range injects condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(500),
			ChunkSize:         500,
		}
		query := requireChunkQuery(t, s, chunk, "id", []string{"id"}, "is_active = true")
		assert.Contains(t, query, `WHERE "id" >= 1 AND "id" <= 500 AND (is_active = true)`)
		assert.Contains(t, query, "ORDER BY id LIMIT 500")
	})

	t.Run("integer range quotes identifiers", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(5),
			ChunkSize:         5,
		}
		query := requireChunkQuery(t, s, chunk, "id", []string{"id"}, "")
		assert.Equal(t, `SELECT * FROM "public"."users" WHERE "id" >= 1 AND "id" <= 5 ORDER BY id LIMIT 5`, query)
	})

	t.Run("offset applies condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
			ChunkStart:        200,
		}
		query := requireChunkQuery(t, s, chunk, "id", nil, "status = 'active'")
		assert.Contains(t, query, "WHERE (status = 'active')")
		assert.Contains(t, query, "ORDER BY id LIMIT 100 OFFSET 200")
	})

	t.Run("condition preserves OR precedence", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			RangeStart:        ptrInt64(1),
			RangeEnd:          ptrInt64(500),
			ChunkSize:         500,
		}
		query := requireChunkQuery(t, s, chunk, "id", []string{"id"}, "status = 'a' OR status = 'b'")
		assert.Contains(t, query, "AND (status = 'a' OR status = 'b')")
	})

	t.Run("offset without condition omits WHERE", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "orders",
			PartitionStrategy: PartitionStrategyOffset,
			ChunkSize:         100,
		}
		assert.NotContains(t, requireChunkQuery(t, s, chunk, "id", nil, ""), "WHERE")
	})

	t.Run("bounded CTID applies condition", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(0),
			BlockEnd:          ptrInt64(100),
			ChunkSize:         100,
		}
		query := requireChunkQuery(t, s, chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, query, "WHERE ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("last CTID chunk has no upper bound", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			BlockStart:        ptrInt64(50),
			IsLastChunk:       true,
			ChunkSize:         100,
		}
		query := requireChunkQuery(t, s, chunk, "", nil, "tenant_id = 7")
		assert.Contains(t, query, "WHERE ctid >= '(50,0)'::tid AND (tenant_id = 7)")
	})

	t.Run("empty CTID table selects all visible rows", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "events",
			PartitionStrategy: PartitionStrategyCTIDBlock,
			ChunkSize:         100,
		}
		assert.Contains(t, requireChunkQuery(t, s, chunk, "", nil, "tenant_id = 7"), `FROM "public"."events" WHERE (tenant_id = 7)`)
		assert.NotContains(t, requireChunkQuery(t, s, chunk, "", nil, ""), "WHERE")
	})

	t.Run("empty integer range is guaranteed empty", func(t *testing.T) {
		chunk := &Chunk{
			TableSchema:       "public",
			TableName:         "users",
			PartitionStrategy: PartitionStrategyIntegerRange,
			ChunkSize:         10,
		}
		query := requireChunkQuery(t, s, chunk, "id", []string{"id"}, "is_active = true")
		assert.Equal(t, `SELECT * FROM "public"."users" WHERE FALSE`, query)
	})
}

func TestBuildChunkQueryRejectsInvalidMetadata(t *testing.T) {
	s := &Snapshotter{}
	tests := []struct {
		name      string
		chunk     *Chunk
		orderBy   string
		pkColumns []string
	}{
		{
			name: "incomplete integer bounds",
			chunk: &Chunk{
				PartitionStrategy: PartitionStrategyIntegerRange,
				RangeStart:        ptrInt64(1),
				ChunkSize:         10,
			},
			orderBy: "id",
		},
		{
			name: "integer range wider than chunk size",
			chunk: &Chunk{
				PartitionStrategy: PartitionStrategyIntegerRange,
				RangeStart:        ptrInt64(1),
				RangeEnd:          ptrInt64(11),
				ChunkSize:         10,
			},
			orderBy:   "id",
			pkColumns: []string{"id"},
		},
		{
			name: "bounded integer chunk without one primary key",
			chunk: &Chunk{
				PartitionStrategy: PartitionStrategyIntegerRange,
				RangeStart:        ptrInt64(1),
				RangeEnd:          ptrInt64(10),
				ChunkSize:         10,
			},
			orderBy: "id",
		},
		{
			name: "bounded CTID chunk without upper bound",
			chunk: &Chunk{
				PartitionStrategy: PartitionStrategyCTIDBlock,
				BlockStart:        ptrInt64(1),
				ChunkSize:         10,
			},
		},
		{
			name: "unknown strategy",
			chunk: &Chunk{
				PartitionStrategy: PartitionStrategy("mystery"),
				ChunkSize:         10,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := s.buildChunkQuery(test.chunk, test.orderBy, test.pkColumns, "")
			require.Error(t, err)
		})
	}
}

func TestParseRowRejectsMismatchedShape(t *testing.T) {
	s := &Snapshotter{}
	_, err := s.parseRow([]pgconn.FieldDescription{{Name: "id"}}, nil)
	require.Error(t, err)
}

func TestParsePrimaryKeyColumnsValidatesMetadata(t *testing.T) {
	results := []*pgconn.Result{{Rows: [][][]byte{
		{[]byte("tenant_id"), []byte("BIGINT")},
		{[]byte("id"), []byte("integer")},
	}}}
	columns, err := parsePrimaryKeyColumns(results)
	require.NoError(t, err)
	assert.Equal(t, []primaryKeyColumn{
		{Name: "tenant_id", DataType: "bigint"},
		{Name: "id", DataType: "integer"},
	}, columns)

	for _, test := range []struct {
		name    string
		results []*pgconn.Result
	}{
		{name: "multiple results", results: []*pgconn.Result{{}, {}}},
		{name: "missing name", results: []*pgconn.Result{{Rows: [][][]byte{{nil, []byte("bigint")}}}}},
		{name: "missing type", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("id"), nil}}}}},
		{name: "unexpected field", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("id")}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePrimaryKeyColumns(test.results); err == nil {
				t.Fatal("parsePrimaryKeyColumns() accepted invalid metadata")
			}
		})
	}
}

func TestParseSnapshotCheckpoint(t *testing.T) {
	valid := []*pgconn.Result{{Rows: [][][]byte{{[]byte("0/10")}}}}
	lsn, err := parseSnapshotCheckpoint(valid)
	require.NoError(t, err)
	assert.Equal(t, "0/10", lsn.String())

	for _, test := range []struct {
		name    string
		results []*pgconn.Result
	}{
		{name: "missing result"},
		{name: "malformed", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("bad")}}}}},
		{name: "zero", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("0/0")}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSnapshotCheckpoint(test.results)
			require.Error(t, err)
		})
	}
}
