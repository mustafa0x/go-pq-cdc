package integration

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/capture"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/require"
)

func TestCapturePlanExcludesIndexIncludeColumnsFromKeys(t *testing.T) {
	ctx := context.Background()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pgExec(ctx, conn, "DROP TABLE IF EXISTS capture_include_identity")
		_ = pgExec(ctx, conn, "DROP TABLE IF EXISTS capture_include_primary")
		_ = conn.Close(ctx)
	})

	require.NoError(t, pgExec(ctx, conn, `
		CREATE TABLE capture_include_primary (
			id integer NOT NULL,
			payload text,
			PRIMARY KEY (id) INCLUDE (payload)
		);
		CREATE TABLE capture_include_identity (id integer NOT NULL, payload text);
		CREATE UNIQUE INDEX capture_include_identity_key
			ON capture_include_identity (id) INCLUDE (payload);
		ALTER TABLE capture_include_identity
			REPLICA IDENTITY USING INDEX capture_include_identity_key;
	`))

	_, err = capture.Compile(ctx, conn, capture.Spec{
		Relations: []capture.RelationSpec{{
			Name:             "capture_include_primary",
			ColumnsSpecified: true,
			Columns:          []capture.ColumnSpec{{Name: "id"}},
			ReplicaIdentity:  publication.ReplicaIdentityDefault,
		}},
		Operations: publication.Operations{
			publication.OperationUpdate,
			publication.OperationDelete,
		},
	})
	require.NoError(t, err)

	_, err = capture.Compile(ctx, conn, capture.Spec{
		Relations: []capture.RelationSpec{{
			Name:             "capture_include_identity",
			ColumnsSpecified: true,
			Columns:          []capture.ColumnSpec{{Name: "id"}},
			ReplicaIdentity:  publication.ReplicaIdentityUsingIndex,
		}},
		Operations: publication.Operations{
			publication.OperationUpdate,
			publication.OperationDelete,
		},
	})
	require.NoError(t, err)
}

func TestCaptureScanMatchesPlannedInheritanceDomain(t *testing.T) {
	ctx := context.Background()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pgExec(ctx, conn, "ROLLBACK")
		_ = pgExec(ctx, conn, "DROP TABLE IF EXISTS capture_scan_root, capture_scan_parent CASCADE")
		_ = conn.Close(ctx)
	})

	require.NoError(t, pgExec(ctx, conn, `
		CREATE TABLE capture_scan_parent (id integer);
		CREATE TABLE capture_scan_child () INHERITS (capture_scan_parent);
		INSERT INTO capture_scan_parent VALUES (1);
		INSERT INTO capture_scan_child VALUES (2);
		CREATE TABLE capture_scan_root (id integer) PARTITION BY RANGE (id);
		CREATE TABLE capture_scan_partition PARTITION OF capture_scan_root FOR VALUES FROM (0) TO (10);
		INSERT INTO capture_scan_root VALUES (3);
	`))
	plan, err := capture.Compile(ctx, conn, capture.Spec{
		Relations: []capture.RelationSpec{
			{Name: "capture_scan_parent"},
			{Name: "capture_scan_child"},
			{Name: "capture_scan_root"},
		},
		Operations:              publication.Operations{publication.OperationInsert},
		PublishViaPartitionRoot: true,
		RequireUnfiltered:       true,
	})
	require.NoError(t, err)
	require.NoError(t, pgExec(ctx, conn, `
		CREATE TABLE capture_scan_partition_2 PARTITION OF capture_scan_root FOR VALUES FROM (10) TO (20)
	`))
	changed, err := capture.Compile(ctx, conn, capture.Spec{
		Relations: []capture.RelationSpec{
			{Name: "capture_scan_parent"},
			{Name: "capture_scan_child"},
			{Name: "capture_scan_root"},
		},
		Operations:              publication.Operations{publication.OperationInsert},
		PublishViaPartitionRoot: true,
		RequireUnfiltered:       true,
	})
	require.NoError(t, err)
	require.NotEqual(t, plan.Hash(), changed.Hash())
	_, err = capture.Compile(ctx, conn, capture.Spec{
		Relations:               []capture.RelationSpec{{Name: "capture_scan_root"}},
		Operations:              publication.Operations{publication.OperationTruncate},
		PublishViaPartitionRoot: true,
		RequireUnfiltered:       true,
	})
	require.ErrorContains(t, err, "cannot publish TRUNCATE through a partition root")
	require.NoError(t, pgExec(ctx, conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))

	rows := map[string][]any{}
	require.NoError(t, plan.Scan(ctx, conn, 10, func(batch capture.Batch) error {
		for _, row := range batch.Rows {
			rows[batch.Table] = append(rows[batch.Table], row["id"])
		}
		return nil
	}))
	require.Equal(t, []any{"1"}, rows["capture_scan_parent"])
	require.Equal(t, []any{"2"}, rows["capture_scan_child"])
	require.Equal(t, []any{"3"}, rows["capture_scan_root"])
}

func TestCapturePlanRejectsOverlappingInheritanceRelations(t *testing.T) {
	ctx := context.Background()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pgExec(ctx, conn, "DROP TABLE IF EXISTS capture_overlap_root CASCADE")
		_ = conn.Close(ctx)
	})
	require.NoError(t, pgExec(ctx, conn, `
		CREATE TABLE capture_overlap_root (id integer) PARTITION BY RANGE (id);
		CREATE TABLE capture_overlap_child PARTITION OF capture_overlap_root FOR VALUES FROM (0) TO (10);
	`))

	_, err = capture.Compile(ctx, conn, capture.Spec{
		Relations: []capture.RelationSpec{
			{Name: "capture_overlap_root"},
			{Name: "capture_overlap_child"},
		},
		Operations:              publication.Operations{publication.OperationInsert},
		PublishViaPartitionRoot: true,
		RequireUnfiltered:       true,
	})
	require.ErrorContains(t, err, "overlap through inheritance")
}

func TestCapturePlanValidateReinspectsSourceRelations(t *testing.T) {
	ctx := context.Background()
	conn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pgExec(ctx, conn, "DROP PUBLICATION IF EXISTS capture_validate_publication")
		_ = pgExec(ctx, conn, "DROP TABLE IF EXISTS capture_validate_source")
		_ = conn.Close(ctx)
	})
	require.NoError(t, pgExec(ctx, conn, `
		CREATE TABLE capture_validate_source (id integer);
		CREATE PUBLICATION capture_validate_publication
			FOR TABLE capture_validate_source
			WITH (publish = 'insert');
	`))
	plan, err := capture.Compile(ctx, conn, capture.Spec{
		PublicationName: "capture_validate_publication",
		Relations:       []capture.RelationSpec{{Name: "capture_validate_source"}},
		Operations:      publication.Operations{publication.OperationInsert},
	})
	require.NoError(t, err)
	require.NoError(t, pgExec(ctx, conn, "ALTER TABLE capture_validate_source ALTER COLUMN id TYPE bigint"))
	require.Error(t, plan.Validate(ctx, conn, "capture_validate_publication"))
}
