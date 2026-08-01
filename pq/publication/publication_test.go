package publication

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestDecodePublicationInfoPreservesSourceContract(t *testing.T) {
	fields := []pgconn.FieldDescription{
		{Name: "pubname", DataTypeOID: pgtype.TextOID},
		{Name: "puballtables", DataTypeOID: pgtype.BoolOID},
		{Name: "pubinsert", DataTypeOID: pgtype.BoolOID},
		{Name: "pubupdate", DataTypeOID: pgtype.BoolOID},
		{Name: "pubdelete", DataTypeOID: pgtype.BoolOID},
		{Name: "pubtruncate", DataTypeOID: pgtype.BoolOID},
		{Name: "pubviaroot", DataTypeOID: pgtype.BoolOID},
		{Name: "pubschemas", DataTypeOID: pgtype.BoolOID},
		{Name: "schemaname", DataTypeOID: pgtype.TextOID},
		{Name: "tablename", DataTypeOID: pgtype.TextOID},
		{Name: "columns", DataTypeOID: pgtype.TextArrayOID},
		{Name: "columns_specified", DataTypeOID: pgtype.BoolOID},
		{Name: "row_filter", DataTypeOID: pgtype.TextOID},
		{Name: "replica_identity", DataTypeOID: pgtype.TextOID},
		{Name: "replica_identity_index", DataTypeOID: pgtype.TextOID},
	}
	result := &pgconn.Result{FieldDescriptions: fields, Rows: [][][]byte{
		{
			[]byte("pub"), []byte("f"), []byte("t"), []byte("t"), []byte("t"), []byte("t"), []byte("f"), []byte("f"),
			[]byte("public"), []byte("users"), []byte("{id,name}"), []byte("t"), []byte("(id > 10)"), []byte("i"), []byte("users_identity"),
		},
		{
			[]byte("pub"), []byte("f"), []byte("t"), []byte("t"), []byte("t"), []byte("t"), []byte("f"), []byte("f"),
			[]byte("public"), []byte("events"), []byte("{id,payload}"), []byte("f"), []byte(""), []byte("d"), []byte(""),
		},
	}}

	info, err := decodePublicationInfoResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Operations) != 4 || len(info.Tables) != 2 {
		t.Fatalf("decoded publication = %#v", info)
	}
	users := info.Tables[0]
	if users.Schema != "public" || users.Name != "users" || len(users.Columns) != 2 || !users.ColumnsSpecified || users.RowFilter != "(id > 10)" || users.ReplicaIdentity != ReplicaIdentityUsingIndex || users.ReplicaIdentityIndex != "users_identity" {
		t.Fatalf("decoded users table = %#v", users)
	}
	if info.Tables[1].ColumnsSpecified || len(info.Tables[1].Columns) != 2 {
		t.Fatalf("decoded unrestricted table = %#v", info.Tables[1])
	}
}

func TestCreateQueryUsesExplicitColumnsAndOperations(t *testing.T) {
	cfg := Config{
		Name: "pub",
		Operations: Operations{
			OperationInsert,
			OperationUpdate,
			OperationDelete,
			OperationTruncate,
		},
		Tables: Tables{{
			Schema:  "public",
			Name:    "events",
			Columns: []string{"id", "name"},
		}},
	}
	query := cfg.createQuery()
	for _, expected := range []string{
		`FOR TABLE ONLY "public"."events"("id", "name")`,
		`publish = 'insert, update, delete, truncate'`,
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("create query %q does not contain %q", query, expected)
		}
	}
}

func TestCreateQueryExpandsOnlyPartitionRoots(t *testing.T) {
	query := (Config{
		Name:       "pub",
		Operations: Operations{OperationInsert},
		Tables: Tables{
			{Schema: "public", Name: "parent"},
			{Schema: "public", Name: "root", Partitioned: true},
		},
	}).createQuery()
	if !strings.Contains(query, `FOR TABLE ONLY "public"."parent", "public"."root"`) {
		t.Fatalf("create query has incorrect inheritance scope: %q", query)
	}
}

func TestConfigValidateRequiresStaticContractWhenAdopting(t *testing.T) {
	cfg := Config{Name: "events", CreateIfNotExists: false}
	if err := cfg.Validate(); err == nil {
		t.Fatal("publication adoption without a static table/operation contract was accepted")
	}
}
