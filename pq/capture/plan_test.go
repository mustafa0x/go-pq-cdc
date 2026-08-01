package capture

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

func TestCompileRejectsNonCanonicalOperationsBeforeSourceInspection(t *testing.T) {
	_, err := Compile(context.Background(), nil, Spec{
		Relations:  []RelationSpec{{Name: "users"}},
		Operations: publication.Operations{publication.OperationInsert, publication.OperationInsert},
	})
	if err == nil {
		t.Fatal("duplicate publication operations were accepted")
	}
}

func TestNormalizeRelationsOwnsInput(t *testing.T) {
	columns := []ColumnSpec{{Name: "id", Type: "integer"}, {Name: "name", Type: "text"}}
	relations := []RelationSpec{{
		Name:             "users",
		Columns:          columns,
		ColumnsSpecified: true,
		ReplicaIdentity:  publication.ReplicaIdentityDefault,
		PrimaryKey:       "id",
	}}

	owned, err := normalizeRelations(relations)
	if err != nil {
		t.Fatal(err)
	}
	columns[0].Name = "changed"
	relations[0].Name = "changed"
	if owned[0].Schema != "public" || owned[0].Name != "users" || owned[0].Columns[0].Name != "id" {
		t.Fatalf("normalized relations changed through caller input: %#v", owned)
	}
}

func TestNormalizeRelationsRejectsAmbiguousInput(t *testing.T) {
	tests := [][]RelationSpec{
		{{Name: "users"}, {Schema: "public", Name: "users"}},
		{{Name: "users", Columns: []ColumnSpec{{Name: "id"}, {Name: "id"}}}},
		{{Name: "users", Columns: []ColumnSpec{{Name: " "}}}},
		{{Name: "users", ColumnsSpecified: true}},
		{{Name: "users", Columns: []ColumnSpec{{Name: "name"}}, PrimaryKey: "id"}},
		{{Name: "users", ReplicaIdentity: publication.ReplicaIdentityUsingIndex}},
		{{Name: "users", ReplicaIdentity: publication.ReplicaIdentityDefault, ReplicaIdentityIndex: "users_idx"}},
	}
	for _, relations := range tests {
		if _, err := normalizeRelations(relations); err == nil {
			t.Fatalf("normalizeRelations(%#v) succeeded", relations)
		}
	}
}

func TestCompileRelationPreservesExplicitColumnBoundary(t *testing.T) {
	row := func(name, oid, typ string, key, primary bool) [][]byte {
		flag := func(v bool) []byte {
			if v {
				return []byte("t")
			}
			return []byte("f")
		}
		return [][]byte{
			[]byte("42"), []byte("d"), nil, []byte("f"), []byte("r"), []byte(name),
			[]byte("t"), []byte(oid), []byte("-1"), flag(key), []byte(typ),
			[]byte("f"), []byte("f"), flag(primary),
		}
	}
	spec := &RelationSpec{
		Schema: "public", Name: "users", ColumnsSpecified: true, PrimaryKey: "id",
		ReplicaIdentity: publication.ReplicaIdentityDefault,
		Columns:         []ColumnSpec{{Name: "id", Type: "integer"}, {Name: "name", Type: "text"}},
	}

	compile := func(secretOID, secretType string) (relation, publication.Table) {
		compiled, table, found, err := compileRelation(spec, [][][]byte{
			row("id", "23", "integer", true, true),
			row("name", "25", "text", false, false),
			row("secret", secretOID, secretType, false, false),
		}, true, true, false)
		if err != nil || !found {
			t.Fatalf("compileRelation found=%t err=%v", found, err)
		}
		return compiled, table
	}
	compiled, table := compile("25", "text")
	if len(compiled.Columns) != 2 || compiled.Columns[0].Name != "id" || compiled.Columns[1].Name != "name" {
		t.Fatalf("compiled columns = %#v, want [id name]", compiled.Columns)
	}
	if got := table.Columns; len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Fatalf("publication columns = %v, want [id name]", got)
	}
	changed, changedTable := compile("20", "bigint")
	hash := func(compiled relation, table publication.Table) string {
		return planHash(&Plan{tables: publication.Tables{table}, relations: map[uint32]relation{compiled.OID: compiled}})
	}
	if hash(compiled, table) != hash(changed, changedTable) {
		t.Fatal("unselected trailing column changed the capture hash")
	}
}

func TestCompileRelationAllowsInsertOnlyCaptureWithoutKey(t *testing.T) {
	flag := func(value bool) []byte {
		if value {
			return []byte("t")
		}
		return []byte("f")
	}
	row := func(name string, key bool) [][]byte {
		return [][]byte{
			[]byte("42"), []byte("d"), nil, []byte("f"), []byte("r"), []byte(name),
			[]byte("t"), []byte("25"), []byte("-1"), flag(key), []byte("text"),
			[]byte("f"), []byte("f"), flag(key),
		}
	}
	spec := &RelationSpec{
		Schema: "public", Name: "events", ColumnsSpecified: true,
		Columns: []ColumnSpec{{Name: "payload", Type: "text"}},
	}

	compiled, _, found, err := compileRelation(spec, [][][]byte{
		row("id", true), row("payload", false),
	}, false, true, false)
	if err != nil || !found || len(compiled.Columns) != 1 || compiled.Columns[0].Name != "payload" {
		t.Fatalf("insert-only capture without key: found=%t columns=%#v err=%v", found, compiled.Columns, err)
	}
}

func TestCompileRelationRejectsGeneratedColumns(t *testing.T) {
	row := [][]byte{
		[]byte("42"), []byte("d"), nil, []byte("f"), []byte("r"), []byte("computed"),
		[]byte("t"), []byte("23"), []byte("-1"), []byte("f"), []byte("integer"),
		[]byte("f"), []byte("t"), []byte("f"),
	}
	_, _, _, err := compileRelation(&RelationSpec{Schema: "public", Name: "values"}, [][][]byte{row}, false, true, false)
	if err == nil {
		t.Fatal("generated column was accepted by an all-column capture")
	}
}

func TestPublicationConfigPreservesColumnListIntent(t *testing.T) {
	plan := &Plan{
		tables: publication.Tables{
			{
				Schema:           "public",
				Name:             "all_columns",
				Columns:          []string{"id", "name"},
				ColumnsSpecified: false,
			},
			{
				Schema:           "public",
				Name:             "selected_columns",
				Columns:          []string{"id"},
				ColumnsSpecified: true,
			},
		},
		operations: publication.Operations{publication.OperationInsert},
	}

	config := plan.PublicationConfig("events", true)
	if config.Tables[0].Columns != nil {
		t.Fatalf("unrestricted publication columns = %v, want nil", config.Tables[0].Columns)
	}
	if got := config.Tables[1].Columns; len(got) != 1 || got[0] != "id" {
		t.Fatalf("explicit publication columns = %v, want [id]", got)
	}
	config.Tables[1].Columns[0] = "changed"
	if plan.tables[1].Columns[0] != "id" {
		t.Fatal("publication config exposed plan backing columns")
	}
}

func TestPlanValidatesRelationContract(t *testing.T) {
	plan := &Plan{relations: map[uint32]relation{42: {
		Schema:          "public",
		Name:            "users",
		OID:             42,
		ReplicaIdentity: 'd',
		Columns: []column{
			{Name: "id", DataType: 23, TypeModifier: ^uint32(0), Key: true},
			{Name: "name", DataType: 25, TypeModifier: ^uint32(0)},
		},
	}}}
	relation := &format.Relation{
		Namespace: "public",
		Name:      "users",
		OID:       42,
		ReplicaID: 'd',
		Columns: []tuple.RelationColumn{
			{Name: "id", DataType: 23, TypeModifier: ^uint32(0), Flags: 1},
			{Name: "name", DataType: 25, TypeModifier: ^uint32(0)},
		},
	}
	if err := plan.ValidateRelation(relation); err != nil {
		t.Fatal(err)
	}
	relation.Columns[1].DataType = 1043
	if err := plan.ValidateRelation(relation); err == nil {
		t.Fatal("type drift was accepted")
	}
}

func TestNormalizeMessageUsesSnapshotTextRepresentation(t *testing.T) {
	plan := &Plan{relations: map[uint32]relation{42: {
		Schema: "public",
		Name:   "items",
		OID:    42,
		Columns: []column{
			{Name: "id", DataType: 23, Key: true},
			{Name: "enabled", DataType: 16},
			{Name: "payload", DataType: 3802},
			{Name: "body", DataType: 25},
		},
	}}}
	message := &format.Update{
		OID:          42,
		OldTupleType: format.TupleTypeKey,
		NewDecoded: map[string]any{
			"enabled": true,
			"payload": map[string]any{"n": float64(1)},
		},
		OldTupleData: &tuple.Data{Columns: tuple.DataColumns{
			{DataType: tuple.DataTypeText, Data: []byte("7")},
			{DataType: tuple.DataTypeNull},
			{DataType: tuple.DataTypeNull},
			{DataType: tuple.DataTypeNull},
		}},
		NewTupleData: &tuple.Data{Columns: tuple.DataColumns{
			{DataType: tuple.DataTypeText, Data: []byte("8")},
			{DataType: tuple.DataTypeText, Data: []byte("t")},
			{DataType: tuple.DataTypeText, Data: []byte(`{"n": 1}`)},
			{DataType: tuple.DataTypeToast},
		}},
	}

	if err := plan.NormalizeMessage(message); err != nil {
		t.Fatal(err)
	}
	if got := message.NewDecoded["id"]; got != "8" {
		t.Fatalf("id = %#v", got)
	}
	if got := message.NewDecoded["enabled"]; got != "t" {
		t.Fatalf("enabled = %#v", got)
	}
	if got := message.NewDecoded["payload"]; got != `{"n": 1}` {
		t.Fatalf("payload = %#v", got)
	}
	if _, ok := message.NewDecoded["body"]; ok {
		t.Fatalf("unchanged TOAST column was included: %#v", message.NewDecoded)
	}
	if len(message.OldDecoded) != 1 || message.OldDecoded["id"] != "7" {
		t.Fatalf("old key tuple = %#v, want only id", message.OldDecoded)
	}
}

func TestNormalizeMessageRejectsTupleShapeDrift(t *testing.T) {
	plan := &Plan{relations: map[uint32]relation{42: {
		Schema:  "public",
		Name:    "items",
		OID:     42,
		Columns: []column{{Name: "id"}, {Name: "name"}},
	}}}
	message := &format.Insert{
		OID:       42,
		TupleData: &tuple.Data{Columns: tuple.DataColumns{{DataType: tuple.DataTypeText, Data: []byte("1")}}},
	}

	if err := plan.NormalizeMessage(message); err == nil {
		t.Fatal("short tuple was accepted")
	}
}

func TestNormalizeMessageRejectsBinaryTuple(t *testing.T) {
	plan := &Plan{relations: map[uint32]relation{42: {
		Schema:  "public",
		Name:    "items",
		OID:     42,
		Columns: []column{{Name: "id", DataType: 23, Key: true}},
	}}}
	message := &format.Insert{
		OID:       42,
		TupleData: &tuple.Data{Columns: tuple.DataColumns{{DataType: tuple.DataTypeBinary, Data: []byte{1}}}},
	}

	if err := plan.NormalizeMessage(message); err == nil {
		t.Fatal("binary tuple was accepted")
	}
}

func TestPlanHashOwnsColumnNullability(t *testing.T) {
	makePlan := func(nullable bool) *Plan {
		return &Plan{
			tables: publication.Tables{{
				Schema:           "public",
				Name:             "users",
				Columns:          []string{"id", "name"},
				ColumnsSpecified: true,
				ReplicaIdentity:  publication.ReplicaIdentityDefault,
			}},
			operations: publication.Operations{publication.OperationInsert},
			relations: map[uint32]relation{42: {
				Schema: "public",
				Name:   "users",
				OID:    42,
				Columns: []column{
					{Name: "id", DataType: 23, TypeModifier: ^uint32(0), Key: true},
					{Name: "name", DataType: 25, TypeModifier: ^uint32(0), Nullable: nullable},
				},
			}},
		}
	}

	if left, right := planHash(makePlan(false)), planHash(makePlan(true)); left == right {
		t.Fatal("column nullability did not change capture plan hash")
	}
}

func TestPlanHashOwnsRelationOID(t *testing.T) {
	makePlan := func(oid uint32) *Plan {
		return &Plan{
			tables: publication.Tables{{
				Schema:           "public",
				Name:             "users",
				Columns:          []string{"id", "name"},
				ColumnsSpecified: true,
				ReplicaIdentity:  publication.ReplicaIdentityDefault,
			}},
			operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationUpdate,
				publication.OperationDelete,
				publication.OperationTruncate,
			},
			relations: map[uint32]relation{oid: {
				Schema:          "public",
				Name:            "users",
				OID:             oid,
				ReplicaIdentity: 'd',
				Columns: []column{
					{Name: "id", DataType: 23, TypeModifier: ^uint32(0), Key: true},
					{Name: "name", DataType: 25, TypeModifier: ^uint32(0)},
				},
			}},
		}
	}

	if left, right := planHash(makePlan(42)), planHash(makePlan(99)); left == right {
		t.Fatal("relation OID did not change capture plan hash")
	}
}

func TestPlanHashOwnsPartitionDescendants(t *testing.T) {
	makePlan := func(descendant uint32) *Plan {
		return &Plan{
			tables: publication.Tables{{Schema: "public", Name: "events", Partitioned: true}},
			relations: map[uint32]relation{42: {
				Schema: "public", Name: "events", OID: 42, Partitioned: true,
				Descendants: []uint32{descendant},
			}},
		}
	}
	if planHash(makePlan(43)) == planHash(makePlan(44)) {
		t.Fatal("partition descendant did not change capture plan hash")
	}
}

func TestRelationSpecsPreserveColumnListOwnership(t *testing.T) {
	tables := publication.Tables{{
		Schema:          "public",
		Name:            "users",
		Columns:         []string{"id", "name"},
		ReplicaIdentity: publication.ReplicaIdentityDefault,
	}}

	configured := RelationsFromConfig(tables)
	observed := RelationsFromPublication(tables)
	if !configured[0].ColumnsSpecified {
		t.Fatal("configured columns were not treated as an explicit publication list")
	}
	if observed[0].ColumnsSpecified {
		t.Fatal("unrestricted observed publication was changed into an explicit list")
	}

	tables[0].Columns[0] = "changed"
	if configured[0].Columns[0].Name != "id" || observed[0].Columns[0].Name != "id" {
		t.Fatal("relation specs retained caller-owned column storage")
	}
}
