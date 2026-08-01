package capture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

const defaultSchema = "public"

type column struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Nullable     bool
	Key          bool
}

type relation struct {
	Schema          string
	Name            string
	OID             uint32
	ReplicaIdentity uint8
	Partitioned     bool
	Descendants     []uint32
	Columns         []column
}

// Plan is the source-observed, deeply owned contract shared by snapshot SQL
// and pgoutput relation validation.
type Plan struct {
	spec                    Spec
	tables                  publication.Tables
	operations              publication.Operations
	publishViaPartitionRoot bool
	relations               map[uint32]relation
	hash                    string
	scanAllowed             bool
}

type ColumnSpec struct {
	Name     string
	Type     string
	Nullable bool
}

type RelationSpec struct {
	Schema               string
	Name                 string
	Optional             bool
	Columns              []ColumnSpec
	ColumnsSpecified     bool
	RowFilter            string
	ReplicaIdentity      string
	ReplicaIdentityIndex string
	PrimaryKey           string
}

// Spec requires fixed partition membership for a capture generation when
// PublishViaPartitionRoot is set. ATTACH or DETACH requires a resnapshot.
type Spec struct {
	PublicationName         string
	Relations               []RelationSpec
	Operations              publication.Operations
	PublishViaPartitionRoot bool
	RequireUnfiltered       bool
}

// RelationsFromPublication preserves the exact semantics observed from
// PostgreSQL, including the distinction between an unrestricted table and an
// explicit list containing every current column.
func RelationsFromPublication(tables publication.Tables) []RelationSpec {
	return relationSpecs(tables, false)
}

// RelationsFromConfig interprets a non-empty configured column list as an
// explicit publication column list. Config input has no separate
// ColumnsSpecified field, unlike PostgreSQL introspection.
func RelationsFromConfig(tables publication.Tables) []RelationSpec {
	return relationSpecs(tables, true)
}

func relationSpecs(tables publication.Tables, configured bool) []RelationSpec {
	relations := make([]RelationSpec, len(tables))
	for i, table := range tables {
		columns := make([]ColumnSpec, len(table.Columns))
		for j, name := range table.Columns {
			columns[j].Name = name
		}
		relations[i] = RelationSpec{
			Schema:               table.Schema,
			Name:                 table.Name,
			Columns:              columns,
			ColumnsSpecified:     table.ColumnsSpecified || configured && len(table.Columns) > 0,
			RowFilter:            table.RowFilter,
			ReplicaIdentity:      table.ReplicaIdentity,
			ReplicaIdentityIndex: table.ReplicaIdentityIndex,
		}
	}
	return relations
}

func Compile(ctx context.Context, conn pq.Connection, spec Spec) (*Plan, error) {
	owned, err := normalizeRelations(spec.Relations)
	if err != nil {
		return nil, err
	}
	operations := append(publication.Operations(nil), spec.Operations...)
	if err := operations.Validate(); err != nil {
		return nil, fmt.Errorf("capture publication operations: %w", err)
	}
	slices.Sort(operations)

	relations := make(map[uint32]relation, len(owned))
	tables := make(publication.Tables, 0, len(owned))
	requiresKey := slices.Contains(operations, publication.OperationUpdate) || slices.Contains(operations, publication.OperationDelete)
	for i := range owned {
		compiled, table, found, err := inspectRelation(
			ctx,
			conn,
			&owned[i],
			requiresKey,
			spec.RequireUnfiltered,
			spec.PublishViaPartitionRoot,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			if owned[i].Optional {
				continue
			}
			return nil, fmt.Errorf("capture relation %s.%s does not exist or has no columns", owned[i].Schema, owned[i].Name)
		}
		if compiled.Partitioned && slices.Contains(operations, publication.OperationTruncate) {
			return nil, fmt.Errorf("capture relation %s.%s cannot publish TRUNCATE through a partition root", compiled.Schema, compiled.Name)
		}
		if previous, ok := relations[compiled.OID]; ok {
			return nil, fmt.Errorf("capture relations %s.%s and %s.%s have the same oid %d",
				previous.Schema, previous.Name, compiled.Schema, compiled.Name, compiled.OID)
		}
		relations[compiled.OID] = compiled
		tables = append(tables, table)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("capture plan resolved no source relations")
	}

	plan := &Plan{
		spec: Spec{
			Relations:               owned,
			Operations:              operations,
			PublishViaPartitionRoot: spec.PublishViaPartitionRoot,
			RequireUnfiltered:       spec.RequireUnfiltered,
		},
		tables:                  tables,
		operations:              operations,
		publishViaPartitionRoot: spec.PublishViaPartitionRoot,
		relations:               relations,
		scanAllowed:             spec.RequireUnfiltered,
	}
	if err := inspectInheritance(ctx, conn, relations); err != nil {
		return nil, err
	}
	if spec.PublicationName != "" {
		actual, err := publication.New(publication.Config{Name: spec.PublicationName}, conn).Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect publication %s: %w", spec.PublicationName, err)
		}
		if err := validatePublication(spec.PublicationName, actual, plan, spec.RequireUnfiltered); err != nil {
			return nil, err
		}
	}
	plan.hash = planHash(plan)
	return plan, nil
}

func validatePublication(name string, actual *publication.Config, plan *Plan, requireUnfiltered bool) error {
	if actual == nil || actual.Name != name {
		return fmt.Errorf("publication %s is missing", name)
	}
	if actual.AllTables || actual.SchemaTables {
		return fmt.Errorf("publication %s must use an explicit table list", name)
	}
	operations := append(publication.Operations(nil), actual.Operations...)
	slices.Sort(operations)
	if !slices.Equal(operations, plan.operations) {
		return fmt.Errorf("publication %s operations are %v, want %v", name, operations, plan.operations)
	}
	if actual.PublishViaPartitionRoot != plan.publishViaPartitionRoot {
		return fmt.Errorf(
			"publication %s publish_via_partition_root=%t, want %t",
			name,
			actual.PublishViaPartitionRoot,
			plan.publishViaPartitionRoot,
		)
	}

	actualByName := make(map[string]publication.Table, len(actual.Tables))
	for _, table := range actual.Tables {
		key := table.Schema + "." + table.Name
		if _, exists := actualByName[key]; exists {
			return fmt.Errorf("publication %s contains duplicate table %s", name, key)
		}
		actualByName[key] = table
	}
	if len(actualByName) != len(plan.tables) {
		return fmt.Errorf("publication %s has %d tables, want %d", name, len(actualByName), len(plan.tables))
	}
	for _, expected := range plan.tables {
		key := expected.Schema + "." + expected.Name
		observed, ok := actualByName[key]
		if !ok {
			return fmt.Errorf("publication %s is missing table %s", name, key)
		}
		if observed.ColumnsSpecified != expected.ColumnsSpecified {
			return fmt.Errorf(
				"publication %s table %s explicit-columns=%t, want %t",
				name,
				key,
				observed.ColumnsSpecified,
				expected.ColumnsSpecified,
			)
		}
		if expected.ColumnsSpecified && !slices.Equal(observed.Columns, expected.Columns) {
			return fmt.Errorf("publication %s table %s columns are %v, want %v", name, key, observed.Columns, expected.Columns)
		}
		observedFilter := strings.TrimSpace(observed.RowFilter)
		expectedFilter := strings.TrimSpace(expected.RowFilter)
		if requireUnfiltered {
			if observedFilter != "" {
				return fmt.Errorf("publication %s table %s has unsupported row filter %s", name, key, observed.RowFilter)
			}
		} else if observedFilter != expectedFilter {
			return fmt.Errorf("publication %s table %s row filter is %q, want %q", name, key, observedFilter, expectedFilter)
		}
		if observed.ReplicaIdentity != expected.ReplicaIdentity || observed.ReplicaIdentityIndex != expected.ReplicaIdentityIndex {
			return fmt.Errorf(
				"publication %s table %s replica identity is %s/%s, want %s/%s",
				name,
				key,
				observed.ReplicaIdentity,
				observed.ReplicaIdentityIndex,
				expected.ReplicaIdentity,
				expected.ReplicaIdentityIndex,
			)
		}
	}
	return nil
}

func normalizeRelations(relations []RelationSpec) ([]RelationSpec, error) {
	if len(relations) == 0 {
		return nil, fmt.Errorf("capture plan requires at least one relation")
	}

	owned := make([]RelationSpec, len(relations))
	seenRelations := make(map[string]struct{}, len(relations))
	for i, relation := range relations {
		relation.Schema = strings.TrimSpace(relation.Schema)
		if relation.Schema == "" {
			relation.Schema = defaultSchema
		}
		relation.Name = strings.TrimSpace(relation.Name)
		if relation.Name == "" {
			return nil, fmt.Errorf("capture relation name cannot be empty")
		}
		key := relation.Schema + "." + relation.Name
		if _, ok := seenRelations[key]; ok {
			return nil, fmt.Errorf("duplicate capture relation %s", key)
		}
		seenRelations[key] = struct{}{}

		relation.PrimaryKey = strings.TrimSpace(relation.PrimaryKey)
		relation.RowFilter = strings.TrimSpace(relation.RowFilter)
		relation.ReplicaIdentityIndex = strings.TrimSpace(relation.ReplicaIdentityIndex)
		if relation.ReplicaIdentity != "" && !slices.Contains(publication.ReplicaIdentityOptions, relation.ReplicaIdentity) {
			return nil, fmt.Errorf("capture relation %s has invalid replica identity %q", key, relation.ReplicaIdentity)
		}
		if relation.ReplicaIdentity == publication.ReplicaIdentityUsingIndex {
			if relation.ReplicaIdentityIndex == "" {
				return nil, fmt.Errorf("capture relation %s requires replica identity index", key)
			}
		} else if relation.ReplicaIdentityIndex != "" {
			return nil, fmt.Errorf("capture relation %s has replica identity index without USING INDEX", key)
		}

		relation.Columns = append([]ColumnSpec(nil), relation.Columns...)
		if relation.ColumnsSpecified && len(relation.Columns) == 0 {
			return nil, fmt.Errorf("capture relation %s has an explicit empty column list", key)
		}
		seenColumns := make(map[string]struct{}, len(relation.Columns))
		for j := range relation.Columns {
			column := &relation.Columns[j]
			column.Name = strings.TrimSpace(column.Name)
			column.Type = strings.TrimSpace(column.Type)
			if column.Name == "" {
				return nil, fmt.Errorf("capture relation %s has an empty column", key)
			}
			if _, ok := seenColumns[column.Name]; ok {
				return nil, fmt.Errorf("capture relation %s has duplicate column %s", key, column.Name)
			}
			seenColumns[column.Name] = struct{}{}
		}
		if relation.PrimaryKey != "" {
			if _, ok := seenColumns[relation.PrimaryKey]; !ok {
				return nil, fmt.Errorf("capture relation %s primary key %s is not selected", key, relation.PrimaryKey)
			}
		}
		owned[i] = relation
	}
	return owned, nil
}

func inspectRelation(
	ctx context.Context,
	conn pq.Connection,
	spec *RelationSpec,
	requiresKey bool,
	requireUnfiltered bool,
	publishViaPartitionRoot bool,
) (relation, publication.Table, bool, error) {
	query := fmt.Sprintf(`
		SELECT
			c.oid::text,
			c.relreplident::text,
			COALESCE(ri_class.relname, ''),
			c.relrowsecurity::text,
			c.relkind::text,
			a.attname,
			has_column_privilege(c.oid, a.attnum, 'SELECT')::text,
			a.atttypid::text,
			a.atttypmod::text,
			CASE
				WHEN c.relreplident = 'f' THEN true
				WHEN c.relreplident = 'n' THEN false
				WHEN c.relreplident = 'i' THEN EXISTS (
					SELECT 1 FROM pg_index i
					WHERE i.indrelid = c.oid AND i.indisreplident
					AND EXISTS (
						SELECT 1 FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
						WHERE k.position <= i.indnkeyatts AND k.attnum = a.attnum
					)
				)
				ELSE EXISTS (
					SELECT 1 FROM pg_index i
					WHERE i.indrelid = c.oid AND i.indisprimary
					AND EXISTS (
						SELECT 1 FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
						WHERE k.position <= i.indnkeyatts AND k.attnum = a.attnum
					)
				)
			END::text,
			format_type(a.atttypid, a.atttypmod),
			(NOT a.attnotnull)::text,
			(a.attgenerated <> '')::text,
			EXISTS (
				SELECT 1 FROM pg_index i
				WHERE i.indrelid = c.oid AND i.indisprimary
				AND EXISTS (
					SELECT 1 FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
					WHERE k.position <= i.indnkeyatts AND k.attnum = a.attnum
				)
			)::text
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		LEFT JOIN pg_index ri ON ri.indrelid = c.oid AND ri.indisreplident
		LEFT JOIN pg_class ri_class ON ri_class.oid = ri.indexrelid
		WHERE n.nspname = %s
		AND c.relname = %s
		AND c.relkind IN ('r', 'p')
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum`, pq.QuoteLiteral(spec.Schema), pq.QuoteLiteral(spec.Name))

	results, err := pq.ExecQuery(ctx, conn, query)
	if err != nil {
		return relation{}, publication.Table{}, false, fmt.Errorf("inspect capture relation %s.%s: %w", spec.Schema, spec.Name, err)
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return relation{}, publication.Table{}, false, nil
	}
	return compileRelation(spec, results[0].Rows, requiresKey, requireUnfiltered, publishViaPartitionRoot)
}

func inspectInheritance(ctx context.Context, conn pq.Connection, relations map[uint32]relation) error {
	oids := make([]string, 0, len(relations))
	for oid := range relations {
		oids = append(oids, strconv.FormatUint(uint64(oid), 10))
	}
	slices.Sort(oids)
	list := strings.Join(oids, ", ")
	results, err := pq.ExecQuery(ctx, conn, fmt.Sprintf(`
		WITH RECURSIVE descendants(ancestor, descendant) AS (
			SELECT inhparent, inhrelid FROM pg_inherits WHERE inhparent IN (%s)
			UNION ALL
			SELECT d.ancestor, i.inhrelid
			FROM descendants d
			JOIN pg_inherits i ON i.inhparent = d.descendant
		)
		SELECT ancestor::text, descendant::text
		FROM descendants
		WHERE ancestor IN (%s)
		ORDER BY ancestor, descendant`, list, list))
	if err != nil {
		return fmt.Errorf("inspect capture relation inheritance: %w", err)
	}
	if len(results) == 0 {
		return nil
	}
	for _, row := range results[0].Rows {
		ancestor, err := parseUint32(row[0], "ancestor relation oid")
		if err != nil {
			return err
		}
		descendant, err := parseUint32(row[1], "descendant relation oid")
		if err != nil {
			return err
		}
		parent := relations[ancestor]
		if child, planned := relations[descendant]; planned && parent.Partitioned {
			return fmt.Errorf("capture relations %s.%s and %s.%s overlap through inheritance", parent.Schema, parent.Name, child.Schema, child.Name)
		}
		if parent.Partitioned {
			parent.Descendants = append(parent.Descendants, descendant)
			relations[ancestor] = parent
		}
	}
	return nil
}

func compileRelation(
	spec *RelationSpec,
	rows [][][]byte,
	requiresKey, requireUnfiltered, publishViaPartitionRoot bool,
) (relation, publication.Table, bool, error) {
	expectedByName := make(map[string]ColumnSpec, len(spec.Columns))
	for _, column := range spec.Columns {
		expectedByName[column.Name] = column
	}

	oid, err := parseUint32(rows[0][0], "relation oid")
	if err != nil {
		return relation{}, publication.Table{}, false, err
	}
	replicaIdentity := string(rows[0][1])
	if len(replicaIdentity) != 1 {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s has invalid replica identity %q", spec.Schema, spec.Name, replicaIdentity)
	}
	actualReplicaIdentity, ok := publication.ReplicaIdentityMap[replicaIdentity]
	if !ok {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s has unsupported replica identity %q", spec.Schema, spec.Name, replicaIdentity)
	}
	actualReplicaIdentityIndex := string(rows[0][2])
	if requireUnfiltered {
		rowSecurity, err := parseCatalogBool(rows[0][3])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s row security: %w", spec.Schema, spec.Name, err)
		}
		if rowSecurity {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s has row-level security", spec.Schema, spec.Name)
		}
	}
	partitioned := string(rows[0][4]) == "p"
	if partitioned && !publishViaPartitionRoot {
		return relation{}, publication.Table{}, false, fmt.Errorf(
			"capture relation %s.%s is partitioned; publish_via_partition_root is required",
			spec.Schema,
			spec.Name,
		)
	}
	if spec.ReplicaIdentity != "" && spec.ReplicaIdentity != actualReplicaIdentity {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s uses replica identity %s, want %s", spec.Schema, spec.Name, actualReplicaIdentity, spec.ReplicaIdentity)
	}
	if spec.ReplicaIdentity == publication.ReplicaIdentityUsingIndex && spec.ReplicaIdentityIndex != actualReplicaIdentityIndex {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s uses replica identity index %s, want %s", spec.Schema, spec.Name, actualReplicaIdentityIndex, spec.ReplicaIdentityIndex)
	}

	compiled := relation{
		Schema:          spec.Schema,
		Name:            spec.Name,
		OID:             oid,
		ReplicaIdentity: replicaIdentity[0],
		Partitioned:     partitioned,
	}
	resolved := publication.Table{
		Schema:               spec.Schema,
		Name:                 spec.Name,
		ColumnsSpecified:     spec.ColumnsSpecified,
		RowFilter:            spec.RowFilter,
		ReplicaIdentity:      actualReplicaIdentity,
		ReplicaIdentityIndex: actualReplicaIdentityIndex,
		Partitioned:          partitioned,
	}
	if spec.PrimaryKey != "" {
		var primaryColumns []string
		for _, row := range rows {
			primary, err := parseCatalogBool(row[13])
			if err != nil {
				return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s primary key: %w", spec.Schema, spec.Name, err)
			}
			if primary {
				primaryColumns = append(primaryColumns, string(row[5]))
			}
		}
		if len(primaryColumns) != 1 || primaryColumns[0] != spec.PrimaryKey {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s primary key is %v, want [%s]", spec.Schema, spec.Name, primaryColumns, spec.PrimaryKey)
		}
	}
	explicitColumns := spec.ColumnsSpecified
	for _, row := range rows {
		name := string(row[5])
		expected, selected := expectedByName[name]
		key, err := parseCatalogBool(row[9])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s key flag: %w", spec.Schema, spec.Name, name, err)
		}
		if explicitColumns && !selected {
			if requiresKey && key {
				return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s omits replica identity column %s", spec.Schema, spec.Name, name)
			}
			continue
		}
		selectable, err := parseCatalogBool(row[6])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s SELECT privilege: %w", spec.Schema, spec.Name, name, err)
		}
		if !selectable {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s is not readable by the capture role", spec.Schema, spec.Name, name)
		}
		dataType, err := parseUint32(row[7], "column type oid")
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s: %w", spec.Schema, spec.Name, name, err)
		}
		typeModifier, err := parseTypeModifier(row[8])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s: %w", spec.Schema, spec.Name, name, err)
		}
		actualNullable, err := parseCatalogBool(row[11])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s nullability: %w", spec.Schema, spec.Name, name, err)
		}
		generated, err := parseCatalogBool(row[12])
		if err != nil {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s generated state: %w", spec.Schema, spec.Name, name, err)
		}
		if generated {
			return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s is generated", spec.Schema, spec.Name, name)
		}
		if selected && expected.Type != "" {
			if actualType := string(row[10]); actualType != expected.Type {
				return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s type is %s, want %s", spec.Schema, spec.Name, name, actualType, expected.Type)
			}
			if actualNullable != expected.Nullable {
				return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s column %s nullable=%t, want %t", spec.Schema, spec.Name, name, actualNullable, expected.Nullable)
			}
		}
		compiled.Columns = append(compiled.Columns, column{
			Name:         name,
			DataType:     dataType,
			TypeModifier: typeModifier,
			Nullable:     actualNullable,
			Key:          key,
		})
		resolved.Columns = append(resolved.Columns, name)
		delete(expectedByName, name)
	}
	if len(compiled.Columns) == 0 {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s selects no columns", spec.Schema, spec.Name)
	}
	if requiresKey && !slices.ContainsFunc(compiled.Columns, func(column column) bool { return column.Key }) {
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s has no replica identity columns for update/delete", spec.Schema, spec.Name)
	}
	if len(expectedByName) > 0 {
		missing := make([]string, 0, len(expectedByName))
		for name := range expectedByName {
			missing = append(missing, name)
		}
		slices.Sort(missing)
		return relation{}, publication.Table{}, false, fmt.Errorf("capture relation %s.%s is missing columns: %s", spec.Schema, spec.Name, strings.Join(missing, ", "))
	}
	return compiled, resolved, true, nil
}

func parseCatalogBool(raw []byte) (bool, error) {
	switch string(raw) {
	case "t", "true":
		return true, nil
	case "f", "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid PostgreSQL boolean %q", raw)
	}
}

func parseUint32(raw []byte, field string) (uint32, error) {
	value, err := strconv.ParseUint(string(raw), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse %s %q: %w", field, raw, err)
	}
	return uint32(value), nil
}

func parseTypeModifier(raw []byte) (uint32, error) {
	value, err := strconv.ParseInt(string(raw), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse type modifier %q: %w", raw, err)
	}
	return uint32(int32(value)), nil
}

// NormalizeMessage rewrites DML payloads into the same PostgreSQL text
// representation used by snapshot scanning. SQL NULL remains nil and an
// unchanged-TOAST marker remains absent so the durable mirror can supply the
// previous value.
func (p *Plan) NormalizeMessage(message any) error {
	var oid uint32
	type messageTuple struct {
		data    *tuple.Data
		target  *map[string]any
		keyOnly bool
	}
	var tuples []messageTuple
	switch message := message.(type) {
	case *format.Insert:
		oid = message.OID
		tuples = append(tuples, messageTuple{message.TupleData, &message.Decoded, false})
	case *format.Update:
		oid = message.OID
		if message.OldTupleData != nil {
			tuples = append(tuples, messageTuple{message.OldTupleData, &message.OldDecoded, message.OldTupleType == format.TupleTypeKey})
		}
		tuples = append(tuples, messageTuple{message.NewTupleData, &message.NewDecoded, false})
	case *format.Delete:
		oid = message.OID
		tuples = append(tuples, messageTuple{message.OldTupleData, &message.OldDecoded, message.OldTupleType == format.TupleTypeKey})
	default:
		return nil
	}

	relation, ok := p.relations[oid]
	if !ok {
		return fmt.Errorf("relation oid %d is not in the capture plan", oid)
	}
	for _, item := range tuples {
		decoded, err := decodeTupleText(item.data, relation.Columns, item.keyOnly)
		if err != nil {
			return fmt.Errorf("normalize relation %s.%s tuple: %w", relation.Schema, relation.Name, err)
		}
		*item.target = decoded
	}
	return nil
}

func decodeTupleText(data *tuple.Data, columns []column, keyOnly bool) (map[string]any, error) {
	if data == nil {
		return nil, fmt.Errorf("tuple is missing")
	}
	if len(data.Columns) != len(columns) {
		return nil, fmt.Errorf("tuple has %d columns for %d-column relation", len(data.Columns), len(columns))
	}
	decoded := make(map[string]any, len(data.Columns))
	for i, value := range data.Columns {
		if keyOnly && !columns[i].Key {
			continue
		}
		name := columns[i].Name
		switch value.DataType {
		case tuple.DataTypeNull:
			decoded[name] = nil
		case tuple.DataTypeText:
			decoded[name] = string(value.Data)
		case tuple.DataTypeToast:
			// The mirror owns the complete old row; omission is the patch
			// representation for an unchanged TOASTed value.
		case tuple.DataTypeBinary:
			return nil, fmt.Errorf("column %s used binary format", name)
		default:
			return nil, fmt.Errorf("column %s has unsupported tuple format %q", name, value.DataType)
		}
	}
	return decoded, nil
}

func (p *Plan) ValidateRelation(actual *format.Relation) error {
	expected, ok := p.relations[actual.OID]
	if !ok {
		return fmt.Errorf("relation oid %d is not in the capture plan", actual.OID)
	}
	if actual.Namespace != expected.Schema || actual.Name != expected.Name {
		return fmt.Errorf("relation oid %d changed from %s.%s to %s.%s", actual.OID, expected.Schema, expected.Name, actual.Namespace, actual.Name)
	}
	if actual.ReplicaID != expected.ReplicaIdentity {
		return fmt.Errorf("relation %s.%s replica identity changed from %q to %q", expected.Schema, expected.Name, expected.ReplicaIdentity, actual.ReplicaID)
	}
	if len(actual.Columns) != len(expected.Columns) {
		return fmt.Errorf("relation %s.%s column count changed from %d to %d", expected.Schema, expected.Name, len(expected.Columns), len(actual.Columns))
	}
	for i, column := range actual.Columns {
		expectedColumn := expected.Columns[i]
		if column.Name != expectedColumn.Name || column.DataType != expectedColumn.DataType || column.TypeModifier != expectedColumn.TypeModifier || (column.Flags&1 != 0) != expectedColumn.Key {
			return fmt.Errorf("relation %s.%s column %d does not match compiled capture plan", expected.Schema, expected.Name, i)
		}
	}
	return nil
}

// Hash returns the canonical source-contract identity compiled into this plan.
// PublicationConfig returns the exact managed publication shape compiled into
// the plan. Callers may choose whether creation is allowed; the semantic shape
// is not reconstructed from mutable configuration.
func (p *Plan) PublicationConfig(name string, createIfNotExists bool) publication.Config {
	tables := make(publication.Tables, len(p.tables))
	for i, table := range p.tables {
		if table.ColumnsSpecified {
			table.Columns = append([]string(nil), table.Columns...)
		} else {
			table.Columns = nil
		}
		tables[i] = table
	}
	return publication.Config{
		Name:                    name,
		Operations:              append(publication.Operations(nil), p.operations...),
		Tables:                  tables,
		CreateIfNotExists:       createIfNotExists,
		PublishViaPartitionRoot: p.publishViaPartitionRoot,
	}
}

// Validate recompiles the complete source and publication contract. Callers
// use it immediately before taking ownership of a replication stream so a
// plan compiled before passive slot waiting cannot outlive source DDL.
func (p *Plan) Validate(ctx context.Context, conn pq.Connection, publicationName string) error {
	spec := p.spec
	spec.PublicationName = publicationName
	actual, err := Compile(ctx, conn, spec)
	if err != nil {
		return err
	}
	if actual.hash != p.hash {
		return fmt.Errorf("capture plan changed from %s to %s", p.hash, actual.hash)
	}
	return nil
}

func (p *Plan) Hash() string {
	return p.hash
}

func planHash(plan *Plan) string {
	hash := sha256.New()
	writeHashPart(hash, "go-pq-cdc-capture-v5")
	if plan.publishViaPartitionRoot {
		writeHashPart(hash, "partition-root")
	}
	for _, operation := range plan.operations {
		writeHashPart(hash, string(operation))
	}
	byName := make(map[string]relation, len(plan.relations))
	for _, relation := range plan.relations {
		byName[relation.Schema+"."+relation.Name] = relation
	}
	tables := append(publication.Tables(nil), plan.tables...)
	slices.SortFunc(tables, func(left, right publication.Table) int {
		if left.Schema != right.Schema {
			return strings.Compare(left.Schema, right.Schema)
		}
		return strings.Compare(left.Name, right.Name)
	})
	for _, table := range tables {
		relation := byName[table.Schema+"."+table.Name]
		writeHashPart(hash, strconv.FormatUint(uint64(relation.OID), 10))
		writeHashPart(hash, table.Schema)
		writeHashPart(hash, table.Name)
		if table.ColumnsSpecified {
			writeHashPart(hash, "explicit-columns")
		} else {
			writeHashPart(hash, "all-columns")
		}
		writeHashPart(hash, strings.TrimSpace(table.RowFilter))
		writeHashPart(hash, table.ReplicaIdentity)
		writeHashPart(hash, table.ReplicaIdentityIndex)
		if relation.Partitioned {
			writeHashPart(hash, "partitioned")
			for _, oid := range relation.Descendants {
				writeHashPart(hash, strconv.FormatUint(uint64(oid), 10))
			}
		}
		for _, column := range relation.Columns {
			writeHashPart(hash, column.Name)
			writeHashPart(hash, strconv.FormatUint(uint64(column.DataType), 10))
			writeHashPart(hash, strconv.FormatUint(uint64(column.TypeModifier), 10))
			if column.Nullable {
				writeHashPart(hash, "nullable")
			} else {
				writeHashPart(hash, "not-null")
			}
			if column.Key {
				writeHashPart(hash, "key")
			}
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func writeHashPart(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
	_, _ = hash.Write([]byte{':'})
	_, _ = hash.Write([]byte(value))
}
