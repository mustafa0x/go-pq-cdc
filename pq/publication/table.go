package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

type Table struct {
	Name                 string `json:"name" yaml:"name"`
	ReplicaIdentity      string `json:"replicaIdentity" yaml:"replicaIdentity"`
	ReplicaIdentityIndex string `json:"replicaIdentityIndex,omitempty" yaml:"replicaIdentityIndex,omitempty"`
	Schema               string `json:"schema,omitempty" yaml:"schema,omitempty"`
	// RowFilter and ColumnsSpecified are populated by publication introspection.
	RowFilter        string   `json:"-" yaml:"-"`
	ColumnsSpecified bool     `json:"-" yaml:"-"`
	Columns          []string `json:"columns,omitempty" yaml:"columns,omitempty"`
	// Boolean flag to indicate if the table is partitioned, used for creating the publication on the root table.
	Partitioned bool `json:"partitioned,omitempty" yaml:"partitioned,omitempty"`
}

func (tc Table) Validate() error {
	if strings.TrimSpace(tc.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, tc.ReplicaIdentity) {
		return errors.Newf("undefined replica identity option. valid identity options are: %v", ReplicaIdentityOptions)
	}

	if strings.TrimSpace(tc.RowFilter) != "" {
		return errors.New("publication row filters are not supported by the static connector contract")
	}

	if tc.ReplicaIdentity == ReplicaIdentityUsingIndex {
		if strings.TrimSpace(tc.ReplicaIdentityIndex) == "" {
			return errors.New("replicaIdentityIndex cannot be empty when replicaIdentity is USING INDEX")
		}
	} else if strings.TrimSpace(tc.ReplicaIdentityIndex) != "" {
		return errors.New("replicaIdentityIndex can only be set when replicaIdentity is USING INDEX")
	}

	return nil
}

type Tables []Table

const defaultTableSchema = "public"

func (ts Tables) Contains(schema, name string) bool {
	if schema == "" {
		schema = defaultTableSchema
	}
	for _, t := range ts {
		tblSchema := t.Schema
		if tblSchema == "" {
			tblSchema = defaultTableSchema
		}
		if tblSchema == schema && t.Name == name {
			return true
		}
	}
	return false
}

func (ts Tables) Validate() error {
	if len(ts) == 0 {
		return errors.New("at least one table must be defined")
	}

	seenTables := make(map[string]struct{}, len(ts))
	for _, t := range ts {
		if err := t.Validate(); err != nil {
			return err
		}
		schema := strings.TrimSpace(t.Schema)
		if schema == "" {
			schema = defaultTableSchema
		}
		key := schema + "." + strings.TrimSpace(t.Name)
		if _, ok := seenTables[key]; ok {
			return errors.Newf("duplicate table %s", key)
		}
		seenTables[key] = struct{}{}

		seenColumns := make(map[string]struct{}, len(t.Columns))
		for _, column := range t.Columns {
			column = strings.TrimSpace(column)
			if column == "" {
				return errors.Newf("table %s has an empty column", key)
			}
			if _, ok := seenColumns[column]; ok {
				return errors.Newf("table %s has duplicate column %s", key, column)
			}
			seenColumns[column] = struct{}{}
		}
	}

	return nil
}

func (ts Tables) Diff(tss Tables) Tables {
	res := Tables{}
	tssMap := make(map[string]Table)

	for _, t := range tss {
		tssMap[t.Schema+"."+t.Name] = t
	}

	for _, t := range ts {
		if v, found := tssMap[t.Schema+"."+t.Name]; !found || v.ReplicaIdentity != t.ReplicaIdentity || v.ReplicaIdentityIndex != t.ReplicaIdentityIndex || !slices.Equal(v.Columns, t.Columns) || v.Partitioned != t.Partitioned {
			res = append(res, t)
		}
	}

	return res
}
