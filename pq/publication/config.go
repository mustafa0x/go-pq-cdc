package publication

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
)

type Config struct {
	Name                    string     `json:"name" yaml:"name"`
	Operations              Operations `json:"operations" yaml:"operations"`
	Tables                  Tables     `json:"tables" yaml:"tables"`
	CreateIfNotExists       bool       `json:"createIfNotExists" yaml:"createIfNotExists"`
	AllTables               bool       `json:"-" yaml:"-"`
	SchemaTables            bool       `json:"-" yaml:"-"`
	PublishViaPartitionRoot bool       `json:"-" yaml:"-"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}
	if validateErr := c.Tables.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
	}
	if validateErr := c.Operations.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
	}
	return err
}

func (c Config) createQuery() string {
	sqlStatement := fmt.Sprintf(`CREATE PUBLICATION %s`, pq.QuoteIdentifier(c.Name))
	var hasPartitionedTable bool

	quotedTables := make([]string, len(c.Tables))
	for i, table := range c.Tables {
		if table.Partitioned {
			hasPartitionedTable = true
		}

		tableName := pq.QuoteQualifiedName(table.Schema, table.Name)
		if !table.Partitioned {
			tableName = "ONLY " + tableName
		}
		if len(table.Columns) > 0 {
			quotedTables[i] = fmt.Sprintf("%s(%s)", tableName, quoteColumnList(table.Columns))
		} else {
			quotedTables[i] = tableName
		}
	}
	sqlStatement += " FOR TABLE " + strings.Join(quotedTables, ", ")

	sqlStatement += fmt.Sprintf(" WITH (publish = %s, publish_via_partition_root = %t)", pq.QuoteLiteral(strings.ToLower(c.Operations.String())), hasPartitionedTable)

	return sqlStatement
}

func quoteColumnList(columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = pq.QuoteIdentifier(column)
	}
	return strings.Join(quoted, ", ")
}

func (c Config) infoQuery() string {
	return fmt.Sprintf(`
		SELECT
			p.pubname,
			p.puballtables,
			p.pubinsert,
			p.pubupdate,
			p.pubdelete,
			p.pubtruncate,
			p.pubviaroot,
			EXISTS (
				SELECT 1 FROM pg_publication_namespace pn WHERE pn.pnpubid = p.oid
			) AS pubschemas,
			pt.schemaname,
			pt.tablename,
			COALESCE(pt.attnames::text[], ARRAY[]::text[]) AS columns,
			(pr.prattrs IS NOT NULL) AS columns_specified,
			COALESCE(pt.rowfilter, '') AS row_filter,
			c.relreplident::text AS replica_identity,
			COALESCE(idx.relname, '') AS replica_identity_index
		FROM pg_publication p
		LEFT JOIN pg_publication_tables pt ON pt.pubname = p.pubname
		LEFT JOIN pg_namespace n ON n.nspname = pt.schemaname
		LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
		LEFT JOIN pg_publication_rel pr ON pr.prpubid = p.oid AND pr.prrelid = c.oid
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisreplident
		LEFT JOIN pg_class idx ON idx.oid = i.indexrelid
		WHERE p.pubname = %s
		ORDER BY pt.schemaname, pt.tablename`, pq.QuoteLiteral(c.Name))
}
