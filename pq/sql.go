package pq

import "strings"

// QuoteIdentifier quotes a PostgreSQL identifier and escapes embedded quotes.
func QuoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// QuoteQualifiedName quotes a two-part PostgreSQL name, such as schema.table.
func QuoteQualifiedName(schema, name string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(name)
}

// QuoteLiteral quotes a PostgreSQL string literal and escapes embedded quotes.
func QuoteLiteral(literal string) string {
	return `'` + strings.ReplaceAll(literal, `'`, `''`) + `'`
}
