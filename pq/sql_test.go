package pq

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	got := QuoteIdentifier(`Mixed"Name`)
	want := `"Mixed""Name"`
	if got != want {
		t.Fatalf("QuoteIdentifier() = %s, want %s", got, want)
	}
}

func TestQuoteQualifiedName(t *testing.T) {
	got := QuoteQualifiedName(`My Schema`, `select`)
	want := `"My Schema"."select"`
	if got != want {
		t.Fatalf("QuoteQualifiedName() = %s, want %s", got, want)
	}
}

func TestQuoteLiteral(t *testing.T) {
	got := QuoteLiteral(`pub'name`)
	want := `'pub''name'`
	if got != want {
		t.Fatalf("QuoteLiteral() = %s, want %s", got, want)
	}
}
