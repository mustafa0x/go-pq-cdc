package snapshot

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseExportedSnapshotID(t *testing.T) {
	valid := []*pgconn.Result{{Rows: [][][]byte{{[]byte("snapshot")}}}}
	value, err := parseExportedSnapshotID(valid)
	if err != nil || value != "snapshot" {
		t.Fatalf("parseExportedSnapshotID() = %q, %v; want snapshot, nil", value, err)
	}

	for _, test := range []struct {
		name    string
		results []*pgconn.Result
	}{
		{name: "missing result"},
		{name: "multiple results", results: []*pgconn.Result{{}, {}}},
		{name: "multiple rows", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("a")}, {[]byte("b")}}}}},
		{name: "wrong shape", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("snapshot"), []byte("extra")}}}}},
		{name: "null snapshot ID", results: []*pgconn.Result{{Rows: [][][]byte{{nil}}}}},
		{name: "empty snapshot ID", results: []*pgconn.Result{{Rows: [][][]byte{{[]byte("")}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseExportedSnapshotID(test.results); err == nil {
				t.Fatal("parseExportedSnapshotID() accepted invalid result")
			}
		})
	}
}
