package snapshot

import (
	"errors"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
)

func TestParseJobRowValidatesPersistedMetadata(t *testing.T) {
	valid := [][]byte{
		[]byte("slot"),
		[]byte("snapshot"),
		[]byte("request"),
		[]byte("0/10"),
		[]byte("2026-07-17 12:00:00"),
		[]byte("f"),
		[]byte("2"),
		[]byte("1"),
	}
	if _, err := parseJobRow(valid, "slot"); err != nil {
		t.Fatalf("parseJobRow() error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func([][]byte)
	}{
		{name: "wrong slot", mutate: func(row [][]byte) { row[0] = []byte("other") }},
		{name: "empty snapshot ID", mutate: func(row [][]byte) { row[1] = nil }},
		{name: "zero LSN", mutate: func(row [][]byte) { row[3] = []byte("0/0") }},
		{name: "missing start time", mutate: func(row [][]byte) { row[4] = nil }},
		{name: "invalid progress", mutate: func(row [][]byte) { row[7] = []byte("3") }},
		{name: "premature completion", mutate: func(row [][]byte) { row[5] = []byte("t") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			row := cloneRow(valid)
			test.mutate(row)
			if _, err := parseJobRow(row, "slot"); err == nil {
				t.Fatal("parseJobRow() accepted invalid metadata")
			}
		})
	}
}

func TestJobSQLIdentityIncludesSnapshotGeneration(t *testing.T) {
	job := &Job{SlotName: "slot'one", SnapshotID: "snapshot'two"}

	if got, want := job.sqlIdentity("j"), "j.slot_name = 'slot''one' AND j.snapshot_id = 'snapshot''two'"; got != want {
		t.Fatalf("sqlIdentity() = %q; want %q", got, want)
	}
}

func TestValidateJobRequestRejectsAnotherResnapshot(t *testing.T) {
	snapshotter := &Snapshotter{config: config.SnapshotConfig{Resnapshot: true, ResnapshotID: "request-a"}}
	if err := snapshotter.validateJobRequest(&Job{ResnapshotID: "request-b"}); !errors.Is(err, ErrResnapshotSuperseded) {
		t.Fatalf("validateJobRequest() error = %v; want ErrResnapshotSuperseded", err)
	}
	if err := snapshotter.validateJobRequest(&Job{ResnapshotID: "request-a"}); err != nil {
		t.Fatalf("validateJobRequest() error = %v; want nil", err)
	}
}
