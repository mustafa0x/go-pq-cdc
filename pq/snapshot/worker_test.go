package snapshot

import (
	"strings"
	"testing"
	"time"
)

func TestBuildClaimChunkQueryFencesSnapshotGeneration(t *testing.T) {
	snapshotter := &Snapshotter{}
	job := &Job{SlotName: "slot", SnapshotID: "generation"}

	query := snapshotter.buildClaimChunkQuery(job, "worker", time.Now(), 30*time.Second)
	if !strings.Contains(query, "j.slot_name = 'slot' AND j.snapshot_id = 'generation'") {
		t.Fatalf("claim query is not generation-fenced:\n%s", query)
	}
}

func TestParseClaimedChunkValidatesPersistedPartitionMetadata(t *testing.T) {
	valid := [][]byte{
		[]byte("1"),
		[]byte("public"),
		[]byte("books"),
		[]byte("0"),
		[]byte("0"),
		[]byte("100"),
		nil,
		nil,
		nil,
		nil,
		[]byte("f"),
		[]byte(PartitionStrategyOffset),
	}

	snapshotter := &Snapshotter{}
	if _, err := snapshotter.parseClaimedChunk(valid, "slot", "worker", time.Now()); err != nil {
		t.Fatalf("parseClaimedChunk() error = %v", err)
	}

	publicSchema := cloneRow(valid)
	publicSchema[1] = nil
	chunk, err := snapshotter.parseClaimedChunk(publicSchema, "slot", "worker", time.Now())
	if err != nil || chunk.TableSchema != "public" {
		t.Fatalf("parseClaimedChunk() schema = %q, %v; want public, nil", chunk.TableSchema, err)
	}

	for _, test := range []struct {
		name   string
		mutate func([][]byte)
	}{
		{
			name:   "non-positive chunk ID",
			mutate: func(row [][]byte) { row[0] = []byte("0") },
		},
		{
			name:   "missing table name",
			mutate: func(row [][]byte) { row[2] = nil },
		},
		{
			name:   "invalid last-chunk flag",
			mutate: func(row [][]byte) { row[10] = []byte("maybe") },
		},
		{
			name:   "missing strategy",
			mutate: func(row [][]byte) { row[11] = nil },
		},
		{
			name:   "unknown strategy",
			mutate: func(row [][]byte) { row[11] = []byte("mystery") },
		},
		{
			name: "incomplete integer range",
			mutate: func(row [][]byte) {
				row[6] = []byte("1")
				row[11] = []byte(PartitionStrategyIntegerRange)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			row := cloneRow(valid)
			test.mutate(row)
			if _, err := snapshotter.parseClaimedChunk(row, "slot", "worker", time.Now()); err == nil {
				t.Fatal("parseClaimedChunk() succeeded for invalid metadata")
			}
		})
	}

	if _, err := snapshotter.parseClaimedChunk(append(cloneRow(valid), []byte("extra")), "slot", "worker", time.Now()); err == nil {
		t.Fatal("parseClaimedChunk() accepted an unexpected column")
	}
}

func TestParsePostgresBool(t *testing.T) {
	for _, test := range []struct {
		value string
		want  bool
	}{
		{value: "t", want: true},
		{value: "true", want: true},
		{value: "f"},
		{value: "false"},
	} {
		got, err := parsePostgresBool([]byte(test.value))
		if err != nil || got != test.want {
			t.Fatalf("parsePostgresBool(%q) = %v, %v; want %v, nil", test.value, got, err, test.want)
		}
	}
	if _, err := parsePostgresBool([]byte("1")); err == nil {
		t.Fatal("parsePostgresBool accepted invalid input")
	}
}

func TestParseNullableInt64DistinguishesNullFromMalformedInput(t *testing.T) {
	value, err := parseNullableInt64(nil)
	if err != nil || value != nil {
		t.Fatalf("parseNullableInt64(nil) = %v, %v; want nil, nil", value, err)
	}

	value, err = parseNullableInt64([]byte("0"))
	if err != nil || value == nil || *value != 0 {
		t.Fatalf("parseNullableInt64(0) = %v, %v; want 0, nil", value, err)
	}

	if _, err := parseNullableInt64([]byte{}); err == nil {
		t.Fatal("parseNullableInt64 accepted an empty non-NULL value")
	}
}

func cloneRow(row [][]byte) [][]byte {
	clone := make([][]byte, len(row))
	for i := range row {
		clone[i] = append([]byte(nil), row[i]...)
	}
	return clone
}
