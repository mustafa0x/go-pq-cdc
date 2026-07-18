package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
)

func TestOpenFromSnapshotLSNAtUsesDeliveredGeneration(t *testing.T) {
	stream := &stream{}
	stream.OpenFromSnapshotLSNAt(pq.LSN(16))
	if !stream.openFromSnapshotLSN {
		t.Fatal("snapshot LSN mode is disabled")
	}
	if stream.snapshotLSN != pq.LSN(16) {
		t.Fatalf("snapshot LSN = %s; want 0/10", stream.snapshotLSN)
	}
}

func TestOpenFromSnapshotLSNPreservesLegacyLookup(t *testing.T) {
	stream := &stream{}
	stream.OpenFromSnapshotLSN()
	if !stream.openFromSnapshotLSN {
		t.Fatal("snapshot LSN mode is disabled")
	}
	if stream.snapshotLSN != 0 {
		t.Fatalf("snapshot LSN = %s; want metadata lookup", stream.snapshotLSN)
	}
}
