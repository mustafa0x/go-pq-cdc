package timescaledb

import (
	"context"
	"testing"
	"time"
)

func TestCloseStopsSyncLoop(t *testing.T) {
	tdb := &TimescaleDB{stopCh: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		tdb.SyncHyperTables(context.Background())
		close(done)
	}()

	tdb.Close(context.Background())
	tdb.Close(context.Background())

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SyncHyperTables did not stop")
	}
}
