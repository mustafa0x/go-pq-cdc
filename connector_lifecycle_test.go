package cdc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
)

type testServer struct{}

func (testServer) Listen()   {}
func (testServer) Shutdown() {}

func newTestConnector() *connector {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	return &connector{
		server:   testServer{},
		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}),
		stopCh:   make(chan struct{}),
	}
}

func TestWaitUntilReadyBroadcastsReadiness(t *testing.T) {
	connector := newTestConnector()
	results := make(chan error, 2)

	for range 2 {
		go func() {
			results <- connector.WaitUntilReady(context.Background())
		}()
	}

	connector.signalReady()
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("WaitUntilReady() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("WaitUntilReady did not observe readiness")
		}
	}
}

func TestWaitUntilReadyStopsBeforeReadiness(t *testing.T) {
	connector := newTestConnector()
	connector.Close()

	if err := connector.WaitUntilReady(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitUntilReady() error = %v, want context.Canceled", err)
	}
}

func TestWaitUntilReadyRemainsReadyAfterStop(t *testing.T) {
	connector := newTestConnector()
	connector.signalReady()
	connector.Close()

	if err := connector.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("WaitUntilReady() error = %v, want nil", err)
	}
}
