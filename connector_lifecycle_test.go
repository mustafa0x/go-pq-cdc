package cdc

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
)

type testServer struct {
	listenCalls   atomic.Int32
	shutdownCalls atomic.Int32
}

func (s *testServer) Listen()   { s.listenCalls.Add(1) }
func (s *testServer) Shutdown() { s.shutdownCalls.Add(1) }

type testStream struct {
	done chan struct{}
	err  error
}

func (s *testStream) Connect(context.Context) error           { return nil }
func (s *testStream) Open(context.Context) error              { return nil }
func (s *testStream) Close(context.Context)                   {}
func (s *testStream) Done() <-chan struct{}                   { return s.done }
func (s *testStream) Err() error                              { return s.err }
func (s *testStream) GetSystemInfo() *pq.IdentifySystemResult { return nil }
func (s *testStream) GetMetric() metric.Metric                { return nil }
func (s *testStream) UpdateXLogPos(pq.LSN)                    {}

func newTestConnector() (*connector, *testServer) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	server := &testServer{}
	return &connector{
		server:  server,
		readyCh: make(chan struct{}),
		stopCh:  make(chan struct{}),
	}, server
}

func TestStartCancellationCleansUpWithoutStartingServer(t *testing.T) {
	connector, server := newTestConnector()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := connector.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want context.Canceled", err)
	}
	connector.Close()

	if got := server.listenCalls.Load(); got != 0 {
		t.Fatalf("server Listen calls = %d, want 0", got)
	}
	if got := server.shutdownCalls.Load(); got != 1 {
		t.Fatalf("server Shutdown calls = %d, want 1", got)
	}
	select {
	case <-connector.stopCh:
	default:
		t.Fatal("Start() returned without closing the connector")
	}
	if err := connector.Start(context.Background()); !errors.Is(err, ErrConnectorStarted) {
		t.Fatalf("second Start() error = %v, want ErrConnectorStarted", err)
	}
}

func TestCloseCancelsRunContext(t *testing.T) {
	connector, _ := newTestConnector()
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
	}()
	connector.runMu.Lock()
	connector.runCancel = cancel
	connector.runDone = done
	connector.runMu.Unlock()

	connector.Close()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the connector run context")
	}
}

func TestStartAfterCloseReturnsCanceled(t *testing.T) {
	connector, _ := newTestConnector()
	connector.Close()

	if err := connector.Start(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() error = %v, want context.Canceled", err)
	}
}

func TestWaitUntilReadyBroadcastsReadiness(t *testing.T) {
	connector, _ := newTestConnector()
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
	connector, _ := newTestConnector()
	connector.Close()

	if err := connector.WaitUntilReady(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitUntilReady() error = %v, want context.Canceled", err)
	}
}

func TestWaitUntilReadyRemainsReadyAfterStop(t *testing.T) {
	connector, _ := newTestConnector()
	connector.signalReady()
	connector.Close()

	if err := connector.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("WaitUntilReady() error = %v, want nil", err)
	}
}

func TestWaitUntilStoppedReturnsReplicationError(t *testing.T) {
	connector, _ := newTestConnector()
	wantErr := errors.New("connection lost")
	stream := &testStream{done: make(chan struct{}), err: wantErr}
	connector.stream = stream
	close(stream.done)

	if err := connector.waitUntilStopped(context.Background(), context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("waitUntilStopped() error = %v, want %v", err, wantErr)
	}
	connector.Close()
}

func TestWaitUntilStoppedReturnsStreamCancellation(t *testing.T) {
	connector, _ := newTestConnector()
	stream := &testStream{done: make(chan struct{}), err: context.Canceled}
	connector.stream = stream
	close(stream.done)

	if err := connector.waitUntilStopped(context.Background(), context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitUntilStopped() error = %v, want context.Canceled", err)
	}
	connector.Close()
}
