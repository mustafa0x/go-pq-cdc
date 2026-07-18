package replication

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type feedbackProbeConn struct {
	fe         *pgproto3.Frontend
	keepalive  []byte
	activeRead atomic.Int32
	overlap    atomic.Bool
	written    chan struct{}
}

func (c *feedbackProbeConn) Connect(context.Context) error                          { return nil }
func (c *feedbackProbeConn) IsClosed() bool                                         { return false }
func (c *feedbackProbeConn) Close(context.Context) error                            { return nil }
func (c *feedbackProbeConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
func (c *feedbackProbeConn) Frontend() *pgproto3.Frontend                           { return c.fe }

func (c *feedbackProbeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	c.activeRead.Add(1)
	defer c.activeRead.Add(-1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Millisecond):
		return &pgproto3.CopyData{Data: c.keepalive}, nil
	}
}

type feedbackProbeWriter struct{ conn *feedbackProbeConn }

func (w feedbackProbeWriter) Write(p []byte) (int, error) {
	if w.conn.activeRead.Load() != 0 {
		w.conn.overlap.Store(true)
	}
	select {
	case w.conn.written <- struct{}{}:
	default:
	}
	return len(p), nil
}

func TestAcknowledgementFlushesDuringContinuousWALTraffic(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	written := make(chan struct{}, 1)
	conn := &feedbackProbeConn{
		keepalive: make([]byte, 18),
		written:   written,
	}
	conn.keepalive[0] = message.PrimaryKeepaliveMessageByteID
	conn.fe = pgproto3.NewFrontend(strings.NewReader(""), feedbackProbeWriter{conn})

	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	stream.conn = conn
	stream.UpdateXLogPos(200)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- stream.sinkLoop(ctx, &messageBuffer{outCh: make(chan *Message, 1)}, &streamTxBuffer{})
	}()

	ack := stream.ackFuncForMessage(&Message{ackLSN: 150}, &transactionAckTracker{})
	if err := ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	select {
	case <-written:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("acknowledgement was not flushed while WAL traffic remained continuous")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("sinkLoop() error = %v, want context.Canceled", err)
	}
	if conn.overlap.Load() {
		t.Fatal("replication feedback write overlapped ReceiveMessage")
	}
}

func TestCloseSkipsFinalFeedbackWhileSinkIsRunning(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	written := make(chan struct{}, 1)
	conn := &feedbackProbeConn{written: written}
	conn.fe = pgproto3.NewFrontend(strings.NewReader(""), feedbackProbeWriter{conn})

	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	stream.conn = conn
	stream.sinkStarted.Store(true)
	stream.UpdateConfirmedXLogPos(100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream.Close(ctx)

	select {
	case <-written:
		t.Fatal("Close wrote feedback before the sink stopped")
	default:
	}
}
