package replication

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestDispatchDefaultKeepsRowOrientedListenerContract(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 4)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.Begin{FinalLSN: pq.LSN(19)}, XLogData{WALStart: pq.LSN(10)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(11)}, buf, streamBuf)
	stream.dispatchMessage(&format.Commit{TransactionEndLSN: pq.LSN(20)}, XLogData{WALStart: pq.LSN(20)}, buf, streamBuf)

	requireMessageCount(t, out, 1)
	msg := <-out
	if _, ok := msg.message.(*format.Insert); !ok {
		t.Fatalf("message = %T, want *format.Insert", msg.message)
	}
	if msg.walStart != pq.LSN(11) {
		t.Fatalf("walStart = %s, want 0/B", msg.walStart)
	}
	if msg.ackLSN != pq.LSN(20) {
		t.Fatalf("ackLSN = %s, want 0/14", msg.ackLSN)
	}
}

func TestDispatchCanEmitTransactionBoundaries(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 8)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.Begin{FinalLSN: pq.LSN(19)}, XLogData{WALStart: pq.LSN(10)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(11)}, buf, streamBuf)
	stream.dispatchMessage(&format.Update{TableName: "books"}, XLogData{WALStart: pq.LSN(12)}, buf, streamBuf)
	stream.dispatchMessage(&format.Commit{TransactionEndLSN: pq.LSN(20)}, XLogData{WALStart: pq.LSN(13)}, buf, streamBuf)

	requireMessageCount(t, out, 4)
	assertMessage[*format.Begin](t, <-out, pq.LSN(10), pq.LSN(10))
	assertMessage[*format.Insert](t, <-out, pq.LSN(11), pq.LSN(11))
	assertMessage[*format.Update](t, <-out, pq.LSN(12), pq.LSN(20))
	assertMessage[*format.Commit](t, <-out, pq.LSN(13), pq.LSN(20))
}

func TestDispatchCanEmitStreamCommitBoundary(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 8)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	stream.dispatchMessage(&format.StreamStart{Xid: 7}, XLogData{WALStart: pq.LSN(30)}, buf, streamBuf)
	stream.dispatchMessage(&format.Insert{TableName: "books"}, XLogData{WALStart: pq.LSN(31)}, buf, streamBuf)
	stream.dispatchMessage(&format.StreamStop{}, XLogData{WALStart: pq.LSN(32)}, buf, streamBuf)
	stream.dispatchMessage(&format.StreamCommit{Xid: 7, TransactionEndLSN: pq.LSN(40)}, XLogData{WALStart: pq.LSN(33)}, buf, streamBuf)

	requireMessageCount(t, out, 2)
	assertMessage[*format.Insert](t, <-out, pq.LSN(31), pq.LSN(40))
	assertMessage[*format.StreamCommit](t, <-out, pq.LSN(33), pq.LSN(40))
}

func TestProcessExposesWALMetadataToListener(t *testing.T) {
	received := make(chan ListenerContext, 1)
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		received <- ListenerContext{
			Message:  ctx.Message,
			WALStart: ctx.WALStart,
			AckLSN:   ctx.AckLSN,
		}
	}).(*stream)
	stream.messageCH <- &Message{
		message:  &format.Insert{TableName: "books"},
		walStart: pq.LSN(11),
		ackLSN:   pq.LSN(20),
	}
	close(stream.messageCH)

	go stream.process(context.Background())

	select {
	case ctx := <-received:
		if _, ok := ctx.Message.(*format.Insert); !ok {
			t.Fatalf("message = %T, want *format.Insert", ctx.Message)
		}
		if ctx.WALStart != pq.LSN(11) {
			t.Fatalf("WALStart = %s, want 0/B", ctx.WALStart)
		}
		if ctx.AckLSN != pq.LSN(20) {
			t.Fatalf("AckLSN = %s, want 0/14", ctx.AckLSN)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("listener was not called")
	}
}

func TestProcessDoesNotDeliverQueuedMessageAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {
		calls++
	}).(*stream)
	stream.messageCH <- &Message{message: &format.Insert{TableName: "queued"}}
	close(stream.messageCH)

	go stream.process(ctx)
	waitForProcessEnd(t, stream)

	if calls != 0 {
		t.Fatalf("listener calls = %d, want 0", calls)
	}
}

func TestProcessStopsAfterListenerCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {
		calls++
		cancel()
	}).(*stream)
	stream.messageCH <- &Message{message: &format.Insert{TableName: "first"}}
	stream.messageCH <- &Message{message: &format.Insert{TableName: "second"}}
	close(stream.messageCH)

	go stream.process(ctx)
	waitForProcessEnd(t, stream)

	if calls != 1 {
		t.Fatalf("listener calls = %d, want 1", calls)
	}
}

func TestDoneBroadcastsStableTerminalError(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	wantErr := errors.New("connection lost")
	stream.finish(wantErr)

	for range 2 {
		select {
		case <-stream.Done():
		case <-time.After(time.Second):
			t.Fatal("Done() did not broadcast completion")
		}
		if err := stream.Err(); !errors.Is(err, wantErr) {
			t.Fatalf("Err() = %v, want %v", err, wantErr)
		}
	}
}

func TestSinkRejectsEmptyCopyData(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	conn := newWriteOnlyConn()
	conn.receive = &pgproto3.CopyData{}
	conn.receiveErr = nil
	stream.conn = conn

	go stream.sink(context.Background())
	select {
	case <-stream.Done():
		if err := stream.Err(); err == nil || !strings.Contains(err.Error(), "empty replication copy data") {
			t.Fatalf("Err() = %v, want empty-copy-data error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not reject empty copy data")
	}
}

func TestSinkReportsReceiveFailure(t *testing.T) {
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	stream.conn = newWriteOnlyConn()

	go stream.sink(context.Background())
	select {
	case <-stream.Done():
		if err := stream.Err(); err == nil {
			t.Fatal("Err() = nil, want receive failure")
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not report receive failure")
	}
}

func TestProcessDefaultAckAdvancesConfirmedLSN(t *testing.T) {
	var ackErr error
	stream := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		ackErr = ctx.Ack()
	}).(*stream)
	stream.messageCH <- &Message{
		message:  &format.Insert{TableName: "books"},
		walStart: pq.LSN(11),
		ackLSN:   pq.LSN(20),
	}
	close(stream.messageCH)

	go stream.process(context.Background())
	waitForProcessEnd(t, stream)

	if ackErr != nil {
		t.Fatalf("Ack() error = %v", ackErr)
	}
	if got := stream.LoadConfirmedXLogPos(); got != pq.LSN(20) {
		t.Fatalf("confirmed LSN = %s, want 0/14", got)
	}
}

func TestProcessTransactionAwareAckAdvancesOnlyOrderedCommits(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	commitAcks := make([]func() error, 0, 2)
	var rowAckErr error
	stream := NewStream("", cfg, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		switch ctx.Message.(type) {
		case *format.Insert:
			rowAckErr = ctx.Ack()
		case *format.Commit, *format.StreamCommit:
			commitAcks = append(commitAcks, ctx.Ack)
		}
	}).(*stream)
	stream.messageCH <- &Message{message: &format.Insert{TableName: "books"}, walStart: pq.LSN(9), ackLSN: pq.LSN(10)}
	stream.messageCH <- &Message{message: &format.Commit{TransactionEndLSN: pq.LSN(10)}, walStart: pq.LSN(10), ackLSN: pq.LSN(10)}
	stream.messageCH <- &Message{message: &format.StreamCommit{Xid: 7, TransactionEndLSN: pq.LSN(20)}, walStart: pq.LSN(20), ackLSN: pq.LSN(20)}
	close(stream.messageCH)

	go stream.process(context.Background())
	waitForProcessEnd(t, stream)

	if rowAckErr != nil {
		t.Fatalf("row Ack() error = %v", rowAckErr)
	}
	if got := stream.LoadConfirmedXLogPos(); got != 0 {
		t.Fatalf("confirmed LSN after row Ack() = %s, want 0/0", got)
	}
	if len(commitAcks) != 2 {
		t.Fatalf("commit Ack count = %d, want 2", len(commitAcks))
	}
	if err := commitAcks[1](); err != nil {
		t.Fatalf("second commit Ack() error = %v", err)
	}
	if got := stream.LoadConfirmedXLogPos(); got != 0 {
		t.Fatalf("confirmed LSN after second commit Ack() = %s, want 0/0", got)
	}
	if err := commitAcks[0](); err != nil {
		t.Fatalf("first commit Ack() error = %v", err)
	}
	if got := stream.LoadConfirmedXLogPos(); got != pq.LSN(20) {
		t.Fatalf("confirmed LSN after ordered commit Acks = %s, want 0/14", got)
	}
}

func requireMessageCount(t *testing.T, ch <-chan *Message, want int) {
	t.Helper()
	if len(ch) != want {
		t.Fatalf("message count = %d, want %d", len(ch), want)
	}
}

func assertMessage[T any](t *testing.T, msg *Message, walStart pq.LSN, ackLSN pq.LSN) {
	t.Helper()
	if _, ok := msg.message.(T); !ok {
		t.Fatalf("message = %T, want %T", msg.message, *new(T))
	}
	if msg.walStart != walStart {
		t.Fatalf("walStart = %s, want %s", msg.walStart, walStart)
	}
	if msg.ackLSN != ackLSN {
		t.Fatalf("ackLSN = %s, want %s", msg.ackLSN, ackLSN)
	}
}

func waitForProcessEnd(t *testing.T, stream *stream) {
	t.Helper()
	select {
	case <-stream.processEnd:
	case <-time.After(time.Second):
		t.Fatal("processor did not finish")
	}
}

type writeOnlyConn struct {
	out        *bytes.Buffer
	receive    pgproto3.BackendMessage
	receiveErr error
}

func newWriteOnlyConn() *writeOnlyConn {
	return &writeOnlyConn{out: &bytes.Buffer{}, receiveErr: errors.New("not implemented")}
}

func (c *writeOnlyConn) Connect(context.Context) error { return nil }
func (c *writeOnlyConn) IsClosed() bool                { return false }
func (c *writeOnlyConn) Close(context.Context) error   { return nil }
func (c *writeOnlyConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return c.receive, c.receiveErr
}
func (c *writeOnlyConn) Frontend() *pgproto3.Frontend {
	return pgproto3.NewFrontend(bytes.NewReader(nil), c.out)
}
func (c *writeOnlyConn) Exec(context.Context, string) *pgconn.MultiResultReader { return nil }
