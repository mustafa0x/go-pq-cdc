package replication

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestReplicationTestRejectsIncompleteStartup(t *testing.T) {
	conn := newReplicationStartConn()
	conn.messages = []pgproto3.BackendMessage{&pgproto3.ReadyForQuery{TxStatus: 'I'}}

	if err := New(conn).Test(context.Background()); err == nil {
		t.Fatal("Test() accepted startup without CopyBothResponse")
	}
}

func TestReplicationTestDrainsErrorResponse(t *testing.T) {
	conn := newReplicationStartConn()
	conn.messages = []pgproto3.BackendMessage{
		&pgproto3.ErrorResponse{Code: "55006", Message: "object in use"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	}

	err := New(conn).Test(context.Background())
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55006" {
		t.Fatalf("Test() error = %v, want PostgreSQL 55006", err)
	}
	if len(conn.messages) != 0 {
		t.Fatalf("Test() left %d startup messages unread", len(conn.messages))
	}
}

func TestReplicationTestIgnoresSidebandMessages(t *testing.T) {
	conn := newReplicationStartConn()
	conn.messages = []pgproto3.BackendMessage{
		&pgproto3.ParameterStatus{Name: "application_name", Value: "cdc"},
		&pgproto3.NotificationResponse{PID: 1, Channel: "events", Payload: "ready"},
		&pgproto3.CopyBothResponse{},
	}

	if err := New(conn).Test(context.Background()); err != nil {
		t.Fatalf("Test() error = %v", err)
	}
}

func TestStartReplicationRequestsOnlySupportedProtocolFeatures(t *testing.T) {
	for _, test := range []struct {
		name          string
		protoVersion  int
		messages      bool
		wantStreaming bool
	}{
		{name: "protocol 1", protoVersion: 1},
		{name: "protocol 1 with messages", protoVersion: 1, messages: true},
		{name: "protocol 2", protoVersion: 2, wantStreaming: true},
		{name: "protocol 2 with messages", protoVersion: 2, messages: true, wantStreaming: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := newReplicationStartConn()
			err := New(conn).Start("books_pub", "books_slot", pq.LSN(16), test.protoVersion, test.messages)
			if err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			query := conn.out.String()
			if got := strings.Contains(query, "messages 'true'"); got != test.messages {
				t.Fatalf("messages option present = %v, want %v; query bytes = %q", got, test.messages, query)
			}
			if got := strings.Contains(query, "streaming 'true'"); got != test.wantStreaming {
				t.Fatalf("streaming option present = %v, want %v; query bytes = %q", got, test.wantStreaming, query)
			}
		})
	}
}

type replicationStartConn struct {
	out      bytes.Buffer
	fe       *pgproto3.Frontend
	messages []pgproto3.BackendMessage
}

func newReplicationStartConn() *replicationStartConn {
	conn := &replicationStartConn{}
	conn.fe = pgproto3.NewFrontend(bytes.NewReader(nil), &conn.out)
	return conn
}

func (*replicationStartConn) Connect(context.Context) error { return nil }
func (*replicationStartConn) IsClosed() bool                { return false }
func (*replicationStartConn) Close(context.Context) error   { return nil }
func (c *replicationStartConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	if len(c.messages) == 0 {
		return nil, errors.New("no message")
	}
	message := c.messages[0]
	c.messages = c.messages[1:]
	return message, nil
}
func (c *replicationStartConn) Frontend() *pgproto3.Frontend { return c.fe }
func (*replicationStartConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	return nil
}
