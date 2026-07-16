package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
	ErrorStreamClosed = goerrors.New("stream is closed")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerContext struct {
	Message any
	// WALStart is the WAL start position for this decoded logical message.
	WALStart pq.LSN
	// AckLSN is the LSN associated with this message's acknowledgement.
	// In transaction-aware mode, only commit-boundary acknowledgements advance confirmed_flush_lsn.
	AckLSN pq.LSN
	Ack    func() error
}

type ListenerFunc func(ctx *ListenerContext)

type Message struct {
	message  any
	walStart pq.LSN
	ackLSN   pq.LSN
}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	OpenFromSnapshotLSN()
}

type stream struct {
	conn                pq.Connection
	metric              metric.Metric
	system              *pq.IdentifySystemResult
	relation            map[uint32]*format.Relation
	messageCH           chan *Message
	listenerFunc        ListenerFunc
	sinkEnd             chan struct{}
	processEnd          chan struct{}
	doneCtx             context.Context
	finish              context.CancelCauseFunc
	mu                  sync.RWMutex
	ackMu               sync.Mutex
	config              config.Config
	lastXLogPos         pq.LSN
	confirmedXLogPos    pq.LSN
	openFromSnapshotLSN bool
	closed              atomic.Bool
	sinkStarted         atomic.Bool
	processStarted      atomic.Bool
	closeOnce           sync.Once
	// connMu serializes every use of conn while streaming. ReceiveMessage toggles
	// the socket deadline, so feedback writes must not overlap it. connMu is
	// always acquired before mu, never the reverse.
	connMu sync.Mutex
}

func NewStream(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	doneCtx, finish := context.WithCancelCause(context.Background())
	return &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		sinkEnd:      make(chan struct{}),
		processEnd:   make(chan struct{}),
		doneCtx:      doneCtx,
		finish:       finish,
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	s.sinkStarted.Store(true)
	s.processStarted.Store(true)

	go s.sink(ctx)
	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}
		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn, s.config.Slot.ProtoVersion); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())
	} else {
		logger.Info("replication started from confirmed_flush_lsn", "slot", s.config.Slot.Name)
	}

	return nil
}

// messageBuffer manages a one-message look-ahead buffer.
//
// The last DML message in each transaction is held back so its WAL position
// can be rewritten to the transaction-end LSN (from COMMIT / STREAM COMMIT).
// All preceding messages are emitted immediately with their original position.
// This keeps memory usage O(1) regardless of transaction size.
type messageBuffer struct {
	pending *Message
	outCh   chan<- *Message
}

// flush emits the pending message (if any) with its original WAL position.
func (b *messageBuffer) flush() {
	if b.pending != nil {
		b.outCh <- b.pending
		b.pending = nil
	}
}

// flushWithLSN emits the pending message (if any), rewriting its WAL position
// to the given transaction-end LSN. Used at COMMIT.
func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		b.outCh <- &Message{
			message:  b.pending.message,
			walStart: b.pending.walStart,
			ackLSN:   lsn,
		}
		b.pending = nil
	}
}

// discard drops the pending message without emitting.
// Used at BEGIN to reset state.
func (b *messageBuffer) discard() {
	b.pending = nil
}

// buffer stores a new DML message, first flushing any previously pending one.
func (b *messageBuffer) buffer(msg *Message) {
	b.flush()
	b.pending = msg
}

// streamTxBuffer accumulates messages from streaming in-progress transactions.
//
// PostgreSQL streams large transactions in chunks (STREAM START / STREAM STOP)
// before the transaction is committed. Chunks from different transactions may
// be interleaved (e.g. TX-A chunk, TX-B chunk, TX-A chunk, …), so messages
// are stored per-XID in a map.
//
// Messages must NOT be delivered to the consumer until STREAM COMMIT arrives,
// because the transaction may still be rolled back (STREAM ABORT). This mirrors
// how PostgreSQL's own logical replication worker handles streaming: it writes
// to temporary storage and only applies on commit.
type streamTxBuffer struct {
	txns      map[uint32][]*Message
	activeXid uint32
	streaming bool
}

// startTx marks the beginning of a streaming chunk for the given XID.
func (s *streamTxBuffer) startTx(xid uint32) {
	if s.txns == nil {
		s.txns = make(map[uint32][]*Message)
	}
	s.activeXid = xid
	s.streaming = true
}

// append adds a message to the currently active streaming transaction.
func (s *streamTxBuffer) append(msg *Message) {
	if msg != nil {
		s.txns[s.activeXid] = append(s.txns[s.activeXid], msg)
	}
}

// stopTx marks the end of the current streaming chunk.
func (s *streamTxBuffer) stopTx() {
	s.streaming = false
}

// flushTx emits every accumulated message for the given XID through outCh.
// The last message's WAL position is rewritten to the transaction-end LSN.
func (s *streamTxBuffer) flushTx(xid uint32, outCh chan<- *Message, endLSN pq.LSN) {
	s.streaming = false
	msgs := s.txns[xid]
	n := len(msgs)
	for i, msg := range msgs {
		if i == n-1 {
			outCh <- &Message{
				message:  msg.message,
				walStart: msg.walStart,
				ackLSN:   endLSN,
			}
		} else {
			outCh <- msg
		}
	}
	delete(s.txns, xid)
}

// discardTx drops all accumulated messages for the given XID without emitting.
func (s *streamTxBuffer) discardTx(xid uint32) {
	s.streaming = false
	delete(s.txns, xid)
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")
	defer close(s.sinkEnd)

	buf := &messageBuffer{outCh: s.messageCH}
	streamErr := s.sinkLoop(ctx, buf, &streamTxBuffer{})
	s.finish(streamErr)
	close(s.messageCH)

	if streamErr != nil && !goerrors.Is(streamErr, context.Canceled) {
		logger.Error("postgres stream stopped", "error", streamErr)
	}
}

// sinkLoop reads raw replication messages and dispatches them until the
// connection is closed, its context is canceled, or a fatal error occurs.
func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	for {
		if err := s.Err(); err != nil {
			return err
		}

		msgCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		// Hold connMu only for the read itself. ReceiveMessage's deferred Unwatch
		// (which clears the socket deadline) has run by the time it returns, so the
		// deadline-toggle window is fully contained here; releasing before the
		// channel sends in handleXLogData keeps acks from blocking the sink and
		// vice versa.
		s.connMu.Lock()
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		s.connMu.Unlock()
		cancel()

		if terminalErr := s.Err(); terminalErr != nil {
			return terminalErr
		}
		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				return nil
			}
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if pgconn.Timeout(err) {
				if s.LoadXLogPos() > 0 {
					if err := s.sendStandbyStatusUpdate(ctx); err != nil {
						return fmt.Errorf("send standby status update: %w", err)
					}
					logger.Debug("send stand by status update")
				}
				continue
			}
			return fmt.Errorf("receive replication message: %w", err)
		}

		copyData, err := extractCopyData(rawMsg)
		if err != nil {
			return err
		}
		if copyData == nil {
			continue
		}
		if len(copyData.Data) == 0 {
			return goerrors.New("received empty replication copy data")
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			if err := s.handleKeepalive(ctx, copyData.Data[1:]); err != nil {
				return fmt.Errorf("handle primary keepalive: %w", err)
			}
		case message.XLogDataByteID:
			if err := s.handleXLogData(copyData.Data[1:], buf, streamBuf); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported replication copy data type %q", copyData.Data[0])
		}
	}
}

// extractCopyData returns replication payloads, ignores PostgreSQL sideband
// messages, and rejects terminal or unexpected backend messages.
func extractCopyData(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, error) {
	switch msg := rawMsg.(type) {
	case *pgproto3.CopyData:
		return msg, nil
	case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
		return nil, nil
	case *pgproto3.ErrorResponse:
		return nil, pgconn.ErrorResponseToPgError(msg)
	default:
		return nil, fmt.Errorf("unexpected replication backend message %T", rawMsg)
	}
}

// handleKeepalive processes a primary keepalive message, updating the WAL
// position and responding with a standby status update when requested.
func (s *stream) handleKeepalive(ctx context.Context, data []byte) error {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("decode primary keepalive message: %w", err)
	}

	if pkm.ServerWALEnd > 0 {
		s.UpdateXLogPos(pkm.ServerWALEnd)
		logger.Debug("updated xlog position from keepalive", "serverWALEnd", pkm.ServerWALEnd.String())
	}

	if pkm.ReplyRequested {
		if err := s.sendStandbyStatusUpdate(ctx); err != nil {
			return err
		}
		logger.Debug("standby status update sent on keepalive request")
	}

	return nil
}

// handleXLogData parses a WAL data message, decodes the logical replication
// event, and dispatches it through the message buffer.
func (s *stream) handleXLogData(data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	xld, err := ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xLog data: %w", err)
	}

	if len(xld.WALData) == 0 {
		return goerrors.New("received empty logical replication message")
	}
	logger.Debug("wal received",
		"messageType", string(xld.WALData[0]),
		"walStart", xld.WALStart,
		"walEnd", xld.ServerWALEnd,
		"serverTime", xld.ServerTime,
	)

	s.UpdateXLogPos(xld.ServerWALEnd)
	s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

	decodedMsg, err := message.New(xld.WALData, streamBuf.streaming, xld.ServerTime, s.relation)
	if err != nil {
		if ignorableLogicalMetadata(xld.WALData) {
			logger.Debug("ignoring logical metadata message", "type", string(xld.WALData[0]))
			return nil
		}
		return fmt.Errorf("decode logical message: %w", err)
	}
	if decodedMsg == nil {
		return goerrors.New("logical message decoder returned nil without error")
	}

	// add LSN to insert/update/delete messages
	switch m := decodedMsg.(type) {
	case *format.Insert:
		m.LSN = xld.WALStart
	case *format.Update:
		m.LSN = xld.WALStart
	case *format.Delete:
		m.LSN = xld.WALStart
	}

	s.dispatchMessage(decodedMsg, xld, buf, streamBuf)
	return nil
}

func ignorableLogicalMetadata(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	switch message.Type(data[0]) {
	case message.TypeByte, message.OriginByte:
		return true
	default:
		return false
	}
}

// dispatchMessage routes a decoded logical replication event to the correct
// buffer action.
//
// For regular (non-streaming) transactions the messageBuffer provides a
// one-message look-ahead so the last DML's WAL position can be rewritten to
// the transaction-end LSN at COMMIT.
//
// For streaming transactions (proto v2) messages are accumulated in the
// streamTxBuffer across STREAM START / STREAM STOP chunks. They are only
// emitted to the consumer on STREAM COMMIT and discarded on STREAM ABORT.
// This prevents uncommitted data from being delivered.
func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer, streamBuf *streamTxBuffer) {
	switch msg := decodedMsg.(type) {
	case *format.Begin:
		buf.discard()
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: msg, walStart: xld.WALStart, ackLSN: xld.WALStart}
		}

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: msg, walStart: xld.WALStart, ackLSN: msg.TransactionEndLSN}
		}

	case *format.StreamStart:
		// Beginning of a streaming chunk – DML events that follow belong
		// to an in-progress transaction and must be buffered per-XID.
		streamBuf.startTx(msg.Xid)

	case *format.StreamStop:
		// End of a streaming chunk. Nothing is emitted to the consumer.
		streamBuf.stopTx()

	case *format.StreamCommit:
		// Final commit of a streamed transaction – emit all messages for this XID.
		streamBuf.flushTx(msg.Xid, buf.outCh, msg.TransactionEndLSN)
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: msg, walStart: xld.WALStart, ackLSN: msg.TransactionEndLSN}
		}

	case *format.StreamAbort:
		// Streamed transaction rolled back – discard messages for this XID.
		streamBuf.discardTx(msg.Xid)

	default:
		// DML event (Insert, Update, Delete, Relation, …)
		m := &Message{
			message:  decodedMsg,
			walStart: xld.WALStart,
			ackLSN:   xld.WALStart,
		}
		if streamBuf.streaming {
			streamBuf.append(m)
		} else {
			buf.buffer(m)
		}
	}
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")
	defer func() {
		logger.Info("postgres message process stopped")
		close(s.processEnd)
	}()

	ackTracker := &transactionAckTracker{}
	for msg := range s.messageCH {
		if msg == nil || ctx.Err() != nil || s.doneCtx.Err() != nil {
			continue
		}

		ackFunc := s.ackFuncForMessage(msg, ackTracker)
		if s.isHeartbeatMessage(msg.message) {
			if err := ackFunc(); err != nil {
				s.finish(fmt.Errorf("heartbeat auto-ack: %w", err))
			}
			continue
		}

		lCtx := &ListenerContext{
			Message:  msg.message,
			WALStart: msg.walStart,
			AckLSN:   msg.ackLSN,
			Ack:      ackFunc,
		}

		switch lCtx.Message.(type) {
		case *format.Insert:
			s.metric.InsertOpIncrement(1)
		case *format.Delete:
			s.metric.DeleteOpIncrement(1)
		case *format.Update:
			s.metric.UpdateOpIncrement(1)
		}

		start := time.Now().UTC()
		s.listenerFunc(lCtx)
		s.metric.SetProcessLatency(time.Since(start).Nanoseconds())
	}
}

type transactionAckCheckpoint struct {
	lsn   pq.LSN
	acked bool
	next  *transactionAckCheckpoint
}

// transactionAckTracker keeps PostgreSQL's cumulative acknowledgement from
// passing an earlier commit that the consumer has not made durable yet.
type transactionAckTracker struct {
	head *transactionAckCheckpoint
	tail *transactionAckCheckpoint
	mu   sync.Mutex
}

func (t *transactionAckTracker) register(lsn pq.LSN) *transactionAckCheckpoint {
	checkpoint := &transactionAckCheckpoint{lsn: lsn}
	t.mu.Lock()
	if t.tail == nil {
		t.head = checkpoint
	} else {
		t.tail.next = checkpoint
	}
	t.tail = checkpoint
	t.mu.Unlock()
	return checkpoint
}

func (t *transactionAckTracker) ack(checkpoint *transactionAckCheckpoint, advance func(pq.LSN)) {
	t.mu.Lock()
	defer t.mu.Unlock()

	checkpoint.acked = true
	for t.head != nil && t.head.acked {
		advance(t.head.lsn)
		t.head = t.head.next
	}
	if t.head == nil {
		t.tail = nil
	}
}

func (s *stream) ackFuncForMessage(msg *Message, tracker *transactionAckTracker) func() error {
	transactionAware := s.config.Listener.EmitTransactionBoundaries
	var checkpoint *transactionAckCheckpoint
	if transactionAware && isTransactionCommitBoundary(msg.message) {
		checkpoint = tracker.register(msg.ackLSN)
	}

	return func() error {
		s.ackMu.Lock()
		defer s.ackMu.Unlock()
		if s.closed.Load() {
			return ErrorStreamClosed
		}
		if !transactionAware {
			s.UpdateConfirmedXLogPos(msg.ackLSN)
		} else if checkpoint != nil {
			tracker.ack(checkpoint, s.UpdateConfirmedXLogPos)
		}
		return nil
	}
}

func isTransactionCommitBoundary(msg any) bool {
	switch msg.(type) {
	case *format.Commit, *format.StreamCommit:
		return true
	default:
		return false
	}
}

func (s *stream) isHeartbeatMessage(msg any) bool {
	if !s.config.IsHeartbeatEnabled() {
		return false
	}

	hbSchema := s.config.Heartbeat.Table.Schema
	hbTable := s.config.Heartbeat.Table.Name

	switch m := msg.(type) {
	case *format.Insert:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Update:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Delete:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	}

	return false
}

func (s *stream) Close(ctx context.Context) {
	s.closeOnce.Do(func() {
		// Stop new delivery first, while keeping the connection available for an
		// acknowledgement from the listener callback already in flight.
		s.finish(context.Canceled)

		if s.sinkStarted.Load() {
			select {
			case <-s.sinkEnd:
				logger.Info("postgres message sink stopped")
			case <-ctx.Done():
				logger.Warn("timed out waiting for postgres message sink", "error", ctx.Err())
			}
		}

		if s.processStarted.Load() {
			select {
			case <-s.processEnd:
				logger.Info("postgres message process stopped")
			case <-ctx.Done():
				logger.Warn("timed out waiting for postgres message process", "error", ctx.Err())
			}
		}

		// Once the processor is done, no synchronous listener acknowledgement can
		// race this final flush. Asynchronous late acknowledgements fail closed.
		s.ackMu.Lock()
		s.closed.Store(true)
		if !s.conn.IsClosed() {
			s.flushFinalStandbyStatusUpdate(ctx)
			if err := s.conn.Close(ctx); err != nil {
				logger.Warn("close postgres connection", "error", err)
			} else {
				logger.Info("postgres connection closed")
			}
		}
		s.ackMu.Unlock()
	})
}

func (s *stream) Done() <-chan struct{} {
	return s.doneCtx.Done()
}

func (s *stream) Err() error {
	return context.Cause(s.doneCtx)
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) UpdateXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func (s *stream) UpdateConfirmedXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.confirmedXLogPos < l {
		s.confirmedXLogPos = l
	}
}

func (s *stream) LoadConfirmedXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.confirmedXLogPos
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

// fetchSnapshotLSN reads the completed snapshot checkpoint used to start CDC.
func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	conn, err := pq.NewConnection(ctx, s.config.DSN())
	if err != nil {
		return 0, errors.Wrap(err, "connect for snapshot LSN")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	query := fmt.Sprintf(`
		SELECT snapshot_lsn, completed
		FROM cdc_snapshot_job
		WHERE slot_name = %s
	`, pq.QuoteLiteral(s.config.Slot.Name))
	reader := conn.Exec(ctx, query)
	results, err := reader.ReadAll()
	if err != nil {
		_ = reader.Close()
		return 0, errors.Wrap(err, "read snapshot LSN")
	}
	if err := reader.Close(); err != nil {
		return 0, errors.Wrap(err, "close snapshot LSN result")
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, errors.New("snapshot job not found for slot: " + s.config.Slot.Name)
	}

	row := results[0].Rows[0]
	if len(row) < 2 {
		return 0, errors.New("invalid snapshot job row")
	}
	if completed := string(row[1]); completed != "t" && completed != "true" {
		return 0, errors.New("snapshot job not completed for slot: " + s.config.Slot.Name)
	}

	lsn, err := pq.ParseLSN(string(row[0]))
	if err != nil {
		return 0, errors.Wrap(err, "parse snapshot LSN")
	}
	logger.Info("fetched snapshot LSN", "slotName", s.config.Slot.Name, "snapshotLSN", lsn.String())
	return lsn, nil
}

// sendStandbyStatusUpdate writes a standby status update under connMu so it can
// never overlap the sink loop's ReceiveMessage, which toggles the connection's
// socket deadline. Every status-update write — idle keepalive,
// reply-on-request, and final close feedback — must go through here rather than
// calling SendStandbyStatusUpdate directly. See the connMu field comment.
func (s *stream) sendStandbyStatusUpdate(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()), uint64(s.LoadConfirmedXLogPos()))
}

func (s *stream) flushFinalStandbyStatusUpdate(ctx context.Context) {
	if s.LoadConfirmedXLogPos() == 0 {
		return
	}
	if err := s.sendStandbyStatusUpdate(ctx); err != nil {
		logger.Warn("final standby status update failed, updates may duplicate on restart", "error", err)
		return
	}
	logger.Debug("final standby status update sent")
}

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walReceivedPosition, walFlushedPosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walReceivedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.UTC().UnixMicro() - microSecFromUnixEpochToY2K)
}
