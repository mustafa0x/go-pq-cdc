package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/capture"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
	ErrorStreamClosed = errors.New("stream is closed")
)

const (
	StandbyStatusUpdateByteID = 'r'
	streamReceivePollInterval = 300 * time.Millisecond
	standbyStatusInterval     = 10 * time.Second
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
	xid      uint32
	autoAck  bool
}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
}

// SessionStreamer exposes an exact start position and listener quiescence for
// callers that own the replication session from bootstrap through shutdown.
type SessionStreamer interface {
	Streamer
	OpenAt(ctx context.Context, lsn pq.LSN) error
	Wait(ctx context.Context) error
	Done() <-chan struct{}
	Err() error
}

type stream struct {
	conn             pq.Connection
	metric           metric.Metric
	system           *pq.IdentifySystemResult
	relation         map[uint32]*format.Relation
	capturePlan      *capture.Plan
	messageCH        chan *Message
	listenerFunc     ListenerFunc
	sinkEnd          chan struct{}
	processEnd       chan struct{}
	feedbackCh       chan struct{}
	doneCtx          context.Context
	finish           context.CancelCauseFunc
	mu               sync.RWMutex
	ackMu            sync.Mutex
	config           config.Config
	lastXLogPos      pq.LSN
	confirmedXLogPos pq.LSN
	closed           atomic.Bool
	sinkStarted      atomic.Bool
	processStarted   atomic.Bool
	closeOnce        sync.Once
}

func NewStream(dsn string, cfg config.Config, plan *capture.Plan, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	return NewStreamWithConnection(pq.NewConnectionTemplate(dsn), cfg, plan, m, listenerFunc)
}

// NewSessionStream constructs a stream around a caller-owned replication
// session and the package's standard metric collector.
func NewSessionStream(conn pq.Connection, cfg config.Config, plan *capture.Plan, listenerFunc ListenerFunc) SessionStreamer {
	return NewStreamWithConnection(conn, cfg, plan, metric.NewMetric(cfg.Slot.Name), listenerFunc)
}

func NewStreamWithConnection(conn pq.Connection, cfg config.Config, plan *capture.Plan, m metric.Metric, listenerFunc ListenerFunc) SessionStreamer {
	doneCtx, finish := context.WithCancelCause(context.Background())
	return &stream{
		conn:         conn,
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		capturePlan:  plan,
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		sinkEnd:      make(chan struct{}),
		processEnd:   make(chan struct{}),
		feedbackCh:   make(chan struct{}, 1),
		doneCtx:      doneCtx,
		finish:       finish,
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return fmt.Errorf("stream connection: %w", err)
	}
	if err := capture.SetTextFormat(ctx, s.conn); err != nil {
		_ = s.conn.Close(ctx)
		return fmt.Errorf("stream text format: %w", err)
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return fmt.Errorf("identify system: %w", err)
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		var v *pgconn.PgError
		if errors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return fmt.Errorf("replication setup: %w", err)
	}

	s.sinkStarted.Store(true)
	s.processStarted.Store(true)

	go s.sink(ctx)
	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	if s.capturePlan != nil {
		if err := s.capturePlan.Validate(ctx, s.conn, s.config.Publication.Name); err != nil {
			return err
		}
	}
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn, s.config.Slot.ProtoVersion, s.config.Slot.Messages); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	logger.Info("replication started", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())

	return nil
}

// messageBuffer manages a one-message look-ahead buffer.
//
// The last DML message in each transaction is held back so its WAL position
// can be rewritten to the transaction-end LSN (from COMMIT / STREAM COMMIT).
// All preceding messages are emitted immediately with their original position.
// This keeps memory usage O(1) regardless of transaction size.
type messageBuffer struct {
	pending       *Message
	outCh         chan<- *Message
	inTransaction bool
}

func (b *messageBuffer) flush() {
	if b.pending != nil {
		b.outCh <- b.pending
		b.pending = nil
	}
}

func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		b.pending.ackLSN = lsn
		b.flush()
	}
}

func (b *messageBuffer) discard() {
	b.pending = nil
}

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
	txns      map[uint32]*streamedTx
	activeXid uint32
	streaming bool
	deferred  []*Message
}

type streamedTx struct {
	messages      []*Message
	baseRelations map[uint32]*format.Relation
	relations     map[uint32]*format.Relation
}

func (s *streamTxBuffer) startTx(xid uint32, relations map[uint32]*format.Relation) {
	if s.txns == nil {
		s.txns = make(map[uint32]*streamedTx)
	}
	if _, exists := s.txns[xid]; !exists {
		base := maps.Clone(relations)
		s.txns[xid] = &streamedTx{baseRelations: base, relations: maps.Clone(base)}
	}
	s.activeXid = xid
	s.streaming = true
}

func (s *streamTxBuffer) append(msg *Message, xid uint32) {
	msg.xid = xid
	tx := s.txns[s.activeXid]
	tx.messages = append(tx.messages, msg)
}

func (s *streamTxBuffer) relationCache() map[uint32]*format.Relation {
	return s.txns[s.activeXid].relations
}

func (s *streamTxBuffer) stopTx() {
	s.streaming = false
}

func (s *streamTxBuffer) commitTx(xid uint32, endLSN pq.LSN) []*Message {
	s.streaming = false
	tx := s.txns[xid]
	msgs := tx.messages
	if len(msgs) > 0 {
		msgs[len(msgs)-1].ackLSN = endLSN
	}
	delete(s.txns, xid)
	return msgs
}

func (s *streamTxBuffer) abortTx(xid, subxid uint32) {
	s.streaming = false
	if xid == subxid {
		delete(s.txns, xid)
		return
	}
	tx := s.txns[xid]
	for i, message := range tx.messages {
		if message.xid == subxid {
			tx.messages = tx.messages[:i]
			tx.relations = maps.Clone(tx.baseRelations)
			for _, retained := range tx.messages {
				if relation, ok := retained.message.(*format.Relation); ok {
					tx.relations[relation.OID] = relation
				}
			}
			return
		}
	}
}

func (s *streamTxBuffer) flushDeferred(out chan<- *Message) {
	if len(s.txns) > 0 {
		return
	}
	for _, message := range s.deferred {
		out <- message
	}
	s.deferred = nil
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")
	defer close(s.sinkEnd)

	buf := &messageBuffer{outCh: s.messageCH}
	streamErr := s.sinkLoop(ctx, buf, &streamTxBuffer{})
	s.finish(streamErr)
	close(s.messageCH)

	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		logger.Error("postgres stream stopped", "error", streamErr)
	}
}

// sinkLoop reads raw replication messages and dispatches them until the
// connection is closed, its context is canceled, or a fatal error occurs.
func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	nextStatusUpdate := time.Now().Add(standbyStatusInterval)
	for {
		if err := s.Err(); err != nil {
			return err
		}

		now := time.Now()
		statusDue := !now.Before(nextStatusUpdate)
		select {
		case <-s.feedbackCh:
			statusDue = true
		default:
		}
		if statusDue {
			if s.LoadXLogPos() > 0 {
				if err := s.sendStandbyStatusUpdate(ctx); err != nil {
					return fmt.Errorf("send standby status update: %w", err)
				}
			}
			nextStatusUpdate = now.Add(standbyStatusInterval)
		}

		msgCtx, cancel := context.WithTimeout(ctx, streamReceivePollInterval)
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
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
			return errors.New("received empty replication copy data")
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			replyRequested, err := s.handleKeepalive(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("handle primary keepalive: %w", err)
			}
			if replyRequested {
				if err := s.sendStandbyStatusUpdate(ctx); err != nil {
					return fmt.Errorf("reply to primary keepalive: %w", err)
				}
				nextStatusUpdate = time.Now().Add(standbyStatusInterval)
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

func (s *stream) handleKeepalive(data []byte) (bool, error) {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		return false, fmt.Errorf("decode primary keepalive message: %w", err)
	}
	if pkm.ServerWALEnd > 0 {
		s.UpdateXLogPos(pkm.ServerWALEnd)
	}
	return pkm.ReplyRequested, nil
}

// handleXLogData parses a WAL data message, decodes the logical replication
// event, and dispatches it through the message buffer.
func (s *stream) handleXLogData(data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	xld, err := ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xLog data: %w", err)
	}

	if len(xld.WALData) == 0 {
		return errors.New("received empty logical replication message")
	}
	logger.Debug("wal received",
		"messageType", string(xld.WALData[0]),
		"walStart", xld.WALStart,
		"walEnd", xld.ServerWALEnd,
		"serverTime", xld.ServerTime,
	)

	s.UpdateXLogPos(xld.ServerWALEnd)
	s.metric.SetCDCLatency(time.Since(xld.ServerTime).Nanoseconds())

	relations := s.relation
	if streamBuf.streaming {
		relations = streamBuf.relationCache()
	}
	decodedMsg, err := message.New(xld.WALData, streamBuf.streaming, xld.ServerTime, relations)
	if err != nil {
		if ignorableLogicalMetadata(xld.WALData, err) {
			logger.Debug("ignoring logical metadata message", "type", string(xld.WALData[0]))
			return nil
		}
		return fmt.Errorf("decode logical message: %w", err)
	}
	if decodedMsg == nil {
		return errors.New("logical message decoder returned nil without error")
	}
	if s.capturePlan != nil && !streamBuf.streaming {
		if relation, ok := decodedMsg.(*format.Relation); ok {
			if err := s.capturePlan.ValidateRelation(relation); err != nil {
				return fmt.Errorf("validate relation: %w", err)
			}
		} else if err := s.capturePlan.NormalizeMessage(decodedMsg); err != nil {
			return fmt.Errorf("normalize capture message: %w", err)
		}
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

	return s.dispatchMessage(decodedMsg, xld, buf, streamBuf)
}

func ignorableLogicalMetadata(data []byte, err error) bool {
	if len(data) == 0 || !errors.Is(err, message.ErrorByteNotSupported) {
		return false
	}
	typeByte := message.Type(data[0])
	return typeByte == message.TypeByte || typeByte == message.OriginByte
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
func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer, streamBuf *streamTxBuffer) error {
	switch msg := decodedMsg.(type) {
	case *format.Begin:
		buf.discard()
		buf.inTransaction = true
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: msg, walStart: xld.WALStart, ackLSN: xld.WALStart}
		}

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: msg, walStart: xld.WALStart, ackLSN: msg.TransactionEndLSN}
		}
		buf.inTransaction = false
		streamBuf.flushDeferred(buf.outCh)

	case *format.StreamStart:
		streamBuf.startTx(msg.Xid, s.relation)

	case *format.StreamStop:
		streamBuf.stopTx()

	case *format.StreamCommit:
		messages := streamBuf.commitTx(msg.Xid, msg.TransactionEndLSN)
		if err := s.commitStreamedMessages(messages); err != nil {
			return err
		}
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: &format.Begin{FinalLSN: msg.CommitLSN, CommitTime: msg.CommitTime, Xid: msg.Xid}, walStart: xld.WALStart, ackLSN: xld.WALStart}
		}
		for _, message := range messages {
			buf.outCh <- message
		}
		if s.config.Listener.EmitTransactionBoundaries {
			buf.outCh <- &Message{message: &format.Commit{Flags: msg.Flags, CommitLSN: msg.CommitLSN, TransactionEndLSN: msg.TransactionEndLSN, CommitTime: msg.CommitTime}, walStart: xld.WALStart, ackLSN: msg.TransactionEndLSN}
		}
		if !buf.inTransaction {
			streamBuf.flushDeferred(buf.outCh)
		}

	case *format.StreamAbort:
		streamBuf.abortTx(msg.Xid, msg.SubXid)
		if !buf.inTransaction {
			streamBuf.flushDeferred(buf.outCh)
		}

	case *format.LogicalMessage:
		message := &Message{message: msg, walStart: xld.WALStart, ackLSN: xld.WALStart}
		if msg.Transactional {
			if streamBuf.streaming {
				streamBuf.append(message, msg.XID)
			} else {
				buf.buffer(message)
			}
			return nil
		}
		message.autoAck = s.config.Listener.EmitTransactionBoundaries
		if message.autoAck && (buf.inTransaction || len(streamBuf.txns) > 0) {
			streamBuf.deferred = append(streamBuf.deferred, message)
		} else {
			buf.outCh <- message
		}

	case *format.Relation:
		message := &Message{message: msg, walStart: xld.WALStart, ackLSN: xld.WALStart}
		if streamBuf.streaming {
			streamBuf.relationCache()[msg.OID] = msg
			streamBuf.append(message, msg.XID)
		} else {
			s.relation[msg.OID] = msg
			buf.buffer(message)
		}

	default:
		m := &Message{
			message:  decodedMsg,
			walStart: xld.WALStart,
			ackLSN:   xld.WALStart,
		}
		if streamBuf.streaming {
			streamBuf.append(m, streamedMessageXID(decodedMsg))
		} else {
			buf.buffer(m)
		}
	}
	return nil
}

func (s *stream) commitStreamedMessages(messages []*Message) error {
	for _, message := range messages {
		if relation, ok := message.message.(*format.Relation); ok {
			if s.capturePlan != nil {
				if err := s.capturePlan.ValidateRelation(relation); err != nil {
					return fmt.Errorf("validate relation: %w", err)
				}
			}
		} else if s.capturePlan != nil {
			if err := s.capturePlan.NormalizeMessage(message.message); err != nil {
				return fmt.Errorf("normalize capture message: %w", err)
			}
		}
	}
	for _, message := range messages {
		if relation, ok := message.message.(*format.Relation); ok {
			s.relation[relation.OID] = relation
		}
	}
	return nil
}

func streamedMessageXID(message any) uint32 {
	switch message := message.(type) {
	case *format.Insert:
		return message.XID
	case *format.Update:
		return message.XID
	case *format.Delete:
		return message.XID
	case *format.Truncate:
		return message.XID
	case *format.Relation:
		return message.XID
	default:
		return 0
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
		if ctx.Err() != nil || s.doneCtx.Err() != nil {
			continue
		}

		ackFunc := s.ackFuncForMessage(msg, ackTracker)
		if msg.autoAck {
			if err := ackFunc(); err != nil {
				s.finish(fmt.Errorf("logical message auto-ack: %w", err))
			}
			continue
		}
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

		start := time.Now()
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
	if transactionAware {
		switch {
		case msg.autoAck:
			checkpoint = tracker.register(msg.ackLSN)
		default:
			switch msg.message.(type) {
			case *format.Commit:
				checkpoint = tracker.register(msg.ackLSN)
			}
		}
	}

	return func() error {
		s.ackMu.Lock()
		defer s.ackMu.Unlock()
		if s.closed.Load() {
			return ErrorStreamClosed
		}
		if transactionAware {
			if checkpoint == nil {
				return nil
			}
			tracker.ack(checkpoint, s.UpdateConfirmedXLogPos)
		} else {
			s.UpdateConfirmedXLogPos(msg.ackLSN)
		}
		s.requestFeedback()
		return nil
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

		sinkStopped := !s.sinkStarted.Load()
		if !sinkStopped {
			select {
			case <-s.sinkEnd:
				sinkStopped = true
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
			if sinkStopped && s.LoadConfirmedXLogPos() > 0 {
				if err := s.sendStandbyStatusUpdate(ctx); err != nil {
					logger.Warn("final standby status update failed, updates may duplicate on restart", "error", err)
				} else {
					logger.Debug("final standby status update sent")
				}
			}
			if err := s.conn.Close(ctx); err != nil {
				logger.Warn("close postgres connection", "error", err)
			} else {
				logger.Info("postgres connection closed")
			}
		}
		s.ackMu.Unlock()
	})
}

func (s *stream) Wait(ctx context.Context) error {
	if !s.processStarted.Load() {
		return nil
	}
	select {
	case <-s.processEnd:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
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

func (s *stream) OpenAt(ctx context.Context, lsn pq.LSN) error {
	s.UpdateXLogPos(lsn)
	s.UpdateConfirmedXLogPos(lsn)
	return s.Open(ctx)
}

func (s *stream) requestFeedback() {
	select {
	case s.feedbackCh <- struct{}{}:
	default:
	}
}

// sendStandbyStatusUpdate is called only by the sink goroutine, or after the
// sink has stopped during final cleanup. This keeps replication socket writes
// single-owned without a connection mutex.
func (s *stream) sendStandbyStatusUpdate(ctx context.Context) error {
	confirmed := s.LoadConfirmedXLogPos()
	return SendStandbyStatusUpdate(ctx, s.conn, uint64(confirmed), uint64(confirmed))
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
	return uint64(t.UnixMicro() - microSecFromUnixEpochToY2K)
}
