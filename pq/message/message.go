package message

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrorByteNotSupported = errors.New("message byte not supported")

type Type uint8

type Decoder struct {
	streamedTransaction bool
}

func NewDecoder() *Decoder {
	return &Decoder{}
}

func New(data []byte, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
	return NewDecoder().New(data, serverTime, relation)
}

func (d *Decoder) New(data []byte, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
	if len(data) == 0 {
		return nil, errors.Wrap(ErrorByteNotSupported, "empty message")
	}

	switch Type(data[0]) {
	case BeginByte:
		return format.NewBegin(data)
	case CommitByte:
		return format.NewCommit(data)
	case InsertByte:
		return format.NewInsert(data, d.streamedTransaction, relation, serverTime)
	case UpdateByte:
		return format.NewUpdate(data, d.streamedTransaction, relation, serverTime)
	case DeleteByte:
		return format.NewDelete(data, d.streamedTransaction, relation, serverTime)
	case TruncateByte:
		return format.NewTruncate(data, d.streamedTransaction, relation, serverTime)
	case StreamStartByte:
		d.streamedTransaction = true
		return format.NewStreamStart(data)
	case StreamStopByte:
		d.streamedTransaction = false
		return &format.StreamStop{}, nil
	case StreamAbortByte:
		d.streamedTransaction = false
		return format.NewStreamAbort(data)
	case StreamCommitByte:
		d.streamedTransaction = false
		return format.NewStreamCommit(data)
	case RelationByte:
		msg, err := format.NewRelation(data, d.streamedTransaction)
		if err == nil && relation != nil {
			relation[msg.OID] = msg
		}
		return msg, err
	default:
		return nil, errors.Wrap(ErrorByteNotSupported, string(data[0]))
	}
}
