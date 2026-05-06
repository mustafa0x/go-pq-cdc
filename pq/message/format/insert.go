package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

const (
	InsertTupleDataType = 'N'
)

type Insert struct {
	MessageTime    time.Time
	TupleData      *tuple.Data
	Decoded        map[string]any
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
}

func NewInsert(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Insert, error) {
	msg := &Insert{
		MessageTime: serverTime,
	}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	msg.TableNamespace = rel.Namespace
	msg.TableName = rel.Name

	msg.Decoded = make(map[string]any)

	var err error
	msg.Decoded, err = msg.TupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Insert) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if err := requireMessageBytes(data, skipByte, 4, "streamed transaction insert xid"); err != nil {
			return err
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if err := requireMessageBytes(data, skipByte, 4, "insert relation oid"); err != nil {
		return err
	}
	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var err error

	m.TupleData, err = tuple.NewData(data, InsertTupleDataType, skipByte)
	if err != nil {
		return errors.Wrap(err, "insert message")
	}

	return nil
}
