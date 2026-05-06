package format

import (
	"bytes"
	"encoding/binary"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

type Relation struct {
	Namespace     string
	Name          string
	Columns       []tuple.RelationColumn
	OID           uint32
	XID           uint32
	ColumnNumbers uint16
	ReplicaID     uint8
}

func NewRelation(data []byte, streamedTransaction bool) (*Relation, error) {
	msg := &Relation{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Relation) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if err := requireMessageBytes(data, skipByte, 4, "streamed transaction relation xid"); err != nil {
			return err
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if err := requireMessageBytes(data, skipByte, 4, "relation oid"); err != nil {
		return err
	}
	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var usedByteCount int
	m.Namespace, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return errors.New("relation message namespace decode error")
	}
	skipByte += usedByteCount

	m.Name, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return errors.New("relation message name decode error")
	}
	skipByte += usedByteCount

	if err := requireMessageBytes(data, skipByte, 3, "relation replica identity and column count"); err != nil {
		return err
	}
	m.ReplicaID = data[skipByte]
	skipByte++

	m.ColumnNumbers = binary.BigEndian.Uint16(data[skipByte:])
	skipByte += 2

	m.Columns = make([]tuple.RelationColumn, m.ColumnNumbers)
	for i := range m.Columns {
		if err := requireMessageBytes(data, skipByte, 1, "relation column flags"); err != nil {
			return errors.Wrapf(err, "column %d", i)
		}

		col := tuple.RelationColumn{}
		col.Flags = data[skipByte]
		skipByte++

		col.Name, usedByteCount = decodeString(data[skipByte:])
		if usedByteCount < 0 {
			return errors.Newf("relation message columns[%d].name decode error", i)
		}
		skipByte += usedByteCount

		if err := requireMessageBytes(data, skipByte, 8, "relation column type metadata"); err != nil {
			return errors.Wrapf(err, "column %d", i)
		}
		col.DataType = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4

		col.TypeModifier = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4

		m.Columns[i] = col
	}

	return nil
}

func decodeString(data []byte) (string, int) {
	end := bytes.IndexByte(data, byte(0))
	if end == -1 {
		return "", -1
	}

	return string(data[:end]), end + 1
}

func requireMessageBytes(data []byte, offset, size int, field string) error {
	if offset < 0 {
		return errors.Newf("%s offset must not be negative: %d", field, offset)
	}
	if size < 0 {
		return errors.Newf("%s size must not be negative: %d", field, size)
	}
	if len(data)-offset < size {
		return errors.Newf("%s requires %d byte at offset %d, but message has %d byte remaining", field, size, offset, max(len(data)-offset, 0))
	}
	return nil
}
