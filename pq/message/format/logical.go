package format

import (
	"encoding/binary"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// LogicalMessage is emitted by pgoutput when messages=true.
type LogicalMessage struct {
	XID           uint32
	Transactional bool
	LSN           pq.LSN
	Prefix        string
	Content       []byte
}

func NewLogicalMessage(data []byte, streamedTransaction bool) (*LogicalMessage, error) {
	message := &LogicalMessage{}
	if err := message.decode(data, streamedTransaction); err != nil {
		return nil, err
	}
	return message, nil
}

func (m *LogicalMessage) decode(data []byte, streamedTransaction bool) error {
	offset := 1
	if streamedTransaction {
		if err := requireMessageBytes(data, offset, 4, "logical message xid"); err != nil {
			return err
		}
		m.XID = binary.BigEndian.Uint32(data[offset:])
		offset += 4
	}

	if err := requireMessageBytes(data, offset, 9, "logical message flags and lsn"); err != nil {
		return err
	}
	flags := data[offset]
	if flags > 1 {
		return errors.Newf("logical message flags must be 0 or 1, got %d", flags)
	}
	m.Transactional = flags == 1
	offset++
	m.LSN = pq.LSN(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	var used int
	m.Prefix, used = decodeString(data[offset:])
	if used < 0 {
		return errors.New("logical message prefix decode error")
	}
	offset += used

	if err := requireMessageBytes(data, offset, 4, "logical message content length"); err != nil {
		return err
	}
	contentLength := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if err := requireMessageBytes(data, offset, contentLength, "logical message content"); err != nil {
		return err
	}
	if len(data) != offset+contentLength {
		return errors.Newf("logical message has %d trailing byte", len(data)-offset-contentLength)
	}
	m.Content = append([]byte(nil), data[offset:]...)
	return nil
}
