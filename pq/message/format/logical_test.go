package format

import (
	"encoding/binary"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
)

func TestLogicalMessageDecode(t *testing.T) {
	data := logicalMessageBytes(false, 0, true, pq.LSN(0x1020), "rukn-sync", []byte("stream-id"))
	message, err := NewLogicalMessage(data, false)
	if err != nil {
		t.Fatal(err)
	}
	if message.XID != 0 || !message.Transactional || message.LSN != pq.LSN(0x1020) || message.Prefix != "rukn-sync" || string(message.Content) != "stream-id" {
		t.Fatalf("decoded logical message = %#v", message)
	}
}

func TestStreamedLogicalMessageDecode(t *testing.T) {
	data := logicalMessageBytes(true, 42, false, pq.LSN(0x2030), "audit", nil)
	message, err := NewLogicalMessage(data, true)
	if err != nil {
		t.Fatal(err)
	}
	if message.XID != 42 || message.Transactional || message.Prefix != "audit" || len(message.Content) != 0 {
		t.Fatalf("decoded streamed logical message = %#v", message)
	}
}

func TestLogicalMessageRejectsMalformedContent(t *testing.T) {
	data := logicalMessageBytes(false, 0, true, pq.LSN(1), "probe", []byte("ok"))
	binary.BigEndian.PutUint32(data[len(data)-6:len(data)-2], 5)
	if _, err := NewLogicalMessage(data, false); err == nil {
		t.Fatal("malformed logical message was accepted")
	}
}

func logicalMessageBytes(streamed bool, xid uint32, transactional bool, lsn pq.LSN, prefix string, content []byte) []byte {
	length := 1 + 1 + 8 + len(prefix) + 1 + 4 + len(content)
	if streamed {
		length += 4
	}
	data := make([]byte, length)
	offset := 0
	data[offset] = 'M'
	offset++
	if streamed {
		binary.BigEndian.PutUint32(data[offset:], xid)
		offset += 4
	}
	if transactional {
		data[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint64(data[offset:], uint64(lsn))
	offset += 8
	copy(data[offset:], prefix)
	offset += len(prefix) + 1
	binary.BigEndian.PutUint32(data[offset:], uint32(len(content)))
	offset += 4
	copy(data[offset:], content)
	return data
}
