package replication

import (
	"encoding/binary"
	"errors"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/stretchr/testify/assert"
)

func TestHandleXLogDataUpdatesReceivedPositionForIgnoredMetadata(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	stream := NewStream("", config.Config{}, nil, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	walEnd := pq.LSN(0x16B6D90)

	err := stream.handleXLogData(
		testXLogData(walEnd, []byte{byte(message.TypeByte)}),
		&messageBuffer{outCh: make(chan *Message, 1)},
		&streamTxBuffer{},
	)

	assert.NoError(t, err)
	assert.Equal(t, walEnd, stream.LoadXLogPos())
}

func TestHandleXLogDataRejectsMalformedRow(t *testing.T) {
	stream := NewStream("", config.Config{}, nil, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)

	err := stream.handleXLogData(
		testXLogData(pq.LSN(20), []byte{byte(message.InsertByte)}),
		&messageBuffer{outCh: make(chan *Message, 1)},
		&streamTxBuffer{},
	)

	assert.Error(t, err)
}

func TestHandleXLogDataCheckpointsNontransactionalLogicalMessage(t *testing.T) {
	cfg := config.Config{Listener: config.ListenerConfig{EmitTransactionBoundaries: true}}
	stream := NewStream("", cfg, nil, metric.NewMetric("test_slot"), func(*ListenerContext) {
		t.Fatal("internal logical checkpoint reached the listener")
	}).(*stream)

	err := stream.handleXLogData(
		testXLogData(pq.LSN(20), testLogicalMessage(false)),
		&messageBuffer{outCh: stream.messageCH},
		&streamTxBuffer{},
	)

	assert.NoError(t, err)
	close(stream.messageCH)
	stream.process(t.Context())
	assert.Equal(t, pq.LSN(19), stream.LoadConfirmedXLogPos())
}

func TestIgnorableLogicalMetadataRequiresUnsupportedSentinel(t *testing.T) {
	assert.True(t, ignorableLogicalMetadata([]byte{byte(message.TypeByte)}, message.ErrorByteNotSupported))
	assert.False(t, ignorableLogicalMetadata([]byte{byte(message.TypeByte)}, errors.New("malformed metadata")))
	assert.False(t, ignorableLogicalMetadata([]byte{byte(message.InsertByte)}, message.ErrorByteNotSupported))
}

func testLogicalMessage(transactional bool) []byte {
	data := make([]byte, 1+1+8+2+4)
	data[0] = byte(message.LogicalByte)
	if transactional {
		data[1] = 1
	}
	binary.BigEndian.PutUint64(data[2:], 1)
	copy(data[10:], "p\x00")
	return data
}

func testXLogData(walEnd pq.LSN, logicalMessage []byte) []byte {
	data := make([]byte, 24+len(logicalMessage))
	binary.BigEndian.PutUint64(data[0:], uint64(walEnd-1))
	binary.BigEndian.PutUint64(data[8:], uint64(walEnd))
	binary.BigEndian.PutUint64(data[16:], 0)
	copy(data[24:], logicalMessage)
	return data
}
