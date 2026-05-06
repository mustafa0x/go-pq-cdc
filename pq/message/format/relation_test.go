package format

import (
	"encoding/binary"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelation_New(t *testing.T) {
	data := []byte{82, 0, 0, 64, 6, 112, 117, 98, 108, 105, 99, 0, 116, 0, 100, 0, 2, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255, 0, 110, 97, 109, 101, 0, 0, 0, 0, 25, 255, 255, 255, 255}

	rel, err := NewRelation(data, false)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Relation{
		OID:           16390,
		XID:           0,
		Namespace:     "public",
		Name:          "t",
		ReplicaID:     100,
		ColumnNumbers: 2,
		Columns: []tuple.RelationColumn{
			{
				Flags:        1,
				Name:         "id",
				DataType:     23,
				TypeModifier: 4294967295,
			},
			{
				Flags:        0,
				Name:         "name",
				DataType:     25,
				TypeModifier: 4294967295,
			},
		},
	}

	assert.Equal(t, expected, rel)
}

func TestNewRelationBoundsChecks(t *testing.T) {
	t.Run("returns error when streamed xid is truncated", func(t *testing.T) {
		msg, err := NewRelation([]byte{'R', 0, 0}, true)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "streamed transaction relation xid")
	})

	t.Run("returns error when replica identity and column count are missing", func(t *testing.T) {
		data := buildRelationMessageForTest(42, "public", "books", nil)
		data = data[:len(data)-3]

		msg, err := NewRelation(data, false)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "relation replica identity and column count")
	})

	t.Run("returns error when column metadata is truncated", func(t *testing.T) {
		data := buildRelationMessageForTest(42, "public", "books", []string{"id"})
		data = data[:len(data)-1]

		msg, err := NewRelation(data, false)

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "relation column type metadata")
	})
}

func buildRelationMessageForTest(oid uint32, namespace, name string, columns []string) []byte {
	buf := []byte{'R'}
	buf = appendUint32ForTest(buf, oid)
	buf = append(buf, []byte(namespace)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(name)...)
	buf = append(buf, 0)
	buf = append(buf, 'd')
	buf = appendUint16ForTest(buf, uint16(len(columns)))

	for _, col := range columns {
		buf = append(buf, 1)
		buf = append(buf, []byte(col)...)
		buf = append(buf, 0)
		buf = appendUint32ForTest(buf, 23)
		buf = appendUint32ForTest(buf, 0xffffffff)
	}

	return buf
}

func appendUint16ForTest(buf []byte, n uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], n)
	return append(buf, b[:]...)
}

func appendUint32ForTest(buf []byte, n uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	return append(buf, b[:]...)
}
