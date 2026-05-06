package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdate_New(t *testing.T) {
	data := []byte{85, 0, 0, 64, 6, 79, 0, 2, 116, 0, 0, 0, 2, 53, 51, 116, 0, 0, 0, 4, 98, 97, 114, 50, 78, 0, 2, 116, 0, 0, 0, 2, 53, 51, 116, 0, 0, 0, 4, 98, 97, 114, 53}

	rel := map[uint32]*Relation{
		16390: {
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
		},
	}

	now := time.Now()
	msg, err := NewUpdate(data, false, rel, now)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Update{
		OID: 16390,
		XID: 0,
		NewTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{
					DataType: 116,
					Length:   2,
					Data:     []byte("53"),
				},
				{
					DataType: 116,
					Length:   4,
					Data:     []byte("bar5"),
				},
			},
			SkipByte: 43,
		},
		NewDecoded: map[string]any{
			"id":   int32(53),
			"name": "bar5",
		},
		OldTupleType: 79,
		OldTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{
					DataType: 116,
					Length:   2,
					Data:     []byte("53"),
				},
				{
					DataType: 116,
					Length:   4,
					Data:     []byte("bar2"),
				},
			},
			SkipByte: 24,
		},
		OldDecoded: map[string]any{
			"id":   int32(53),
			"name": "bar2",
		},
		TableNamespace: "public",
		TableName:      "t",
		MessageTime:    now,
	}

	assert.Equal(t, expected, msg)
}

func TestUpdateBoundsChecks(t *testing.T) {
	t.Run("returns error when relation oid and tuple type are truncated", func(t *testing.T) {
		msg, err := NewUpdate([]byte{'U', 0, 0, 0, 42}, false, nil, time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "update relation oid and tuple type")
	})

	t.Run("returns error when toast column exceeds old tuple columns", func(t *testing.T) {
		data := []byte{'U'}
		data = appendUint32ForTest(data, 42)
		data = append(data,
			'O', 0, 1, tuple.DataTypeText, 0, 0, 0, 1, 'a',
			'N', 0, 2, tuple.DataTypeText, 0, 0, 0, 1, 'a', tuple.DataTypeToast,
		)

		msg, err := NewUpdate(data, false, map[uint32]*Relation{}, time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "exceeds old tuple column count")
	})
}
