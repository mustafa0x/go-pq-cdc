package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

type Data struct {
	Columns      DataColumns
	SkipByte     int
	ColumnNumber uint16
}

type DataColumns []*DataColumn

type DataColumn struct {
	Data     []byte
	Length   uint32
	DataType uint8
}

type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

func NewData(data []byte, tupleDataType uint8, skipByteLength int) (*Data, error) {
	if skipByteLength < 0 {
		return nil, errors.Newf("invalid tuple data offset: %d", skipByteLength)
	}
	if len(data) <= skipByteLength {
		return nil, errors.Newf("tuple data type byte missing at offset %d: message length %d", skipByteLength, len(data))
	}

	if data[skipByteLength] != tupleDataType {
		return nil, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	d := &Data{}
	if err := d.Decode(data, skipByteLength); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *Data) Decode(data []byte, skipByteLength int) error {
	if err := requireAvailable(data, skipByteLength, 2, "tuple column count"); err != nil {
		return err
	}

	d.Columns = nil
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	for i := range d.ColumnNumber {
		if err := requireAvailable(data, skipByteLength, 1, "tuple column data type"); err != nil {
			return errors.Wrapf(err, "column %d", i)
		}

		col := new(DataColumn)
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			if err := requireAvailable(data, skipByteLength, 4, "tuple column length"); err != nil {
				return errors.Wrapf(err, "column %d", i)
			}
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			if uint64(col.Length) > uint64(len(data)-skipByteLength) {
				return errors.Newf("column %d tuple column data length %d exceeds remaining message length %d", i, col.Length, len(data)-skipByteLength)
			}
			col.Data = append([]byte(nil), data[skipByteLength:skipByteLength+int(col.Length)]...)

			skipByteLength += int(col.Length)
		default:
			return errors.Newf("unsupported tuple column data type %q at column %d", col.DataType, i)
		}

		d.Columns = append(d.Columns, col)
	}
	d.SkipByte = skipByteLength
	return nil
}

func (d *Data) DecodeWithColumn(columns []RelationColumn) (map[string]any, error) {
	if len(d.Columns) > len(columns) {
		return nil, errors.Newf("tuple column count %d exceeds relation column count %d", len(d.Columns), len(columns))
	}

	decoded := make(map[string]any, d.ColumnNumber)
	for idx, col := range d.Columns {
		colName := columns[idx].Name
		switch col.DataType {
		case DataTypeNull:
			decoded[colName] = nil
		case DataTypeText:
			val, err := decodeTextColumnData(col.Data, columns[idx].DataType)
			if err != nil {
				return nil, errors.Wrap(err, "decode column")
			}
			decoded[colName] = val
		case DataTypeBinary:
			val, err := decodeBinaryColumnData(col.Data, columns[idx].DataType)
			if err != nil {
				return nil, errors.Wrap(err, "decode binary column")
			}
			decoded[colName] = val
		}
	}

	return decoded, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func decodeBinaryColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.BinaryFormatCode, data)
	}
	return append([]byte(nil), data...), nil
}

func requireAvailable(data []byte, offset, size int, field string) error {
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
