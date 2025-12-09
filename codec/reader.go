package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/oarkflow/scrt/schema"
)

// Reader consumes SCRT streams and produces typed rows.
type Reader struct {
	src    *bufio.Reader
	schema *schema.Schema

	headerRead bool
	pageState  decodedPage
}

type decodedPage struct {
	rows     int
	cursor   int
	columns  []decodedColumn
	rawBytes []byte
}

type decodedColumn struct {
	kind    schema.FieldKind
	uints   []uint64
	strings []string
	bools   []bool
	ints    []int64
	floats  []float64
	bytes   [][]byte
}

// NewReader constructs a streaming decoder bound to schema.
func NewReader(src io.Reader, s *schema.Schema) *Reader {
	return &Reader{
		src:    bufio.NewReader(src),
		schema: s,
		pageState: decodedPage{
			columns: make([]decodedColumn, len(s.Fields)),
		},
	}
}

// ReadRow populates row with the next record. It returns false when the stream ends.
func (r *Reader) ReadRow(row Row) (bool, error) {
	if row.schema != r.schema {
		return false, ErrSchemaFingerprintMismatch
	}
	if !r.headerRead {
		if err := r.consumeHeader(); err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, err
		}
	}
	if r.pageState.cursor >= r.pageState.rows {
		if err := r.loadPage(); err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, err
		}
	}

	idx := r.pageState.cursor
	for fieldIdx, field := range r.schema.Fields {
		switch field.Kind {
		case schema.KindUint64, schema.KindRef:
			row.values[fieldIdx].Uint = r.pageState.columns[fieldIdx].uints[idx]
			row.values[fieldIdx].Str = ""
			row.values[fieldIdx].Set = true
		case schema.KindString:
			row.values[fieldIdx].Str = r.pageState.columns[fieldIdx].strings[idx]
			row.values[fieldIdx].Set = true
		case schema.KindBool:
			row.values[fieldIdx].Bool = r.pageState.columns[fieldIdx].bools[idx]
			row.values[fieldIdx].Set = true
		case schema.KindInt64:
			row.values[fieldIdx].Int = r.pageState.columns[fieldIdx].ints[idx]
			row.values[fieldIdx].Set = true
		case schema.KindFloat64:
			row.values[fieldIdx].Float = r.pageState.columns[fieldIdx].floats[idx]
			row.values[fieldIdx].Set = true
		case schema.KindBytes:
			row.values[fieldIdx].Bytes = cloneBytes(r.pageState.columns[fieldIdx].bytes[idx])
			row.values[fieldIdx].Set = true
		default:
			return false, ErrUnknownField
		}
	}
	r.pageState.cursor++
	return true, nil
}

func (r *Reader) consumeHeader() error {
	header := make([]byte, len(magic)+1+8)
	if _, err := io.ReadFull(r.src, header); err != nil {
		return err
	}
	if string(header[:len(magic)]) != magic {
		return fmt.Errorf("codec: invalid magic header")
	}
	if header[len(magic)] != version {
		return fmt.Errorf("codec: unsupported version %d", header[len(magic)])
	}
	fp := binary.LittleEndian.Uint64(header[len(magic)+1:])
	if fp != r.schema.Fingerprint() {
		return ErrSchemaFingerprintMismatch
	}
	r.headerRead = true
	return nil
}

func (r *Reader) loadPage() error {
	length, err := binary.ReadUvarint(r.src)
	if err != nil {
		return err
	}
	if length == 0 {
		return io.EOF
	}
	if cap(r.pageState.rawBytes) < int(length) {
		r.pageState.rawBytes = make([]byte, int(length))
	}
	buf := r.pageState.rawBytes[:int(length)]
	if _, err := io.ReadFull(r.src, buf); err != nil {
		return err
	}
	return r.decodePage(buf)
}

func (r *Reader) decodePage(raw []byte) error {
	r.pageState.cursor = 0
	rows, n := binary.Uvarint(raw)
	if n <= 0 {
		return fmt.Errorf("codec: malformed row count")
	}
	raw = raw[n:]

	columnCount, n := binary.Uvarint(raw)
	if n <= 0 {
		return fmt.Errorf("codec: malformed column count")
	}
	raw = raw[n:]
	if int(columnCount) != len(r.schema.Fields) {
		return fmt.Errorf("codec: column count mismatch")
	}

	if len(r.pageState.columns) != len(r.schema.Fields) {
		r.pageState.columns = make([]decodedColumn, len(r.schema.Fields))
	}

	for i := 0; i < int(columnCount); i++ {
		fieldIdx, consumed := binary.Uvarint(raw)
		if consumed <= 0 {
			return fmt.Errorf("codec: malformed field index")
		}
		raw = raw[consumed:]
		if len(raw) == 0 {
			return io.ErrUnexpectedEOF
		}
		kind := schema.FieldKind(raw[0])
		raw = raw[1:]
		payloadLen, consumed := binary.Uvarint(raw)
		if consumed <= 0 {
			return fmt.Errorf("codec: malformed payload length")
		}
		raw = raw[consumed:]
		if len(raw) < int(payloadLen) {
			return io.ErrUnexpectedEOF
		}
		payload := raw[:payloadLen]
		raw = raw[payloadLen:]
		col := decodedColumn{kind: kind}
		switch kind {
		case schema.KindUint64, schema.KindRef:
			values, err := decodeUintColumn(payload)
			if err != nil {
				return err
			}
			col.uints = values
		case schema.KindString:
			values, err := decodeStringColumn(payload)
			if err != nil {
				return err
			}
			col.strings = values
		case schema.KindBool:
			values, err := decodeBoolColumn(payload)
			if err != nil {
				return err
			}
			col.bools = values
		case schema.KindInt64:
			values, err := decodeIntColumn(payload)
			if err != nil {
				return err
			}
			col.ints = values
		case schema.KindFloat64:
			values, err := decodeFloatColumn(payload)
			if err != nil {
				return err
			}
			col.floats = values
		case schema.KindBytes:
			values, err := decodeBytesColumn(payload)
			if err != nil {
				return err
			}
			col.bytes = values
		default:
			return fmt.Errorf("codec: unsupported field kind %d", kind)
		}
		r.pageState.columns[int(fieldIdx)] = col
	}

	r.pageState.rows = int(rows)
	return nil
}

func decodeUintColumn(data []byte) ([]uint64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed uint column length")
	}
	data = data[n:]
	values := make([]uint64, int(count))
	for i := 0; i < int(count); i++ {
		v, consumed := binary.Uvarint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed uint value")
		}
		values[i] = v
		data = data[consumed:]
	}
	return values, nil
}

func decodeStringColumn(data []byte) ([]string, error) {
	dictLen, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed dictionary length")
	}
	data = data[n:]
	dictionary := make([]string, int(dictLen))
	for i := 0; i < int(dictLen); i++ {
		strLen, consumed := binary.Uvarint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed string length")
		}
		data = data[consumed:]
		if len(data) < int(strLen) {
			return nil, io.ErrUnexpectedEOF
		}
		dictionary[i] = string(data[:strLen])
		data = data[strLen:]
	}
	indexLen, consumed := binary.Uvarint(data)
	if consumed <= 0 {
		return nil, fmt.Errorf("codec: malformed index length")
	}
	data = data[consumed:]
	values := make([]string, int(indexLen))
	for i := 0; i < int(indexLen); i++ {
		idx, used := binary.Uvarint(data)
		if used <= 0 {
			return nil, fmt.Errorf("codec: malformed string index")
		}
		data = data[used:]
		if int(idx) >= len(dictionary) {
			return nil, fmt.Errorf("codec: string index out of range")
		}
		values[i] = dictionary[int(idx)]
	}
	return values, nil
}

func decodeBoolColumn(data []byte) ([]bool, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed bool column length")
	}
	data = data[n:]
	if len(data) < int(count) {
		return nil, io.ErrUnexpectedEOF
	}
	values := make([]bool, int(count))
	for i := 0; i < int(count); i++ {
		values[i] = data[i] != 0
	}
	return values, nil
}

func decodeIntColumn(data []byte) ([]int64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed int column length")
	}
	data = data[n:]
	values := make([]int64, int(count))
	for i := 0; i < int(count); i++ {
		v, consumed := binary.Varint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed int value")
		}
		values[i] = v
		data = data[consumed:]
	}
	return values, nil
}

func decodeFloatColumn(data []byte) ([]float64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed float column length")
	}
	data = data[n:]
	values := make([]float64, int(count))
	for i := 0; i < int(count); i++ {
		if len(data) < 8 {
			return nil, io.ErrUnexpectedEOF
		}
		bits := binary.LittleEndian.Uint64(data[:8])
		values[i] = math.Float64frombits(bits)
		data = data[8:]
	}
	return values, nil
}

func decodeBytesColumn(data []byte) ([][]byte, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed bytes column length")
	}
	data = data[n:]
	values := make([][]byte, int(count))
	for i := 0; i < int(count); i++ {
		length, consumed := binary.Uvarint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed bytes length")
		}
		data = data[consumed:]
		if len(data) < int(length) {
			return nil, io.ErrUnexpectedEOF
		}
		buf := make([]byte, int(length))
		copy(buf, data[:length])
		values[i] = buf
		data = data[length:]
	}
	return values, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	buf := make([]byte, len(src))
	copy(buf, src)
	return buf
}
