package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"

	"github.com/oarkflow/scrt/schema"
)

// Reader consumes SCRT streams and produces typed rows.
type Reader struct {
	src    *bufio.Reader
	schema *schema.Schema

	headerRead    bool
	pageState     decodedPage
	zeroCopyBytes bool
}

type decodedPage struct {
	rows     int
	cursor   int
	columns  []decodedColumn
	rawBytes []byte
}

type decodedColumn struct {
	kind          schema.FieldKind
	uints         []uint64
	stringOffsets []uint32
	stringLens    []uint32
	stringIndexes []uint32
	stringArena   []byte
	byteOffsets   []uint32
	byteLens      []uint32
	byteArena     []byte
	bools         []bool
	ints          []int64
	floats        []float64
}

// NewReader constructs a streaming decoder bound to schema.
// Options controls reader behavior.
type Options struct {
	// ZeroCopyBytes, when true, returns byte slices backed by the page buffer.
	// Callers must treat returned byte slices as read-only and they remain valid
	// only until the next page is loaded or the reader is reused.
	ZeroCopyBytes bool
}

// NewReader constructs a streaming decoder bound to schema.
func NewReader(src io.Reader, s *schema.Schema) *Reader {
	return NewReaderWithOptions(src, s, Options{})
}

// NewReaderWithOptions constructs a decoder with custom options.
func NewReaderWithOptions(src io.Reader, s *schema.Schema, opts Options) *Reader {
	return &Reader{
		src:           bufio.NewReader(src),
		schema:        s,
		zeroCopyBytes: opts.ZeroCopyBytes,
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
			col := r.pageState.columns[fieldIdx]
			if idx >= len(col.stringIndexes) {
				return false, fmt.Errorf("codec: string index missing")
			}
			dictIdx := col.stringIndexes[idx]
			if int(dictIdx) >= len(col.stringOffsets) {
				return false, fmt.Errorf("codec: string index out of range")
			}
			offset := col.stringOffsets[dictIdx]
			length := col.stringLens[dictIdx]
			if length == 0 {
				row.values[fieldIdx].Str = ""
			} else {
				start := int(offset)
				end := start + int(length)
				if end > len(col.stringArena) {
					return false, fmt.Errorf("codec: string slice out of bounds")
				}
				row.values[fieldIdx].Str = unsafe.String(&col.stringArena[start], int(length))
			}
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
			col := r.pageState.columns[fieldIdx]
			if len(col.byteOffsets) > idx {
				offset := col.byteOffsets[idx]
				length := col.byteLens[idx]
				segment := col.byteArena[offset : offset+length]
				if r.zeroCopyBytes {
					row.values[fieldIdx].Bytes = segment
					row.values[fieldIdx].Borrowed = true
				} else {
					row.values[fieldIdx].Bytes = cloneBytes(segment)
					row.values[fieldIdx].Borrowed = false
				}
			} else {
				row.values[fieldIdx].Bytes = nil
				row.values[fieldIdx].Borrowed = false
			}
			row.values[fieldIdx].Set = true
		default:
			return false, ErrUnknownField
		}
	}
	r.pageState.cursor++
	return true, nil
}

// RowsRemainingHint returns the number of buffered rows left in the current page.
func (r *Reader) RowsRemainingHint() int {
	remaining := r.pageState.rows - r.pageState.cursor
	if remaining < 0 {
		return 0
	}
	return remaining
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
		col := &r.pageState.columns[int(fieldIdx)]
		col.kind = kind
		switch kind {
		case schema.KindUint64, schema.KindRef:
			values, err := decodeUintColumn(payload, col.uints)
			if err != nil {
				return err
			}
			col.uints = values
		case schema.KindString:
			offsets, lens, indexes, arena, err := decodeStringColumn(payload, col.stringOffsets, col.stringLens, col.stringIndexes)
			if err != nil {
				return err
			}
			col.stringOffsets = offsets
			col.stringLens = lens
			col.stringIndexes = indexes
			col.stringArena = arena
		case schema.KindBool:
			values, err := decodeBoolColumn(payload, col.bools)
			if err != nil {
				return err
			}
			col.bools = values
		case schema.KindInt64:
			values, err := decodeIntColumn(payload, col.ints)
			if err != nil {
				return err
			}
			col.ints = values
		case schema.KindFloat64:
			values, err := decodeFloatColumn(payload, col.floats)
			if err != nil {
				return err
			}
			col.floats = values
		case schema.KindBytes:
			offsets, lengths, arena, err := decodeBytesColumn(payload, col.byteOffsets, col.byteLens)
			if err != nil {
				return err
			}
			col.byteOffsets = offsets
			col.byteLens = lengths
			col.byteArena = arena
		default:
			return fmt.Errorf("codec: unsupported field kind %d", kind)
		}
	}

	r.pageState.rows = int(rows)
	return nil
}

func decodeUintColumn(data []byte, dst []uint64) ([]uint64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed uint column length")
	}
	data = data[n:]
	dst = ensureUint64Slice(dst, int(count))
	for i := 0; i < int(count); i++ {
		v, consumed := binary.Uvarint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed uint value")
		}
		dst[i] = v
		data = data[consumed:]
	}
	return dst, nil
}

func decodeStringColumn(data []byte, offsets, lengths, indexes []uint32) ([]uint32, []uint32, []uint32, []byte, error) {
	dictLen, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, nil, nil, fmt.Errorf("codec: malformed dictionary length")
	}
	data = data[n:]
	dictBytes := data
	cursor := 0
	offsets = ensureUint32Slice(offsets, int(dictLen))
	lengths = ensureUint32Slice(lengths, int(dictLen))
	for i := 0; i < int(dictLen); i++ {
		length, consumed := binary.Uvarint(dictBytes[cursor:])
		if consumed <= 0 {
			return nil, nil, nil, nil, fmt.Errorf("codec: malformed string length")
		}
		cursor += consumed
		if len(dictBytes) < cursor+int(length) {
			return nil, nil, nil, nil, io.ErrUnexpectedEOF
		}
		offsets[i] = uint32(cursor)
		lengths[i] = uint32(length)
		cursor += int(length)
	}
	arena := dictBytes[:cursor]
	data = dictBytes[cursor:]
	indexLen, consumed := binary.Uvarint(data)
	if consumed <= 0 {
		return nil, nil, nil, nil, fmt.Errorf("codec: malformed index length")
	}
	data = data[consumed:]
	indexes = ensureUint32Slice(indexes, int(indexLen))
	for i := 0; i < int(indexLen); i++ {
		idx, used := binary.Uvarint(data)
		if used <= 0 {
			return nil, nil, nil, nil, fmt.Errorf("codec: malformed string index")
		}
		data = data[used:]
		if idx >= dictLen {
			return nil, nil, nil, nil, fmt.Errorf("codec: string index out of range")
		}
		indexes[i] = uint32(idx)
	}
	return offsets, lengths, indexes, arena, nil
}

func decodeBoolColumn(data []byte, dst []bool) ([]bool, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed bool column length")
	}
	data = data[n:]
	if len(data) < int(count) {
		return nil, io.ErrUnexpectedEOF
	}
	dst = ensureBoolSlice(dst, int(count))
	for i := 0; i < int(count); i++ {
		dst[i] = data[i] != 0
	}
	return dst, nil
}

func decodeIntColumn(data []byte, dst []int64) ([]int64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed int column length")
	}
	data = data[n:]
	dst = ensureInt64Slice(dst, int(count))
	for i := 0; i < int(count); i++ {
		v, consumed := binary.Varint(data)
		if consumed <= 0 {
			return nil, fmt.Errorf("codec: malformed int value")
		}
		dst[i] = v
		data = data[consumed:]
	}
	return dst, nil
}

func decodeFloatColumn(data []byte, dst []float64) ([]float64, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, fmt.Errorf("codec: malformed float column length")
	}
	data = data[n:]
	dst = ensureFloat64Slice(dst, int(count))
	for i := 0; i < int(count); i++ {
		if len(data) < 8 {
			return nil, io.ErrUnexpectedEOF
		}
		bits := binary.LittleEndian.Uint64(data[:8])
		dst[i] = math.Float64frombits(bits)
		data = data[8:]
	}
	return dst, nil
}

func decodeBytesColumn(data []byte, offsets, lengths []uint32) ([]uint32, []uint32, []byte, error) {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, nil, fmt.Errorf("codec: malformed bytes column length")
	}
	idx := n
	payloadStart := idx
	offsets = ensureUint32Slice(offsets, int(count))
	lengths = ensureUint32Slice(lengths, int(count))
	for i := 0; i < int(count); i++ {
		length, consumed := binary.Uvarint(data[idx:])
		if consumed <= 0 {
			return nil, nil, nil, fmt.Errorf("codec: malformed bytes length")
		}
		idx += consumed
		if len(data) < idx+int(length) {
			return nil, nil, nil, io.ErrUnexpectedEOF
		}
		offsets[i] = uint32(idx - payloadStart)
		lengths[i] = uint32(length)
		idx += int(length)
	}
	arena := data[payloadStart:idx]
	return offsets, lengths, arena, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	buf := make([]byte, len(src))
	copy(buf, src)
	return buf
}

func ensureUint64Slice(buf []uint64, size int) []uint64 {
	if size == 0 {
		if buf == nil {
			return nil
		}
		return buf[:0]
	}
	if cap(buf) < size {
		newCap := growCapacity(cap(buf), size)
		buf = make([]uint64, newCap)
	}
	return buf[:size]
}

func ensureInt64Slice(buf []int64, size int) []int64 {
	if size == 0 {
		if buf == nil {
			return nil
		}
		return buf[:0]
	}
	if cap(buf) < size {
		newCap := growCapacity(cap(buf), size)
		buf = make([]int64, newCap)
	}
	return buf[:size]
}

func ensureFloat64Slice(buf []float64, size int) []float64 {
	if size == 0 {
		if buf == nil {
			return nil
		}
		return buf[:0]
	}
	if cap(buf) < size {
		newCap := growCapacity(cap(buf), size)
		buf = make([]float64, newCap)
	}
	return buf[:size]
}

func ensureBoolSlice(buf []bool, size int) []bool {
	if size == 0 {
		if buf == nil {
			return nil
		}
		return buf[:0]
	}
	if cap(buf) < size {
		newCap := growCapacity(cap(buf), size)
		buf = make([]bool, newCap)
	}
	return buf[:size]
}

func ensureUint32Slice(buf []uint32, size int) []uint32 {
	if size == 0 {
		if buf == nil {
			return nil
		}
		return buf[:0]
	}
	if cap(buf) < size {
		newCap := growCapacity(cap(buf), size)
		buf = make([]uint32, newCap)
	}
	return buf[:size]
}

func growCapacity(current, needed int) int {
	if current == 0 {
		current = 1
	}
	for current < needed {
		if current >= math.MaxInt/2 {
			return needed
		}
		current *= 2
	}
	return current
}
