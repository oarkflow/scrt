package page

import (
	"bytes"
	"sync"

	"github.com/oarkflow/scrt/column"
	"github.com/oarkflow/scrt/schema"
)

// Builder accumulates rows for a schema and emits compact columnar pages.
type Builder struct {
	schema   *schema.Schema
	rowLimit int
	rows     int

	columns   []columnHandle
	columnBuf bytes.Buffer
}

type columnHandle struct {
	kind    schema.FieldKind
	uints   *column.Uint64Column
	strings *column.StringColumn
	bools   *column.BoolColumn
	ints    *column.Int64Column
	floats  *column.Float64Column
	bytes   *column.BytesColumn
}

// NewBuilder creates a builder with the provided row capacity.
func NewBuilder(s *schema.Schema, rowLimit int) *Builder {
	return newBuilderWithLimit(s, normalizeRowLimit(rowLimit))
}

// AcquireBuilder returns a reusable builder for the schema and rowLimit.
func AcquireBuilder(s *schema.Schema, rowLimit int) *Builder {
	limit := normalizeRowLimit(rowLimit)
	key := builderPoolKey{schema: s, rowsPerPage: limit}
	poolIface, _ := builderPools.LoadOrStore(key, &sync.Pool{
		New: func() any {
			return newBuilderWithLimit(s, limit)
		},
	})
	b := poolIface.(*sync.Pool).Get().(*Builder)
	b.Reset()
	return b
}

// ReleaseBuilder returns a builder back to the pool after resetting it.
func ReleaseBuilder(b *Builder) {
	if b == nil || b.schema == nil {
		return
	}
	key := builderPoolKey{schema: b.schema, rowsPerPage: b.rowLimit}
	if poolIface, ok := builderPools.Load(key); ok {
		b.Reset()
		poolIface.(*sync.Pool).Put(b)
	}
}

type builderPoolKey struct {
	schema      *schema.Schema
	rowsPerPage int
}

var builderPools sync.Map

func normalizeRowLimit(rowLimit int) int {
	if rowLimit <= 0 {
		return 1024
	}
	return rowLimit
}

func newBuilderWithLimit(s *schema.Schema, rowLimit int) *Builder {
	cols := make([]columnHandle, len(s.Fields))
	for i, f := range s.Fields {
		valueKind := f.ValueKind()
		handle := columnHandle{kind: valueKind}
		switch valueKind {
		case schema.KindUint64:
			handle.uints = column.NewUint64Column(rowLimit)
		case schema.KindString:
			handle.strings = column.NewStringColumn(rowLimit)
		case schema.KindBool:
			handle.bools = column.NewBoolColumn(rowLimit)
		case schema.KindInt64,
			schema.KindDate,
			schema.KindDateTime,
			schema.KindTimestamp,
			schema.KindDuration:
			handle.ints = column.NewInt64Column(rowLimit)
		case schema.KindFloat64:
			handle.floats = column.NewFloat64Column(rowLimit)
		case schema.KindBytes:
			handle.bytes = column.NewBytesColumn(rowLimit)
		case schema.KindTimestampTZ:
			handle.strings = column.NewStringColumn(rowLimit)
		default:
			panic("unsupported field kind")
		}
		cols[i] = handle
	}
	return &Builder{schema: s, rowLimit: rowLimit, columns: cols}
}

// Rows returns the number of buffered rows.
func (b *Builder) Rows() int { return b.rows }

// Full reports whether the builder reached its capacity.
func (b *Builder) Full() bool { return b.rows >= b.rowLimit }

// AppendUint records a uint64 value for the specified field index.
func (b *Builder) AppendUint(idx int, v uint64) {
	handle := &b.columns[idx]
	if handle.uints == nil {
		panic("field is not numeric")
	}
	handle.uints.Append(v)
}

// AppendString records a string value for the specified field index.
func (b *Builder) AppendString(idx int, v string) {
	handle := &b.columns[idx]
	if handle.strings == nil {
		panic("field is not string")
	}
	handle.strings.Append(v)
}

// AppendBool records a bool value for the specified field index.
func (b *Builder) AppendBool(idx int, v bool) {
	handle := &b.columns[idx]
	if handle.bools == nil {
		panic("field is not bool")
	}
	handle.bools.Append(v)
}

// AppendInt records an int64 value for the specified field index.
func (b *Builder) AppendInt(idx int, v int64) {
	handle := &b.columns[idx]
	if handle.ints == nil {
		panic("field is not int64")
	}
	handle.ints.Append(v)
}

// AppendFloat records a float64 value for the specified field index.
func (b *Builder) AppendFloat(idx int, v float64) {
	handle := &b.columns[idx]
	if handle.floats == nil {
		panic("field is not float64")
	}
	handle.floats.Append(v)
}

// AppendBytes records a []byte value for the specified field index.
func (b *Builder) AppendBytes(idx int, v []byte) {
	handle := &b.columns[idx]
	if handle.bytes == nil {
		panic("field is not bytes")
	}
	handle.bytes.Append(v)
}

// SealRow increments the buffered row count after all field values were appended.
func (b *Builder) SealRow() {
	b.rows++
	if b.rows > b.rowLimit {
		panic("page builder capacity exceeded")
	}
}

// Reset clears buffers for the next page.
func (b *Builder) Reset() {
	b.rows = 0
	for i := range b.columns {
		if b.columns[i].uints != nil {
			b.columns[i].uints.Reset()
		}
		if b.columns[i].strings != nil {
			b.columns[i].strings.Reset()
		}
		if b.columns[i].bools != nil {
			b.columns[i].bools.Reset()
		}
		if b.columns[i].ints != nil {
			b.columns[i].ints.Reset()
		}
		if b.columns[i].floats != nil {
			b.columns[i].floats.Reset()
		}
		if b.columns[i].bytes != nil {
			b.columns[i].bytes.Reset()
		}
	}
}

// Encode writes the current page into dst.
func (b *Builder) Encode(dst *bytes.Buffer) {
	if b.rows == 0 {
		return
	}
	writeUvarint(dst, uint64(b.rows))
	writeUvarint(dst, uint64(len(b.columns)))

	for idx, col := range b.columns {
		b.columnBuf.Reset()
		switch col.kind {
		case schema.KindUint64, schema.KindRef:
			col.uints.Encode(&b.columnBuf)
		case schema.KindString, schema.KindTimestampTZ:
			col.strings.Encode(&b.columnBuf)
		case schema.KindBool:
			col.bools.Encode(&b.columnBuf)
		case schema.KindInt64,
			schema.KindDate,
			schema.KindDateTime,
			schema.KindTimestamp,
			schema.KindDuration:
			col.ints.Encode(&b.columnBuf)
		case schema.KindFloat64:
			col.floats.Encode(&b.columnBuf)
		case schema.KindBytes:
			col.bytes.Encode(&b.columnBuf)
		}
		segment := b.columnBuf.Bytes()
		writeUvarint(dst, uint64(idx))
		dst.WriteByte(byte(col.kind))
		writeUvarint(dst, uint64(len(segment)))
		dst.Write(segment)
	}
}
