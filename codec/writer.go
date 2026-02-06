package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"

	"github.com/oarkflow/scrt/page"
	"github.com/oarkflow/scrt/schema"
)

const (
	magic   = "SCRT"
	version = byte(2)
)

// Writer streams values directly into the SCRT binary format.
// Optimized to reduce memory allocations by writing directly to pages.
type Writer struct {
	dst           io.Writer
	schema        *schema.Schema
	builder       *page.Builder
	headerWritten bool
	scratch       bytes.Buffer
}

// NewWriter constructs a streaming writer for a schema.
func NewWriter(dst io.Writer, s *schema.Schema, rowsPerPage int) *Writer {
	return &Writer{
		dst:     dst,
		schema:  s,
		builder: page.AcquireBuilder(s, rowsPerPage),
	}
}

// StartRow prepares the writer for a new row.
func (w *Writer) StartRow() error {
	return w.ensureHeader()
}

// EndRow seals the current row and flushes if the page is full.
func (w *Writer) EndRow() error {
	w.builder.SealRow()
	if w.builder.Full() {
		return w.flushPage()
	}
	return nil
}

// RecordPresence marks a field as present or absent in the current row.
func (w *Writer) RecordPresence(idx int, present bool) {
	w.builder.RecordPresence(idx, present)
}

// AppendUint writes a uint64 value to the specified column.
func (w *Writer) AppendUint(idx int, v uint64) {
	w.builder.AppendUint(idx, v)
}

// AppendString writes a string value to the specified column.
func (w *Writer) AppendString(idx int, v string) {
	w.builder.AppendString(idx, v)
}

// AppendBool writes a bool value to the specified column.
func (w *Writer) AppendBool(idx int, v bool) {
	w.builder.AppendBool(idx, v)
}

// AppendInt writes an int64 value to the specified column.
func (w *Writer) AppendInt(idx int, v int64) {
	w.builder.AppendInt(idx, v)
}

// AppendFloat writes a float64 value to the specified column.
func (w *Writer) AppendFloat(idx int, v float64) {
	w.builder.AppendFloat(idx, v)
}

// AppendBytes writes a byte slice to the specified column.
func (w *Writer) AppendBytes(idx int, v []byte) {
	w.builder.AppendBytes(idx, v)
}

// WriteValueDirect writes a reflect.Value directly to the underlying stream.
// This bypasses the Row intermediate representation for better performance.
func (w *Writer) WriteValueDirect(value reflect.Value) error {
	if err := w.ensureHeader(); err != nil {
		return err
	}

	// Direct field access and encoding
	for idx, field := range w.schema.Fields {
		fieldValue := getFieldValue(value, field.Name)
		if !fieldValue.IsValid() {
			w.builder.RecordPresence(idx, false)
			continue
		}

		w.builder.RecordPresence(idx, true)

		// Direct encoding based on field kind
		switch field.Kind {
		case schema.KindUint64:
			if v, ok := fieldValue.Interface().(uint64); ok {
				w.builder.AppendUint(idx, v)
			}
		case schema.KindString:
			if v, ok := fieldValue.Interface().(string); ok {
				w.builder.AppendString(idx, v)
			}
		case schema.KindBool:
			if v, ok := fieldValue.Interface().(bool); ok {
				w.builder.AppendBool(idx, v)
			}
		case schema.KindInt64:
			if v, ok := fieldValue.Interface().(int64); ok {
				w.builder.AppendInt(idx, v)
			}
		case schema.KindFloat64:
			if v, ok := fieldValue.Interface().(float64); ok {
				w.builder.AppendFloat(idx, v)
			}
		case schema.KindBytes:
			if v, ok := fieldValue.Interface().([]byte); ok {
				w.builder.AppendBytes(idx, v)
			}
			// Add other field types...
		}
	}

	// Seal the row after all fields are appended
	w.builder.SealRow()

	// Check if page is full and needs to be written
	if w.builder.Full() {
		return w.flushPage()
	}

	return nil
}

// Flush forces the current page to be written.
func (w *Writer) Flush() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	return w.flushPage()
}

// Close flushes remaining data and releases resources.
func (w *Writer) Close() error {
	err := w.Flush()
	page.ReleaseBuilder(w.builder)
	w.builder = nil
	w.headerWritten = false
	w.scratch.Reset()
	return err
}

// ensureHeader writes the SCRT header if not already written.
func (w *Writer) ensureHeader() error {
	if w.headerWritten {
		return nil
	}

	w.scratch.Reset()
	w.scratch.WriteString(magic)
	w.scratch.WriteByte(version)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], w.schema.Fingerprint())
	w.scratch.Write(buf[:])

	if _, err := w.dst.Write(w.scratch.Bytes()); err != nil {
		return err
	}

	w.headerWritten = true
	return nil
}

// flushPage writes the current page to the destination.
func (w *Writer) flushPage() error {
	if w.builder.Rows() == 0 {
		return nil
	}

	w.scratch.Reset()
	w.builder.Encode(&w.scratch)

	// Write page length prefix
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(w.scratch.Len()))
	if _, err := w.dst.Write(lenBuf[:n]); err != nil {
		return err
	}

	if _, err := w.dst.Write(w.scratch.Bytes()); err != nil {
		return err
	}

	w.builder.Reset()
	return nil
}

// getFieldValue extracts a field value by name from a reflect.Value.
// This is a simplified version - in practice, this would handle struct tags, etc.
// Note: This helper might need to be moved or made a method if used elsewhere,
// but for now it stays as a helper for WriteValueDirect.


// WriteRow writes a single row to the underlying stream.
func (w *Writer) WriteRow(row Row) error {
	if len(row.values) != len(w.schema.Fields) {
		return ErrMismatchedFieldCount
	}

	if err := w.ensureHeader(); err != nil {
		return err
	}

	for idx, field := range w.schema.Fields {
		val := &row.values[idx]
		if !val.Set {
			w.builder.RecordPresence(idx, false)
			continue
		}
		w.builder.RecordPresence(idx, true)
		kind := field.ValueKind()
		switch kind {
		case schema.KindUint64:
			w.builder.AppendUint(idx, val.Uint)
		case schema.KindString:
			w.builder.AppendString(idx, val.Str)
		case schema.KindBool:
			w.builder.AppendBool(idx, val.Bool)
		case schema.KindInt64:
			w.builder.AppendInt(idx, val.Int)
		case schema.KindFloat64:
			w.builder.AppendFloat(idx, val.Float)
		case schema.KindBytes:
			w.builder.AppendBytes(idx, val.Bytes)
		case schema.KindDate, schema.KindDateTime, schema.KindTimestamp, schema.KindDuration:
			w.builder.AppendInt(idx, val.Int)
		case schema.KindTimestampTZ:
			w.builder.AppendString(idx, val.Str)
		default:
			return ErrUnknownField
		}
	}
	w.builder.SealRow()

	if w.builder.Full() {
		if err := w.flushPage(); err != nil {
			return err
		}
		w.builder.Reset()
	}
	return nil
}

// getFieldValue extracts a field value by name from a reflect.Value.
// This is a simplified version - in practice, this would handle struct tags, etc.
func getFieldValue(value reflect.Value, fieldName string) reflect.Value {
	if value.Kind() == reflect.Struct {
		field := value.FieldByName(fieldName)
		if field.IsValid() {
			return field
		}
	} else if value.Kind() == reflect.Map && value.Type().Key().Kind() == reflect.String {
		return value.MapIndex(reflect.ValueOf(fieldName))
	}
	return reflect.Value{}
}
