package codec

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/oarkflow/scrt/page"
	"github.com/oarkflow/scrt/schema"
)

const (
	magic   = "SCRT"
	version = byte(2)
)

// Writer streams rows into the SCRT binary format.
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

// Flush forces the current page to be written.
func (w *Writer) Flush() error {
	if err := w.ensureHeader(); err != nil {
		return err
	}
	return w.flushPage()
}

// Close flushes remaining data.
func (w *Writer) Close() error {
	err := w.Flush()
	page.ReleaseBuilder(w.builder)
	w.builder = nil
	w.headerWritten = false
	w.scratch.Reset()
	return err
}

func (w *Writer) ensureHeader() error {
	if w.headerWritten {
		return nil
	}
	var header bytes.Buffer
	header.WriteString(magic)
	header.WriteByte(version)
	var fp [8]byte
	binary.LittleEndian.PutUint64(fp[:], w.schema.Fingerprint())
	header.Write(fp[:])
	if _, err := w.dst.Write(header.Bytes()); err != nil {
		return err
	}
	w.headerWritten = true
	return nil
}

func (w *Writer) flushPage() error {
	if w.builder.Rows() == 0 {
		return nil
	}
	w.scratch.Reset()
	w.builder.Encode(&w.scratch)
	pageBytes := w.scratch.Bytes()

	// Write length and page data directly without intermediate allocation
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(pageBytes)))

	// Write in a single operation if possible to reduce syscalls
	if _, err := w.dst.Write(lenBuf[:n]); err != nil {
		return err
	}
	if _, err := w.dst.Write(pageBytes); err != nil {
		return err
	}
	return nil
}
