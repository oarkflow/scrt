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
	version = byte(1)
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
		builder: page.NewBuilder(s, rowsPerPage),
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
		if !val.Set && field.Default != nil {
			applyDefault(field, val)
		}
		switch field.Kind {
		case schema.KindUint64, schema.KindRef:
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
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
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
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(pageBytes)))
	if _, err := w.dst.Write(lenBuf[:n]); err != nil {
		return err
	}
	if _, err := w.dst.Write(pageBytes); err != nil {
		return err
	}
	return nil
}

func applyDefault(field schema.Field, v *Value) {
	if field.Default == nil || v == nil {
		return
	}
	switch field.Default.Kind {
	case schema.KindUint64, schema.KindRef:
		v.Uint = field.Default.Uint
	case schema.KindString:
		v.Str = field.Default.String
	case schema.KindBool:
		v.Bool = field.Default.Bool
	case schema.KindInt64:
		v.Int = field.Default.Int
	case schema.KindFloat64:
		v.Float = field.Default.Float
	case schema.KindBytes:
		if field.Default.Bytes != nil {
			buf := make([]byte, len(field.Default.Bytes))
			copy(buf, field.Default.Bytes)
			v.Bytes = buf
		} else {
			v.Bytes = nil
		}
	}
	v.Set = true
}
