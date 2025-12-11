package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

const (
	columnIndexMagic   = "KIDX"
	columnIndexVersion = uint16(1)
)

// IndexSpec declares which schema fields should be indexed when persisting a payload.
type IndexSpec struct {
	Field  string
	Unique bool
}

// ColumnIndex materializes a numeric key -> rowID lookup table.
type ColumnIndex struct {
	Field         string
	Unique        bool
	Kind          schema.FieldKind
	uintEntries   map[uint64]uint64
	stringEntries map[string]uint64
}

// LookupUint returns the rowID for a numeric key.
func (ci *ColumnIndex) LookupUint(key uint64) (uint64, bool) {
	if ci == nil {
		return 0, false
	}
	rowID, ok := ci.uintEntries[key]
	return rowID, ok
}

// LookupString returns the rowID for a string key.
func (ci *ColumnIndex) LookupString(key string) (uint64, bool) {
	if ci == nil {
		return 0, false
	}
	rowID, ok := ci.stringEntries[key]
	return rowID, ok
}

// EntryCount returns the number of indexed keys.
func (ci *ColumnIndex) EntryCount() int {
	if ci == nil {
		return 0
	}
	return len(ci.uintEntries) + len(ci.stringEntries)
}

// MaxKey returns the highest key present in the index.
func (ci *ColumnIndex) MaxKey() (uint64, bool) {
	if ci == nil || len(ci.uintEntries) == 0 {
		return 0, false
	}
	var max uint64
	set := false
	for key := range ci.uintEntries {
		if !set || key > max {
			max = key
			set = true
		}
	}
	return max, set
}

// buildColumnIndexes constructs indexes for the provided specs.
func buildColumnIndexes(sch *schema.Schema, payload []byte, specs []IndexSpec) (map[string]*ColumnIndex, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	builders := make(map[string]*columnIndexBuilder, len(specs))
	for _, spec := range specs {
		fieldIdx, ok := sch.FieldIndex(spec.Field)
		if !ok {
			return nil, fmt.Errorf("storage: schema %s lacks field %s", sch.Name, spec.Field)
		}
		field := sch.Fields[fieldIdx]
		kind := field.ValueKind()
		if kind != schema.KindUint64 && kind != schema.KindRef && kind != schema.KindString {
			return nil, fmt.Errorf("storage: field %s must be uint64/ref/string for indexing", spec.Field)
		}
		if _, exists := builders[spec.Field]; exists {
			return nil, fmt.Errorf("storage: duplicate index spec for field %s", spec.Field)
		}
		ci := &ColumnIndex{
			Field:  spec.Field,
			Unique: spec.Unique,
			Kind:   kind,
		}
		if kind == schema.KindString {
			ci.stringEntries = make(map[string]uint64)
		} else {
			ci.uintEntries = make(map[uint64]uint64)
		}
		builders[spec.Field] = &columnIndexBuilder{
			ColumnIndex: ci,
			fieldIdx:    fieldIdx,
		}
	}

	reader := codec.NewReader(bytesReader(payload), sch)
	row := codec.NewRow(sch)
	var rowID uint64
	for {
		ok, err := reader.ReadRow(row)
		if errors.Is(err, io.EOF) || !ok {
			break
		}
		if err != nil {
			return nil, err
		}
		values := row.Values()
		for _, builder := range builders {
			val := values[builder.fieldIdx]
			if !val.Set {
				continue
			}
			switch builder.Kind {
			case schema.KindUint64, schema.KindRef:
				key := val.Uint
				if builder.Unique {
					if _, exists := builder.uintEntries[key]; exists {
						return nil, fmt.Errorf("storage: duplicate key %d for field %s", key, builder.Field)
					}
				}
				builder.uintEntries[key] = rowID
			case schema.KindString:
				key := val.Str
				if builder.Unique {
					if _, exists := builder.stringEntries[key]; exists {
						return nil, fmt.Errorf("storage: duplicate key %s for field %s", key, builder.Field)
					}
				}
				builder.stringEntries[key] = rowID
			}
		}
		rowID++
	}

	out := make(map[string]*ColumnIndex, len(builders))
	for field, builder := range builders {
		out[field] = builder.ColumnIndex
	}
	return out, nil
}

// Persist writes the index to disk.
func (ci *ColumnIndex) Persist(w io.Writer) error {
	if ci == nil {
		return fmt.Errorf("storage: column index is nil")
	}
	var header [4 + 2 + 2 + 1 + 1 + 8]byte
	copy(header[:4], columnIndexMagic)
	binary.LittleEndian.PutUint16(header[4:6], columnIndexVersion)
	fieldLen := len(ci.Field)
	if fieldLen > int(^uint16(0)) {
		return fmt.Errorf("storage: field name too long")
	}
	binary.LittleEndian.PutUint16(header[6:8], uint16(fieldLen))
	if ci.Unique {
		header[8] = 1
	}
	header[9] = byte(ci.Kind)
	var count uint64
	if ci.Kind == schema.KindString {
		count = uint64(len(ci.stringEntries))
	} else {
		count = uint64(len(ci.uintEntries))
	}
	binary.LittleEndian.PutUint64(header[10:], count)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if fieldLen > 0 {
		if _, err := io.WriteString(w, ci.Field); err != nil {
			return err
		}
	}
	if ci.Kind == schema.KindString {
		keys := make([]string, 0, len(ci.stringEntries))
		for key := range ci.stringEntries {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if len(key) > int(^uint16(0)) {
				return fmt.Errorf("storage: key too long for string index")
			}
			var prefix [2]byte
			binary.LittleEndian.PutUint16(prefix[:], uint16(len(key)))
			if _, err := w.Write(prefix[:]); err != nil {
				return err
			}
			if len(key) > 0 {
				if _, err := io.WriteString(w, key); err != nil {
					return err
				}
			}
			var rowBuf [8]byte
			binary.LittleEndian.PutUint64(rowBuf[:], ci.stringEntries[key])
			if _, err := w.Write(rowBuf[:]); err != nil {
				return err
			}
		}
		return nil
	}
	keys := make([]uint64, 0, len(ci.uintEntries))
	for key := range ci.uintEntries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var entry [16]byte
	for _, key := range keys {
		binary.LittleEndian.PutUint64(entry[:8], key)
		binary.LittleEndian.PutUint64(entry[8:], ci.uintEntries[key])
		if _, err := w.Write(entry[:]); err != nil {
			return err
		}
	}
	return nil
}

// LoadColumnIndex reconstructs an index from disk.
func LoadColumnIndex(r io.Reader) (*ColumnIndex, error) {
	head := make([]byte, 4+2+2+1+1+8)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}
	if string(head[:4]) != columnIndexMagic {
		return nil, fmt.Errorf("storage: invalid column index magic")
	}
	version := binary.LittleEndian.Uint16(head[4:6])
	if version != columnIndexVersion {
		return nil, fmt.Errorf("storage: unsupported column index version %d", version)
	}
	nameLen := binary.LittleEndian.Uint16(head[6:8])
	unique := head[8] == 1
	kind := schema.FieldKind(head[9])
	count := binary.LittleEndian.Uint64(head[10:])
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return nil, err
	}
	ci := &ColumnIndex{
		Field:         string(name),
		Unique:        unique,
		Kind:          kind,
		uintEntries:   make(map[uint64]uint64),
		stringEntries: make(map[string]uint64),
	}
	if kind == schema.KindString {
		for i := uint64(0); i < count; i++ {
			var prefix [2]byte
			if _, err := io.ReadFull(r, prefix[:]); err != nil {
				return nil, err
			}
			length := binary.LittleEndian.Uint16(prefix[:])
			value := make([]byte, length)
			if _, err := io.ReadFull(r, value); err != nil {
				return nil, err
			}
			var rowBuf [8]byte
			if _, err := io.ReadFull(r, rowBuf[:]); err != nil {
				return nil, err
			}
			ci.stringEntries[string(value)] = binary.LittleEndian.Uint64(rowBuf[:])
		}
		return ci, nil
	}
	var entryBuf [16]byte
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(r, entryBuf[:]); err != nil {
			return nil, err
		}
		key := binary.LittleEndian.Uint64(entryBuf[:8])
		rowID := binary.LittleEndian.Uint64(entryBuf[8:])
		ci.uintEntries[key] = rowID
	}
	return ci, nil
}

type columnIndexBuilder struct {
	*ColumnIndex
	fieldIdx int
}

// bytesReader avoids importing bytes in multiple files.
func bytesReader(payload []byte) io.Reader {
	return bytes.NewReader(payload)
}
