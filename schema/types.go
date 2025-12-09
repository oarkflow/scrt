package schema

import (
	"hash/fnv"
	"sort"
	"sync"
)

// FieldKind identifies the primitive storage category for a field.
type FieldKind uint8

const (
	KindInvalid FieldKind = iota
	KindUint64
	KindString
	KindRef
	KindBool
	KindInt64
	KindFloat64
	KindBytes
)

// Field models a single field declaration inside a schema.
type Field struct {
	Name          string
	Kind          FieldKind
	TargetSchema  string
	TargetField   string
	AutoIncrement bool
	RawType       string
	Attributes    []string
	Default       *DefaultValue
}

// Schema represents a canonical schema definition extracted from the DSL.
type Schema struct {
	Name   string
	Fields []Field

	once        sync.Once
	fingerprint uint64

	indexOnce  sync.Once
	fieldIndex map[string]int
}

// Fingerprint deterministically hashes the schema definition for caching.
func (s *Schema) Fingerprint() uint64 {
	s.once.Do(func() {
		h := fnv.New64a()
		write := func(str string) {
			_, _ = h.Write([]byte(str))
		}
		write(s.Name)
		for _, f := range s.Fields {
			write("|")
			write(f.Name)
			write(":")
			write(f.RawType)
			if f.TargetSchema != "" {
				write("->")
				write(f.TargetSchema)
				write(".")
				write(f.TargetField)
			}
			if f.AutoIncrement {
				write("+auto")
			}
			if len(f.Attributes) > 0 {
				attrs := append([]string(nil), f.Attributes...)
				sort.Strings(attrs)
				for _, attr := range attrs {
					write("@")
					write(attr)
				}
			}
			if f.Default != nil {
				write("=def:")
				write(f.Default.hashKey())
			}
		}
		s.fingerprint = h.Sum64()
	})
	return s.fingerprint
}

// FieldIndex returns the zero-based index for a field within the schema.
func (s *Schema) FieldIndex(name string) (int, bool) {
	s.indexOnce.Do(func() {
		s.fieldIndex = make(map[string]int, len(s.Fields))
		for i, f := range s.Fields {
			s.fieldIndex[f.Name] = i
		}
	})
	idx, ok := s.fieldIndex[name]
	return idx, ok
}
