package schema

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
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
	KindDate
	KindDateTime
	KindTimestamp
	KindTimestampTZ
	KindDuration
)

// Field models a single field declaration inside a schema.
type Field struct {
	Name          string
	Kind          FieldKind
	TargetSchema  string
	TargetField   string
	AutoIncrement bool
	ReadOnly      bool
	PrimaryKey    bool
	Unique        bool
	Nullable      bool
	Format        string
	Pattern       string
	Enum          []string
	MinLength     *int
	MaxLength     *int
	Minimum       *float64
	Maximum       *float64
	Description   string
	Example       string
	IsArray       bool
	IsMap         bool
	IsObject      bool
	ArrayElemType string
	MapKeyType    string
	MapValueType  string
	ObjectSchema  string
	OnDelete      string
	OnUpdate      string
	RawType       string
	Attributes    []string
	Default       *DefaultValue

	ResolvedKind   FieldKind
	pendingDefault string
}

// Index represents a database index definition.
type Index struct {
	Name   string
	Fields []string
	Unique bool
}

// Schema represents a canonical schema definition extracted from the DSL.
type Schema struct {
	Name      string
	Fields    []Field
	Indexes   []Index
	Relations []Relation

	once        sync.Once
	fingerprint uint64

	indexOnce  sync.Once
	fieldIndex map[string]int
}

type Relation struct {
	Name         string
	Field        string
	TargetSchema string
	TargetField  string
	OnDelete     string
	OnUpdate     string
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
			if f.ReadOnly {
				write("+readonly")
			}
			if f.Nullable {
				write("+nullable")
			}
			if f.Format != "" {
				write("+format:")
				write(f.Format)
			}
			if f.Pattern != "" {
				write("+pattern:")
				write(f.Pattern)
			}
			if len(f.Enum) > 0 {
				write("+enum:")
				for _, v := range f.Enum {
					write(v)
					write("|")
				}
			}
			if f.MinLength != nil {
				write("+minlen:")
				write(intToString(*f.MinLength))
			}
			if f.MaxLength != nil {
				write("+maxlen:")
				write(intToString(*f.MaxLength))
			}
			if f.Minimum != nil {
				write("+min:")
				write(floatToString(*f.Minimum))
			}
			if f.Maximum != nil {
				write("+max:")
				write(floatToString(*f.Maximum))
			}
			if f.IsArray {
				write("+array:")
				write(f.ArrayElemType)
			}
			if f.IsMap {
				write("+map:")
				write(f.MapKeyType)
				write("->")
				write(f.MapValueType)
			}
			if f.IsObject {
				write("+object:")
				write(f.ObjectSchema)
			}
			if f.OnDelete != "" {
				write("+ondelete:")
				write(f.OnDelete)
			}
			if f.OnUpdate != "" {
				write("+onupdate:")
				write(f.OnUpdate)
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
		for _, rel := range s.Relations {
			write("|rel:")
			write(rel.Name)
			write(":")
			write(rel.Field)
			write("->")
			write(rel.TargetSchema)
			write(".")
			write(rel.TargetField)
			write("/d:")
			write(rel.OnDelete)
			write("/u:")
			write(rel.OnUpdate)
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

// FieldByName returns a pointer to the field definition for name, if present.
func (s *Schema) FieldByName(name string) (*Field, bool) {
	idx, ok := s.FieldIndex(name)
	if !ok {
		return nil, false
	}
	return &s.Fields[idx], true
}

// ValueKind reports the effective storage kind for the field.
// Reference fields resolve to the target field's kind when available.
func (f Field) ValueKind() FieldKind {
	if f.Kind == KindRef {
		if f.ResolvedKind != KindInvalid {
			return f.ResolvedKind
		}
		return KindUint64
	}
	if f.ResolvedKind != KindInvalid {
		return f.ResolvedKind
	}
	return f.Kind
}

// IsReference reports whether the field refers to another schema field.
func (f Field) IsReference() bool {
	return f.Kind == KindRef && f.TargetSchema != "" && f.TargetField != ""
}

// HasAttribute reports whether the field declaration included the attribute label.
func (f Field) HasAttribute(label string) bool {
	if label == "" {
		return false
	}
	lower := strings.ToLower(label)
	for _, attr := range f.Attributes {
		if attr == lower {
			return true
		}
	}
	return false
}

func intToString(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

func floatToString(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Argument represents a function or query argument.
type Argument struct {
	Name string
	Type string
}

// Function represents a server-side function definition.
type Function struct {
	Name       string
	Args       []Argument
	ReturnType string
	Body       string
}

// Query represents a stored SQL query.
type Query struct {
	Name string
	Args []Argument
	SQL  string
}
