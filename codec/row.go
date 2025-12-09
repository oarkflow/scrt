package codec

import "github.com/oarkflow/scrt/schema"

// Value holds the typed payload for a schema field.
type Value struct {
	Uint  uint64
	Int   int64
	Float float64
	Str   string
	Bytes []byte
	Bool  bool
	Set   bool
}

// Row is a reusable, schema-aware container for field values.
type Row struct {
	schema *schema.Schema
	values []Value
}

// NewRow allocates a row aligned to the provided schema.
func NewRow(s *schema.Schema) Row {
	return Row{schema: s, values: make([]Value, len(s.Fields))}
}

// Reset clears string references to aid GC.
func (r Row) Reset() {
	for i := range r.values {
		r.values[i] = Value{}
	}
}

// SetUint sets a numeric field value by name.
func (r Row) SetUint(field string, v uint64) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	r.values[idx].Uint = v
	r.values[idx].Str = ""
	r.values[idx].Set = true
	return nil
}

// SetString sets a string field value by name.
func (r Row) SetString(field string, v string) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	r.values[idx].Str = v
	r.values[idx].Set = true
	return nil
}

// SetBool sets a bool field value by name.
func (r Row) SetBool(field string, v bool) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	r.values[idx].Bool = v
	r.values[idx].Set = true
	return nil
}

// SetInt sets an int64 field value by name.
func (r Row) SetInt(field string, v int64) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	r.values[idx].Int = v
	r.values[idx].Set = true
	return nil
}

// SetFloat sets a float64 field value by name.
func (r Row) SetFloat(field string, v float64) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	r.values[idx].Float = v
	r.values[idx].Set = true
	return nil
}

// SetBytes sets a bytes field value by name.
func (r Row) SetBytes(field string, v []byte) error {
	idx, ok := r.schema.FieldIndex(field)
	if !ok {
		return ErrUnknownField
	}
	buf := make([]byte, len(v))
	copy(buf, v)
	r.values[idx].Bytes = buf
	r.values[idx].Set = true
	return nil
}

// Values exposes the ordered slice consumed by the writer.
func (r Row) Values() []Value {
	return r.values
}

// Schema exposes the schema associated with the row.
func (r Row) Schema() *schema.Schema {
	return r.schema
}

// SetByIndex writes a prepared value by zero-based field index.
func (r Row) SetByIndex(idx int, v Value) {
	r.values[idx] = v
	r.values[idx].Set = true
}
