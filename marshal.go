package scrt

import (
	"bytes"
	"fmt"
	"os"
	"reflect"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

var mapStringAnyType = reflect.TypeOf(map[string]any{})

// MarshalOptions controls high-level marshal behavior.
type MarshalOptions struct {
	RowsPerPage int
}

// MarshalOption mutates MarshalOptions.
type MarshalOption func(*MarshalOptions)

// WithRowsPerPage overrides the default page size used during marshaling.
func WithRowsPerPage(n int) MarshalOption {
	return func(opts *MarshalOptions) {
		if n > 0 {
			opts.RowsPerPage = n
		}
	}
}

// Marshal serializes the provided record(s) into SCRT binary form.
func Marshal(s *schema.Schema, input any, opts ...MarshalOption) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("scrt: schema is required")
	}
	config := MarshalOptions{RowsPerPage: 1024}
	for _, opt := range opts {
		opt(&config)
	}
	var buf bytes.Buffer
	if err := encodeInto(&buf, s, input, config); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalToFile writes SCRT data directly to the provided file path.
func MarshalToFile(path string, s *schema.Schema, input any, opts ...MarshalOption) error {
	data, err := Marshal(s, input, opts...)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// MarshalFiles loads a schema from schemaPath and writes encoded data to dataPath.
func MarshalFiles(schemaPath, schemaName, dataPath string, input any, opts ...MarshalOption) error {
	doc, err := schema.ParseFile(schemaPath)
	if err != nil {
		return err
	}
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return fmt.Errorf("scrt: schema %q not found in %s", schemaName, schemaPath)
	}
	return MarshalToFile(dataPath, sch, input, opts...)
}

func encodeInto(dst *bytes.Buffer, s *schema.Schema, input any, cfg MarshalOptions) error {
	writer := codec.NewWriter(dst, s, cfg.RowsPerPage)
	row := codec.AcquireRow(s)
	defer codec.ReleaseRow(row)
	err := visitRecords(input, func(v reflect.Value) error {
		v = indirect(v)
		if !v.IsValid() {
			return fmt.Errorf("scrt: nil record")
		}
		row.Reset()
		if err := populateRow(*row, v, s); err != nil {
			return err
		}
		return writer.WriteRow(*row)
	})
	if err != nil {
		return err
	}
	return writer.Close()
}

func visitRecords(input any, fn func(reflect.Value) error) error {
	if input == nil {
		return fmt.Errorf("scrt: cannot marshal <nil>")
	}
	v := reflect.ValueOf(input)
	if !v.IsValid() {
		return fmt.Errorf("scrt: invalid input value")
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return fmt.Errorf("scrt: nil pointer input")
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		length := v.Len()
		for i := 0; i < length; i++ {
			if err := fn(v.Index(i)); err != nil {
				return err
			}
		}
	default:
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

func populateRow(row codec.Row, value reflect.Value, s *schema.Schema) error {
	switch value.Kind() {
	case reflect.Struct:
		return populateRowFromStruct(row, value, s)
	case reflect.Map:
		return populateRowFromMap(row, value, s)
	default:
		return fmt.Errorf("scrt: unsupported record kind %s", value.Kind())
	}
}

func populateRowFromStruct(row codec.Row, value reflect.Value, s *schema.Schema) error {
	if fast := fastEncoderForStruct(value.Type(), s); fast != nil && value.CanAddr() {
		fast.encode(row, value)
		return nil
	}
	bindings := structBindingsForSchema(value.Type(), s)
	for idx, binding := range bindings {
		if len(binding.index) == 0 {
			continue
		}
		fv := value.FieldByIndex(binding.index)
		if !fv.IsValid() {
			continue
		}
		if err := assignValueToRow(row, idx, s.Fields[idx].Kind, fv); err != nil {
			return fmt.Errorf("scrt: field %s: %w", s.Fields[idx].Name, err)
		}
	}
	return nil
}

func populateRowFromMap(row codec.Row, value reflect.Value, s *schema.Schema) error {
	if value.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("scrt: map key must be string, got %s", value.Type().Key())
	}
	switch data := value.Interface().(type) {
	case map[string]any:
		return populateRowFromMapAny(row, data, s)
	case map[string]bool:
		return populateRowFromMapBool(row, data, s)
	case map[string]int:
		return populateRowFromMapInt(row, data, s)
	case map[string]int64:
		return populateRowFromMapInt64(row, data, s)
	case map[string]uint:
		return populateRowFromMapUint(row, data, s)
	case map[string]uint64:
		return populateRowFromMapUint64(row, data, s)
	case map[string]float64:
		return populateRowFromMapFloat64(row, data, s)
	case map[string]string:
		return populateRowFromMapString(row, data, s)
	case map[string][]byte:
		return populateRowFromMapBytes(row, data, s)
	default:
		return populateRowFromMapReflect(row, value, s)
	}
}

func populateRowFromMapAny(row codec.Row, data map[string]any, s *schema.Schema) error {
	for idx, field := range s.Fields {
		mv, ok := data[field.Name]
		if !ok || mv == nil {
			continue
		}
		if err := assignAnyToRow(row, idx, field.Kind, mv); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
	}
	return nil
}

func populateRowFromMapReflect(row codec.Row, value reflect.Value, s *schema.Schema) error {
	for idx, field := range s.Fields {
		mv := value.MapIndex(reflect.ValueOf(field.Name))
		if !mv.IsValid() {
			continue
		}
		if err := assignValueToRow(row, idx, field.Kind, mv); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
	}
	return nil
}

func populateRowFromMapBool(row codec.Row, data map[string]bool, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindBool {
			return fmt.Errorf("scrt: field %s expects bool, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Bool = v
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapInt(row codec.Row, data map[string]int, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindInt64 {
			return fmt.Errorf("scrt: field %s expects int64, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Int = int64(v)
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapInt64(row codec.Row, data map[string]int64, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindInt64 {
			return fmt.Errorf("scrt: field %s expects int64, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Int = v
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapUint(row codec.Row, data map[string]uint, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindUint64 && field.Kind != schema.KindRef {
			return fmt.Errorf("scrt: field %s expects uint64/ref, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Uint = uint64(v)
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapUint64(row codec.Row, data map[string]uint64, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindUint64 && field.Kind != schema.KindRef {
			return fmt.Errorf("scrt: field %s expects uint64/ref, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Uint = v
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapFloat64(row codec.Row, data map[string]float64, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindFloat64 {
			return fmt.Errorf("scrt: field %s expects float64, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Float = v
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapString(row codec.Row, data map[string]string, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindString {
			return fmt.Errorf("scrt: field %s expects string, got kind %d", field.Name, field.Kind)
		}
		var val codec.Value
		val.Set = true
		val.Str = v
		row.SetByIndex(idx, val)
	}
	return nil
}

func populateRowFromMapBytes(row codec.Row, data map[string][]byte, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindBytes {
			return fmt.Errorf("scrt: field %s expects bytes, got kind %d", field.Name, field.Kind)
		}
		clone := cloneBytes(v)
		var val codec.Value
		val.Set = true
		val.Bytes = clone
		row.SetByIndex(idx, val)
	}
	return nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dup := make([]byte, len(src))
	copy(dup, src)
	return dup
}

func assignValueToRow(row codec.Row, idx int, kind schema.FieldKind, v reflect.Value) error {
	v = indirect(v)
	if !v.IsValid() {
		return nil
	}
	var val codec.Value
	val.Set = true
	switch kind {
	case schema.KindBool:
		b, err := valueAsBool(v)
		if err != nil {
			return err
		}
		val.Bool = b
	case schema.KindInt64:
		i, err := valueAsInt(v)
		if err != nil {
			return err
		}
		val.Int = i
	case schema.KindUint64, schema.KindRef:
		u, err := valueAsUint(v)
		if err != nil {
			return err
		}
		val.Uint = u
	case schema.KindFloat64:
		f, err := valueAsFloat(v)
		if err != nil {
			return err
		}
		val.Float = f
	case schema.KindString:
		s, err := valueAsString(v)
		if err != nil {
			return err
		}
		val.Str = s
	case schema.KindBytes:
		b, err := valueAsBytes(v)
		if err != nil {
			return err
		}
		val.Bytes = b
	default:
		return fmt.Errorf("scrt: unsupported field kind %d", kind)
	}
	row.SetByIndex(idx, val)
	return nil
}

func assignAnyToRow(row codec.Row, idx int, kind schema.FieldKind, src any) error {
	if src == nil {
		return nil
	}
	var val codec.Value
	val.Set = true
	switch kind {
	case schema.KindBool:
		b, err := anyAsBool(src)
		if err != nil {
			return err
		}
		val.Bool = b
	case schema.KindInt64:
		i, err := anyAsInt(src)
		if err != nil {
			return err
		}
		val.Int = i
	case schema.KindUint64, schema.KindRef:
		u, err := anyAsUint(src)
		if err != nil {
			return err
		}
		val.Uint = u
	case schema.KindFloat64:
		f, err := anyAsFloat(src)
		if err != nil {
			return err
		}
		val.Float = f
	case schema.KindString:
		s, err := anyAsString(src)
		if err != nil {
			return err
		}
		val.Str = s
	case schema.KindBytes:
		b, err := anyAsBytes(src)
		if err != nil {
			return err
		}
		val.Bytes = b
	default:
		return fmt.Errorf("scrt: unsupported field kind %d", kind)
	}
	row.SetByIndex(idx, val)
	return nil
}
