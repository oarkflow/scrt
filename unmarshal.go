package scrt

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

// UnmarshalOptions controls decoding behavior.
type UnmarshalOptions struct {
	ZeroCopyBytes bool
}

// UnmarshalOption mutates UnmarshalOptions.
type UnmarshalOption func(*UnmarshalOptions)

// WithZeroCopyBytes enables returning byte slices backed by the input buffer.
// Callers must treat returned slices as read-only and valid only until the next
// page of data is read.
func WithZeroCopyBytes() UnmarshalOption {
	return func(o *UnmarshalOptions) {
		o.ZeroCopyBytes = true
	}
}

// Unmarshal decodes SCRT binary data into the provided output pointer.
func Unmarshal(data []byte, s *schema.Schema, out any) error {
	return UnmarshalWithOptions(data, s, out)
}

// UnmarshalWithOptions decodes SCRT data with additional options.
func UnmarshalWithOptions(data []byte, s *schema.Schema, out any, opts ...UnmarshalOption) error {
	if s == nil {
		return fmt.Errorf("scrt: schema is required")
	}
	cfg := UnmarshalOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}
	reader := codec.NewReaderWithOptions(bytes.NewReader(data), s, codec.Options{ZeroCopyBytes: cfg.ZeroCopyBytes})
	return decodeInto(reader, s, out)
}

// UnmarshalFromFile decodes SCRT binary data stored on disk.
func UnmarshalFromFile(path string, s *schema.Schema, out any) error {
	return UnmarshalFromFileWithOptions(path, s, out)
}

// UnmarshalFromFileWithOptions decodes SCRT binary data stored on disk using options.
func UnmarshalFromFileWithOptions(path string, s *schema.Schema, out any, opts ...UnmarshalOption) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return UnmarshalWithOptions(data, s, out, opts...)
}

// UnmarshalFiles loads schemaName from schemaPath and decodes dataPath into out.
func UnmarshalFiles(schemaPath, schemaName, dataPath string, out any) error {
	return UnmarshalFilesWithOptions(schemaPath, schemaName, dataPath, out)
}

// UnmarshalFilesWithOptions loads schemaName from schemaPath and decodes dataPath into out using options.
func UnmarshalFilesWithOptions(schemaPath, schemaName, dataPath string, out any, opts ...UnmarshalOption) error {
	doc, err := schema.ParseFile(schemaPath)
	if err != nil {
		return err
	}
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return fmt.Errorf("scrt: schema %q not found in %s", schemaName, schemaPath)
	}
	return UnmarshalFromFileWithOptions(dataPath, sch, out, opts...)
}

func decodeInto(reader *codec.Reader, s *schema.Schema, out any) error {
	if out == nil {
		return fmt.Errorf("scrt: output cannot be nil")
	}
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("scrt: output must be a non-nil pointer")
	}
	target := rv.Elem()
	row := codec.AcquireRow(s)
	defer codec.ReleaseRow(row)
	switch target.Kind() {
	case reflect.Slice:
		return decodeIntoSlice(reader, s, target, *row)
	case reflect.Struct, reflect.Map:
		return decodeSingleValue(reader, s, target, *row)
	default:
		return fmt.Errorf("scrt: unsupported output kind %s", target.Kind())
	}
}

func decodeIntoSlice(reader *codec.Reader, s *schema.Schema, slice reflect.Value, row codec.Row) error {
	elemType := slice.Type().Elem()
	idx := slice.Len()
	for {
		row.Reset()
		ok, err := reader.ReadRow(row)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !ok {
			break
		}
		if slice.Cap() <= idx {
			slice = growSlice(slice, idx+1)
		}
		if slice.Len() < idx+1 {
			slice.SetLen(idx + 1)
		}
		dest := slice.Index(idx)
		if elemType.Kind() == reflect.Pointer {
			val := reflect.New(elemType.Elem())
			dest.Set(val)
			if err := assignRowToValue(row, val.Elem(), s); err != nil {
				return err
			}
		} else {
			if err := assignRowToValue(row, dest, s); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func decodeSingleValue(reader *codec.Reader, s *schema.Schema, dst reflect.Value, row codec.Row) error {
	row.Reset()
	ok, err := reader.ReadRow(row)
	if err != nil {
		return err
	}
	if !ok {
		return io.EOF
	}
	if err := assignRowToValue(row, dst, s); err != nil {
		return err
	}
	row.Reset()
	more, err := reader.ReadRow(row)
	if err != nil && err != io.EOF {
		return err
	}
	if more {
		return fmt.Errorf("scrt: multiple rows present, use a slice output")
	}
	return nil
}

func growSlice(slice reflect.Value, needed int) reflect.Value {
	if slice.Cap() >= needed {
		return slice
	}
	newCap := slice.Cap()*2 + 1
	if newCap < needed {
		newCap = needed
	}
	newSlice := reflect.MakeSlice(slice.Type(), slice.Len(), newCap)
	reflect.Copy(newSlice, slice)
	slice.Set(newSlice)
	return slice
}

func assignRowToValue(row codec.Row, dst reflect.Value, s *schema.Schema) error {
	dst = indirect(dst)
	switch dst.Kind() {
	case reflect.Struct:
		return assignRowToStruct(row, dst, s)
	case reflect.Map:
		if dst.IsNil() {
			dst.Set(reflect.MakeMap(dst.Type()))
		}
		return assignRowToMap(row, dst, s)
	default:
		return fmt.Errorf("scrt: unsupported destination kind %s", dst.Kind())
	}
}

func assignRowToStruct(row codec.Row, dst reflect.Value, s *schema.Schema) error {
	bindings := structBindingsForSchema(dst.Type(), s)
	vals := row.Values()
	for idx, binding := range bindings {
		if len(binding.index) == 0 {
			continue
		}
		fv := dst.FieldByIndex(binding.index)
		if !fv.IsValid() || !fv.CanSet() {
			continue
		}
		base := valueFromRow(s.Fields[idx].Kind, vals[idx])
		if base == nil {
			continue
		}
		if err := assignInterface(fv, base); err != nil {
			return fmt.Errorf("scrt: field %s: %w", s.Fields[idx].Name, err)
		}
	}
	return nil
}

func assignRowToMap(row codec.Row, dst reflect.Value, s *schema.Schema) error {
	if dst.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("scrt: map key must be string, got %s", dst.Type().Key())
	}
	elemType := dst.Type().Elem()
	vals := row.Values()
	for idx, field := range s.Fields {
		base := valueFromRow(field.Kind, vals[idx])
		if base == nil {
			continue
		}
		val, err := convertInterface(base, elemType)
		if err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
		dst.SetMapIndex(reflect.ValueOf(field.Name), val)
	}
	return nil
}

func valueFromRow(kind schema.FieldKind, v codec.Value) interface{} {
	switch kind {
	case schema.KindBool:
		return v.Bool
	case schema.KindInt64:
		return v.Int
	case schema.KindUint64, schema.KindRef:
		return v.Uint
	case schema.KindFloat64:
		return v.Float
	case schema.KindString:
		return v.Str
	case schema.KindBytes:
		if v.Bytes == nil {
			return []byte(nil)
		}
		if v.Borrowed {
			return v.Bytes
		}
		buf := make([]byte, len(v.Bytes))
		copy(buf, v.Bytes)
		return buf
	default:
		return nil
	}
}

func assignInterface(field reflect.Value, value interface{}) error {
	if !field.CanSet() {
		return fmt.Errorf("cannot set field %s", field.Type())
	}
	if value == nil {
		field.Set(reflect.Zero(field.Type()))
		return nil
	}
	val := reflect.ValueOf(value)
	if val.Type().AssignableTo(field.Type()) {
		field.Set(val)
		return nil
	}
	if val.Type().ConvertibleTo(field.Type()) {
		field.Set(val.Convert(field.Type()))
		return nil
	}
	if field.Kind() == reflect.Pointer {
		elem := reflect.New(field.Type().Elem())
		if err := assignInterface(elem.Elem(), value); err != nil {
			return err
		}
		field.Set(elem)
		return nil
	}
	return fmt.Errorf("cannot assign %T to %s", value, field.Type())
}

func convertInterface(value interface{}, typ reflect.Type) (reflect.Value, error) {
	if typ.Kind() == reflect.Interface && typ.NumMethod() == 0 {
		return reflect.ValueOf(value), nil
	}
	val := reflect.ValueOf(value)
	if val.Type().AssignableTo(typ) {
		return val, nil
	}
	if val.Type().ConvertibleTo(typ) {
		return val.Convert(typ), nil
	}
	if typ.Kind() == reflect.Pointer {
		elem := reflect.New(typ.Elem())
		if err := assignInterface(elem.Elem(), value); err != nil {
			return reflect.Value{}, err
		}
		return elem, nil
	}
	return reflect.Value{}, fmt.Errorf("cannot convert %T to %s", value, typ)
}
