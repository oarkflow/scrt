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

// Unmarshal decodes SCRT binary data into the provided output pointer.
func Unmarshal(data []byte, s *schema.Schema, out any) error {
	if s == nil {
		return fmt.Errorf("scrt: schema is required")
	}
	reader := codec.NewReader(bytes.NewReader(data), s)
	return decodeInto(reader, s, out)
}

// UnmarshalFromFile decodes SCRT binary data stored on disk.
func UnmarshalFromFile(path string, s *schema.Schema, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return Unmarshal(data, s, out)
}

// UnmarshalFiles loads schemaName from schemaPath and decodes dataPath into out.
func UnmarshalFiles(schemaPath, schemaName, dataPath string, out any) error {
	doc, err := schema.ParseFile(schemaPath)
	if err != nil {
		return err
	}
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return fmt.Errorf("scrt: schema %q not found in %s", schemaName, schemaPath)
	}
	return UnmarshalFromFile(dataPath, sch, out)
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
	row := codec.NewRow(s)
	switch target.Kind() {
	case reflect.Slice:
		return decodeIntoSlice(reader, s, target, row)
	case reflect.Struct, reflect.Map:
		return decodeSingleValue(reader, s, target, row)
	default:
		return fmt.Errorf("scrt: unsupported output kind %s", target.Kind())
	}
}

func decodeIntoSlice(reader *codec.Reader, s *schema.Schema, slice reflect.Value, row codec.Row) error {
	elemType := slice.Type().Elem()
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
		appendVal, dest := allocateElement(elemType)
		if err := assignRowToValue(row, dest, s); err != nil {
			return err
		}
		slice.Set(reflect.Append(slice, appendVal))
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

func allocateElement(elemType reflect.Type) (reflect.Value, reflect.Value) {
	if elemType.Kind() == reflect.Pointer {
		ptr := reflect.New(elemType.Elem())
		return ptr, ptr.Elem()
	}
	val := reflect.New(elemType).Elem()
	return val, val
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
	desc := describeStruct(dst.Type())
	vals := row.Values()
	for idx, field := range s.Fields {
		fv, ok := desc.lookup(dst, field.Name)
		if !ok || !fv.CanSet() {
			continue
		}
		base := valueFromRow(field.Kind, vals[idx])
		if base == nil {
			continue
		}
		if err := assignInterface(fv, base); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
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
