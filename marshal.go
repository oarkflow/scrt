package scrt

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/temporal"
)

const nestedValueKey = "value"

var nestedValueAliases = []string{
	nestedValueKey,
	"$value",
	"$",
	"Value",
	"$data",
	"data",
	"Data",
	"raw",
	"Raw",
}

type signedInt interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type unsignedInt interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type floatNumber interface {
	~float32 | ~float64
}

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
	if flattened, ok := flattenNestedMap(value); ok {
		return populateRowFromMapAny(row, flattened, s)
	}
	switch data := value.Interface().(type) {
	case map[string]any:
		return populateRowFromMapAny(row, data, s)
	case map[string]bool:
		return populateRowFromMapBool(row, data, s)
	case map[string]int:
		return populateRowFromMapInt(row, data, s)
	case map[string]int8:
		return populateRowFromMapInt8(row, data, s)
	case map[string]int16:
		return populateRowFromMapInt16(row, data, s)
	case map[string]int32:
		return populateRowFromMapInt32(row, data, s)
	case map[string]int64:
		return populateRowFromMapInt64(row, data, s)
	case map[string]uint:
		return populateRowFromMapUint(row, data, s)
	case map[string]uint8:
		return populateRowFromMapUint8(row, data, s)
	case map[string]uint16:
		return populateRowFromMapUint16(row, data, s)
	case map[string]uint32:
		return populateRowFromMapUint32(row, data, s)
	case map[string]uint64:
		return populateRowFromMapUint64(row, data, s)
	case map[string]float64:
		return populateRowFromMapFloat64(row, data, s)
	case map[string]float32:
		return populateRowFromMapFloat32(row, data, s)
	case map[string]string:
		return populateRowFromMapString(row, data, s)
	case map[string][]byte:
		return populateRowFromMapBytes(row, data, s)
	case map[string]time.Time:
		return populateRowFromMapTime(row, data, s)
	case map[string]time.Duration:
		return populateRowFromMapDuration(row, data, s)
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
	return populateRowFromMapSigned(row, data, s)
}

func populateRowFromMapInt8(row codec.Row, data map[string]int8, s *schema.Schema) error {
	return populateRowFromMapSigned(row, data, s)
}

func populateRowFromMapInt16(row codec.Row, data map[string]int16, s *schema.Schema) error {
	return populateRowFromMapSigned(row, data, s)
}

func populateRowFromMapInt32(row codec.Row, data map[string]int32, s *schema.Schema) error {
	return populateRowFromMapSigned(row, data, s)
}

func populateRowFromMapInt64(row codec.Row, data map[string]int64, s *schema.Schema) error {
	return populateRowFromMapSigned(row, data, s)
}

func populateRowFromMapSigned[T signedInt](row codec.Row, data map[string]T, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind == schema.KindInt64 {
			var val codec.Value
			val.Set = true
			val.Int = int64(v)
			row.SetByIndex(idx, val)
			continue
		}
		if intStoredKind(field.Kind) {
			if err := assignValueToRow(row, idx, field.Kind, reflect.ValueOf(int64(v))); err != nil {
				return fmt.Errorf("scrt: field %s: %w", field.Name, err)
			}
			continue
		}
		return fmt.Errorf("scrt: field %s expects int64, got kind %d", field.Name, field.Kind)
	}
	return nil
}

func populateRowFromMapUint(row codec.Row, data map[string]uint, s *schema.Schema) error {
	return populateRowFromMapUnsigned(row, data, s)
}

func populateRowFromMapUint8(row codec.Row, data map[string]uint8, s *schema.Schema) error {
	return populateRowFromMapUnsigned(row, data, s)
}

func populateRowFromMapUint16(row codec.Row, data map[string]uint16, s *schema.Schema) error {
	return populateRowFromMapUnsigned(row, data, s)
}

func populateRowFromMapUint32(row codec.Row, data map[string]uint32, s *schema.Schema) error {
	return populateRowFromMapUnsigned(row, data, s)
}

func populateRowFromMapUint64(row codec.Row, data map[string]uint64, s *schema.Schema) error {
	return populateRowFromMapUnsigned(row, data, s)
}

func populateRowFromMapUnsigned[T unsignedInt](row codec.Row, data map[string]T, s *schema.Schema) error {
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

func populateRowFromMapFloat32(row codec.Row, data map[string]float32, s *schema.Schema) error {
	return populateRowFromMapFloat(row, data, s)
}

func populateRowFromMapFloat64(row codec.Row, data map[string]float64, s *schema.Schema) error {
	return populateRowFromMapFloat(row, data, s)
}

func populateRowFromMapFloat[T floatNumber](row codec.Row, data map[string]T, s *schema.Schema) error {
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
		val.Float = float64(v)
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
		if field.Kind == schema.KindString {
			var val codec.Value
			val.Set = true
			val.Str = v
			row.SetByIndex(idx, val)
			continue
		}
		if err := assignValueToRow(row, idx, field.Kind, reflect.ValueOf(v)); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
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

func populateRowFromMapTime(row codec.Row, data map[string]time.Time, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if !isTemporalField(field.Kind) {
			return fmt.Errorf("scrt: field %s expects temporal kind, got %d", field.Name, field.Kind)
		}
		if err := assignValueToRow(row, idx, field.Kind, reflect.ValueOf(v)); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
	}
	return nil
}

func populateRowFromMapDuration(row codec.Row, data map[string]time.Duration, s *schema.Schema) error {
	for idx, field := range s.Fields {
		v, ok := data[field.Name]
		if !ok {
			continue
		}
		if field.Kind != schema.KindDuration {
			return fmt.Errorf("scrt: field %s expects duration kind, got %d", field.Name, field.Kind)
		}
		if err := assignValueToRow(row, idx, field.Kind, reflect.ValueOf(v)); err != nil {
			return fmt.Errorf("scrt: field %s: %w", field.Name, err)
		}
	}
	return nil
}

func encodeTemporalInt(kind schema.FieldKind, t time.Time) int64 {
	switch kind {
	case schema.KindDate:
		return temporal.EncodeDate(t)
	case schema.KindDateTime, schema.KindTimestamp:
		return temporal.EncodeInstant(t)
	default:
		return temporal.EncodeInstant(t)
	}
}

func isTemporalField(kind schema.FieldKind) bool {
	switch kind {
	case schema.KindDate, schema.KindDateTime, schema.KindTimestamp, schema.KindTimestampTZ:
		return true
	default:
		return false
	}
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dup := make([]byte, len(src))
	copy(dup, src)
	return dup
}

func flattenNestedMap(value reflect.Value) (map[string]any, bool) {
	if value.Type().Elem().Kind() != reflect.Map {
		return nil, false
	}
	if value.Type().Elem().Key().Kind() != reflect.String {
		return nil, false
	}
	flattened := make(map[string]any, value.Len())
	iter := value.MapRange()
	for iter.Next() {
		fieldName := iter.Key().String()
		inner := indirect(iter.Value())
		if inner.Kind() != reflect.Map || inner.Type().Key().Kind() != reflect.String {
			continue
		}
		if actual, ok := nestedMapValue(inner, fieldName); ok {
			flattened[fieldName] = actual.Interface()
		}
	}
	if len(flattened) == 0 {
		return nil, false
	}
	return flattened, true
}

func nestedMapValue(inner reflect.Value, fieldName string) (reflect.Value, bool) {
	var fallback reflect.Value
	aliasSet := make(map[string]struct{}, len(nestedValueAliases)+1)
	for _, alias := range nestedValueAliases {
		aliasSet[alias] = struct{}{}
	}
	aliasSet[fieldName] = struct{}{}
	iter := inner.MapRange()
	for iter.Next() {
		keyVal := iter.Key()
		if keyVal.Kind() != reflect.String {
			continue
		}
		key := keyVal.String()
		val := indirect(iter.Value())
		if _, ok := aliasSet[key]; ok {
			return val, true
		}
		if !fallback.IsValid() {
			fallback = val
		}
	}
	if fallback.IsValid() {
		return fallback, true
	}
	return reflect.Value{}, false
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
	case schema.KindDate, schema.KindDateTime, schema.KindTimestamp:
		t, err := valueAsTime(v, kind)
		if err != nil {
			return err
		}
		val.Int = encodeTemporalInt(kind, t)
	case schema.KindTimestampTZ:
		t, err := valueAsTime(v, kind)
		if err != nil {
			return err
		}
		val.Str = temporal.FormatTimestampTZ(t)
	case schema.KindDuration:
		d, err := valueAsDuration(v)
		if err != nil {
			return err
		}
		val.Int = int64(d)
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
	case schema.KindDate, schema.KindDateTime, schema.KindTimestamp:
		t, err := anyAsTime(kind, src)
		if err != nil {
			return err
		}
		val.Int = encodeTemporalInt(kind, t)
	case schema.KindTimestampTZ:
		t, err := anyAsTime(kind, src)
		if err != nil {
			return err
		}
		val.Str = temporal.FormatTimestampTZ(t)
	case schema.KindDuration:
		d, err := anyAsDuration(src)
		if err != nil {
			return err
		}
		val.Int = int64(d)
	default:
		return fmt.Errorf("scrt: unsupported field kind %d", kind)
	}
	row.SetByIndex(idx, val)
	return nil
}
