package scrt

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/temporal"
)

var (
	structCache        sync.Map
	structBindingCache sync.Map
	stringerType       = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	timeType           = reflect.TypeOf(time.Time{})
	durationType       = reflect.TypeOf(time.Duration(0))
)

type structBindingKey struct {
	typeKey   reflect.Type
	schemaKey *schema.Schema
}

type structDescriptor struct {
	fields map[string]structField
}

type structField struct {
	index []int
}

func describeStruct(t reflect.Type) *structDescriptor {
	if v, ok := structCache.Load(t); ok {
		return v.(*structDescriptor)
	}
	desc := &structDescriptor{fields: make(map[string]structField)}
	num := t.NumField()
	for i := 0; i < num; i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("scrt")
		if tag == "-" {
			continue
		}
		name := field.Name
		if tag != "" {
			name = tag
		}
		desc.fields[name] = structField{index: field.Index}
	}
	structCache.Store(t, desc)
	return desc
}

func (d *structDescriptor) lookup(v reflect.Value, field string) (reflect.Value, bool) {
	if d == nil {
		return reflect.Value{}, false
	}
	def, ok := d.fields[field]
	if !ok {
		return reflect.Value{}, false
	}
	return v.FieldByIndex(def.index), true
}

func structBindingsForSchema(t reflect.Type, s *schema.Schema) []structField {
	if t == nil || s == nil {
		return nil
	}
	key := structBindingKey{typeKey: t, schemaKey: s}
	if cached, ok := structBindingCache.Load(key); ok {
		return cached.([]structField)
	}
	desc := describeStruct(t)
	bindings := make([]structField, len(s.Fields))
	if desc != nil {
		for idx, field := range s.Fields {
			if sf, ok := desc.fields[field.Name]; ok {
				bindings[idx] = sf
			}
		}
	}
	structBindingCache.Store(key, bindings)
	return bindings
}

func indirect(v reflect.Value) reflect.Value {
	for {
		if !v.IsValid() {
			return v
		}
		if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}
			}
			v = v.Elem()
			continue
		}
		break
	}
	return v
}

func valueAsBool(v reflect.Value) (bool, error) {
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() != 0, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() != 0, nil
	case reflect.String:
		b, err := strconv.ParseBool(v.String())
		if err != nil {
			return false, fmt.Errorf("scrt: cannot convert %q to bool", v.String())
		}
		return b, nil
	default:
		return false, fmt.Errorf("scrt: unsupported bool source %s", v.Kind())
	}
}

func valueAsInt(v reflect.Value) (int64, error) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		val := v.Uint()
		if val > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: value %d overflows int64", val)
		}
		return int64(val), nil
	case reflect.String:
		i, err := strconv.ParseInt(v.String(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as int64", v.String())
		}
		return i, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported int source %s", v.Kind())
	}
}

func valueAsUint(v reflect.Value) (uint64, error) {
	switch v.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val := v.Int()
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case reflect.String:
		u, err := strconv.ParseUint(v.String(), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as uint64", v.String())
		}
		return u, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported uint source %s", v.Kind())
	}
}

func valueAsFloat(v reflect.Value) (float64, error) {
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Convert(reflect.TypeOf(float64(0))).Float(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(v.Uint()), nil
	case reflect.String:
		f, err := strconv.ParseFloat(v.String(), 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as float64", v.String())
		}
		return f, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported float source %s", v.Kind())
	}
}

func valueAsString(v reflect.Value) (string, error) {
	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return string(v.Bytes()), nil
		}
	}
	if v.Type().Implements(stringerType) {
		return v.Interface().(fmt.Stringer).String(), nil
	}
	return fmt.Sprintf("%v", v.Interface()), nil
}

func valueAsBytes(v reflect.Value) ([]byte, error) {
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		src := make([]byte, v.Len())
		copy(src, v.Bytes())
		return src, nil
	}
	if v.Kind() == reflect.String {
		data := []byte(v.String())
		return data, nil
	}
	return nil, fmt.Errorf("scrt: unsupported bytes source %s", v.Kind())
}

func anyAsBool(v any) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int:
		return val != 0, nil
	case int8:
		return val != 0, nil
	case int16:
		return val != 0, nil
	case int32:
		return val != 0, nil
	case int64:
		return val != 0, nil
	case uint:
		return val != 0, nil
	case uint8:
		return val != 0, nil
	case uint16:
		return val != 0, nil
	case uint32:
		return val != 0, nil
	case uint64:
		return val != 0, nil
	case uintptr:
		return val != 0, nil
	case string:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("scrt: cannot convert %q to bool", val)
		}
		return b, nil
	default:
		return false, fmt.Errorf("scrt: unsupported bool source %T", v)
	}
}

func anyAsInt(v any) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		if uint64(val) > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: value %d overflows int64", val)
		}
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		if val > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: value %d overflows int64", val)
		}
		return int64(val), nil
	case uintptr:
		if uint64(val) > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: value %d overflows int64", val)
		}
		return int64(val), nil
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as int64", val)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported int source %T", v)
	}
}

func anyAsUint(v any) (uint64, error) {
	switch val := v.(type) {
	case uint:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint64:
		return val, nil
	case uintptr:
		return uint64(val), nil
	case int:
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int8:
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int16:
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int32:
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("scrt: negative value %d cannot convert to uint64", val)
		}
		return uint64(val), nil
	case string:
		u, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as uint64", val)
		}
		return u, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported uint source %T", v)
	}
}

func anyAsFloat(v any) (float64, error) {
	switch val := v.(type) {
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uintptr:
		return float64(val), nil
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("scrt: cannot parse %q as float64", val)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("scrt: unsupported float source %T", v)
	}
}

func anyAsString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	case fmt.Stringer:
		return val.String(), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func anyAsBytes(v any) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		dup := make([]byte, len(val))
		copy(dup, val)
		return dup, nil
	case string:
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("scrt: unsupported bytes source %T", v)
	}
}

func valueAsTime(v reflect.Value, kind schema.FieldKind) (time.Time, error) {
	v = indirect(v)
	if !v.IsValid() {
		return time.Time{}, fmt.Errorf("scrt: invalid time value")
	}
	if v.Type() == timeType {
		return v.Interface().(time.Time), nil
	}
	switch v.Kind() {
	case reflect.String:
		return parseTemporalString(kind, v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return decodeTemporalFromInt(kind, v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value := v.Uint()
		if value > math.MaxInt64 {
			return time.Time{}, fmt.Errorf("scrt: epoch value %d overflows", value)
		}
		return decodeTemporalFromInt(kind, int64(value)), nil
	case reflect.Float32, reflect.Float64:
		seconds := v.Convert(reflect.TypeOf(float64(0))).Float()
		sec, frac := math.Modf(seconds)
		return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC(), nil
	}
	if v.Type().ConvertibleTo(timeType) {
		converted := v.Convert(timeType)
		return converted.Interface().(time.Time), nil
	}
	return time.Time{}, fmt.Errorf("scrt: unsupported time source %s", v.Kind())
}

func anyAsTime(kind schema.FieldKind, value any) (time.Time, error) {
	switch val := value.(type) {
	case time.Time:
		return val, nil
	case *time.Time:
		if val == nil {
			return time.Time{}, fmt.Errorf("scrt: nil *time.Time")
		}
		return *val, nil
	case string:
		return parseTemporalString(kind, val)
	case fmt.Stringer:
		return parseTemporalString(kind, val.String())
	case int:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case int8:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case int16:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case int32:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case int64:
		return decodeTemporalFromInt(kind, val), nil
	case uint:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case uint8:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case uint16:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case uint32:
		return decodeTemporalFromInt(kind, int64(val)), nil
	case uint64:
		if val > math.MaxInt64 {
			return time.Time{}, fmt.Errorf("scrt: epoch value %d overflows", val)
		}
		return decodeTemporalFromInt(kind, int64(val)), nil
	case float32:
		sec, frac := math.Modf(float64(val))
		return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC(), nil
	case float64:
		sec, frac := math.Modf(val)
		return time.Unix(int64(sec), int64(frac*float64(time.Second))).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("scrt: unsupported time source %T", value)
	}
}

func valueAsDuration(v reflect.Value) (time.Duration, error) {
	v = indirect(v)
	if !v.IsValid() {
		return 0, fmt.Errorf("scrt: invalid duration value")
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return time.Duration(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value := v.Uint()
		if value > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: duration value %d overflows", value)
		}
		return time.Duration(value), nil
	case reflect.Float32, reflect.Float64:
		seconds := v.Convert(reflect.TypeOf(float64(0))).Float()
		return time.Duration(seconds * float64(time.Second)), nil
	case reflect.String:
		return temporal.ParseDuration(v.String())
	}
	if v.Type() == durationType {
		return v.Interface().(time.Duration), nil
	}
	return 0, fmt.Errorf("scrt: unsupported duration source %s", v.Kind())
}

func anyAsDuration(value any) (time.Duration, error) {
	switch val := value.(type) {
	case time.Duration:
		return val, nil
	case *time.Duration:
		if val == nil {
			return 0, fmt.Errorf("scrt: nil *time.Duration")
		}
		return *val, nil
	case string:
		return temporal.ParseDuration(val)
	case int:
		return time.Duration(val), nil
	case int8:
		return time.Duration(val), nil
	case int16:
		return time.Duration(val), nil
	case int32:
		return time.Duration(val), nil
	case int64:
		return time.Duration(val), nil
	case uint:
		return time.Duration(val), nil
	case uint8:
		return time.Duration(val), nil
	case uint16:
		return time.Duration(val), nil
	case uint32:
		return time.Duration(val), nil
	case uint64:
		if val > math.MaxInt64 {
			return 0, fmt.Errorf("scrt: duration value %d overflows", val)
		}
		return time.Duration(val), nil
	case float32:
		return time.Duration(float64(val) * float64(time.Second)), nil
	case float64:
		return time.Duration(val * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("scrt: unsupported duration source %T", value)
	}
}

func parseTemporalString(kind schema.FieldKind, input string) (time.Time, error) {
	switch kind {
	case schema.KindDate:
		return temporal.ParseDate(input)
	case schema.KindDateTime:
		return temporal.ParseDateTime(input)
	case schema.KindTimestamp:
		return temporal.ParseTimestamp(input)
	case schema.KindTimestampTZ:
		return temporal.ParseTimestampTZ(input)
	default:
		return temporal.ParseTimestamp(input)
	}
}

func decodeTemporalFromInt(kind schema.FieldKind, raw int64) time.Time {
	switch kind {
	case schema.KindDate:
		return temporal.DecodeDate(raw)
	case schema.KindDateTime, schema.KindTimestamp, schema.KindTimestampTZ:
		return temporal.DecodeInstant(raw)
	default:
		return temporal.DecodeInstant(raw)
	}
}
