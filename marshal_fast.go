package scrt

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

type fieldSetter func([]codec.Value, unsafe.Pointer)

type fastStructEncoder struct {
	setters []fieldSetter
}

var fastEncoderCache sync.Map

func fastEncoderForStruct(t reflect.Type, s *schema.Schema) *fastStructEncoder {
	if t.Kind() != reflect.Struct {
		return nil
	}
	key := structBindingKey{typeKey: t, schemaKey: s}
	if cached, ok := fastEncoderCache.Load(key); ok {
		return cached.(*fastStructEncoder)
	}
	enc := buildFastStructEncoder(t, s)
	fastEncoderCache.Store(key, enc)
	return enc
}

func buildFastStructEncoder(t reflect.Type, s *schema.Schema) *fastStructEncoder {
	bindings := structBindingsForSchema(t, s)
	setters := make([]fieldSetter, len(s.Fields))
	for idx, binding := range bindings {
		if len(binding.index) == 0 {
			return nil
		}
		field := t.FieldByIndex(binding.index)
		setter, ok := makeFieldSetter(idx, field, s.Fields[idx])
		if !ok {
			return nil
		}
		setters[idx] = setter
	}
	return &fastStructEncoder{setters: setters}
}

func makeFieldSetter(idx int, field reflect.StructField, schemaField schema.Field) (fieldSetter, bool) {
	offset := field.Offset
	kind := schemaField.ValueKind()
	switch kind {
	case schema.KindUint64:
		if field.Type.Kind() != reflect.Uint64 {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Uint = *(*uint64)(ptr)
			vals[idx].Str = ""
			vals[idx].Set = true
		}, true
	case schema.KindInt64:
		if field.Type.Kind() != reflect.Int64 {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Int = *(*int64)(ptr)
			vals[idx].Set = true
		}, true
	case schema.KindFloat64:
		if field.Type.Kind() != reflect.Float64 {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Float = *(*float64)(ptr)
			vals[idx].Set = true
		}, true
	case schema.KindBool:
		if field.Type.Kind() != reflect.Bool {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Bool = *(*bool)(ptr)
			vals[idx].Set = true
		}, true
	case schema.KindString:
		if field.Type.Kind() != reflect.String {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Str = *(*string)(ptr)
			vals[idx].Set = true
		}, true
	case schema.KindBytes:
		if field.Type.Kind() != reflect.Slice || field.Type.Elem().Kind() != reflect.Uint8 {
			return nil, false
		}
		return func(vals []codec.Value, base unsafe.Pointer) {
			ptr := unsafe.Pointer(uintptr(base) + offset)
			vals[idx].Bytes = *(*[]byte)(ptr)
			vals[idx].Borrowed = false
			vals[idx].Set = true
		}, true
	default:
		return nil, false
	}
}

func (f *fastStructEncoder) encode(row codec.Row, value reflect.Value) {
	if f == nil || !value.CanAddr() {
		return
	}
	vals := row.Values()
	base := unsafe.Pointer(value.UnsafeAddr())
	for _, setter := range f.setters {
		if setter != nil {
			setter(vals, base)
		}
	}
}
