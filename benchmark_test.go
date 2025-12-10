package scrt_test

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
)

type BenchMessage struct {
	MsgID uint64
	User  uint64
	Text  string
	Lang  string
	Seen  bool
}

var benchSchema = &schema.Schema{
	Name: "Message",
	Fields: []schema.Field{
		{Name: "MsgID", Kind: schema.KindUint64, RawType: "uint64"},
		{Name: "User", Kind: schema.KindRef, RawType: "ref:User:ID"},
		{Name: "Text", Kind: schema.KindString, RawType: "string"},
		{Name: "Lang", Kind: schema.KindString, RawType: "string"},
		{Name: "Seen", Kind: schema.KindBool, RawType: "bool"},
	},
}

var counterSchema = &schema.Schema{
	Name: "Counter",
	Fields: []schema.Field{
		{Name: "CountA", Kind: schema.KindUint64, RawType: "uint64"},
		{Name: "CountB", Kind: schema.KindUint64, RawType: "uint64"},
		{Name: "CountC", Kind: schema.KindUint64, RawType: "uint64"},
	},
}

var bytesSchema = &schema.Schema{
	Name: "Binary",
	Fields: []schema.Field{
		{Name: "ID", Kind: schema.KindUint64, RawType: "uint64"},
		{Name: "Payload", Kind: schema.KindBytes, RawType: "bytes"},
	},
}

func generateMessages(n int) []BenchMessage {
	messages := make([]BenchMessage, n)
	for i := 0; i < n; i++ {
		messages[i] = BenchMessage{
			MsgID: uint64(i + 1),
			User:  uint64(1000 + (i % 100)),
			Text:  "This is a test message with some content to make it realistic",
			Lang:  "en",
			Seen:  i%2 == 0,
		}
	}
	return messages
}

func generateMessageMaps(n int) []map[string]interface{} {
	messages := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		messages[i] = map[string]interface{}{
			"MsgID": uint64(i + 1),
			"User":  uint64(1000 + (i % 100)),
			"Text":  "This is a test message with some content to make it realistic",
			"Lang":  "en",
			"Seen":  i%2 == 0,
		}
	}
	return messages
}

func generateCounterMaps(n int) []map[string]uint64 {
	records := make([]map[string]uint64, n)
	for i := 0; i < n; i++ {
		records[i] = map[string]uint64{
			"CountA": uint64(i + 1),
			"CountB": uint64((i % 17) + 10),
			"CountC": uint64((i % 91) + 1000),
		}
	}
	return records
}

func generateNestedMessageMaps(n int) []map[string]map[string]any {
	records := make([]map[string]map[string]any, n)
	for i := 0; i < n; i++ {
		records[i] = map[string]map[string]any{
			"MsgID": {
				"value": uint64(i + 1),
				"meta":  map[string]any{"source": "msg"},
			},
			"User": {
				"value": uint64(1000 + (i % 100)),
				"meta":  map[string]any{"role": "tester"},
			},
			"Text": {
				"value": "Nested map payload",
				"meta":  map[string]any{"locale": "en"},
			},
			"Lang": {
				"value": "en",
			},
			"Seen": {
				"value": i%2 == 0,
				"meta":  map[string]any{"hint": "bool"},
			},
		}
	}
	return records
}

func initNestedResult(n int) []map[string]map[string]any {
	result := make([]map[string]map[string]any, n)
	for i := 0; i < n; i++ {
		result[i] = map[string]map[string]any{
			"MsgID": {"meta": map[string]any{"source": "msg"}},
			"User":  {"meta": map[string]any{"role": "tester"}},
			"Text":  {"meta": map[string]any{"locale": "en"}},
			"Lang":  {"meta": map[string]any{"hint": "lang"}},
			"Seen":  {"meta": map[string]any{"hint": "bool"}},
		}
	}
	return result
}

func generateBinaryMapRecords(n int, payloadSize int) []map[string]any {
	records := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		payload := bytes.Repeat([]byte{byte('A' + (i % 26))}, payloadSize)
		records[i] = map[string]any{
			"ID":      uint64(i + 1),
			"Payload": payload,
		}
	}
	return records
}

func BenchmarkSCRT_Marshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(benchSchema, messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Marshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Marshal_100(b *testing.B) {
	messages := generateMessages(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generateCSV(messages)
	}
}

func BenchmarkProto_Marshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := encodeProtoMessages(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Marshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(benchSchema, messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Marshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Marshal_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generateCSV(messages)
	}
}

func BenchmarkProto_Marshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := encodeProtoMessages(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Marshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(benchSchema, messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Marshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Marshal_10000(b *testing.B) {
	messages := generateMessages(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generateCSV(messages)
	}
}

func BenchmarkProto_Marshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := encodeProtoMessages(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Unmarshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Unmarshal_100(b *testing.B) {
	messages := generateMessages(100)
	data := generateCSV(messages)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := parseCSV(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProto_Unmarshal_Struct_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decodeProtoMessages(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Unmarshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Unmarshal_1000(b *testing.B) {
	messages := generateMessages(1000)
	data := generateCSV(messages)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := parseCSV(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProto_Unmarshal_Struct_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decodeProtoMessages(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCSV_Unmarshal_10000(b *testing.B) {
	messages := generateMessages(10000)
	data := generateCSV(messages)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := parseCSV(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProto_Unmarshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := decodeProtoMessages(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Unmarshal_Struct_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []BenchMessage
		err := json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDataSize_SCRT_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_JSON_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_PROTO_100(b *testing.B) {
	messages := generateMessages(100)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_SCRT_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_JSON_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_PROTO_1000(b *testing.B) {
	messages := generateMessages(1000)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_SCRT_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_JSON_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_PROTO_10000(b *testing.B) {
	messages := generateMessages(10000)
	data, err := encodeProtoMessages(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkRoundTrip_SCRT_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := scrt.Marshal(benchSchema, messages)
		if err != nil {
			b.Fatal(err)
		}
		var result []BenchMessage
		err = scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundTrip_JSON_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(messages)
		if err != nil {
			b.Fatal(err)
		}
		var result []BenchMessage
		err = json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundTrip_PROTO_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := encodeProtoMessages(messages)
		if err != nil {
			b.Fatal(err)
		}
		_, err = decodeProtoMessages(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Marshal_Map_1000(b *testing.B) {
	messages := generateMessageMaps(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(benchSchema, messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Marshal_TypedMap_1000(b *testing.B) {
	records := generateCounterMaps(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(counterSchema, records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Marshal_Map_1000(b *testing.B) {
	messages := generateMessageMaps(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(messages)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_Map_1000(b *testing.B) {
	messages := generateMessageMaps(1000)
	data, err := scrt.Marshal(benchSchema, messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []map[string]interface{}
		err := scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_TypedMap_1000(b *testing.B) {
	records := generateCounterMaps(1000)
	data, err := scrt.Marshal(counterSchema, records)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []map[string]uint64
		err := scrt.Unmarshal(data, counterSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Marshal_NestedMap_1000(b *testing.B) {
	records := generateNestedMessageMaps(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := scrt.Marshal(benchSchema, records)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_NestedMap_1000(b *testing.B) {
	records := generateNestedMessageMaps(1000)
	data, err := scrt.Marshal(benchSchema, records)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := initNestedResult(len(records))
		err := scrt.Unmarshal(data, benchSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_BytesMap_Copy(b *testing.B) {
	records := generateBinaryMapRecords(1000, 512)
	data, err := scrt.Marshal(bytesSchema, records)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []map[string]any
		err := scrt.Unmarshal(data, bytesSchema, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCRT_Unmarshal_BytesMap_ZeroCopy(b *testing.B) {
	records := generateBinaryMapRecords(1000, 512)
	data, err := scrt.Marshal(bytesSchema, records)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []map[string]any
		err := scrt.UnmarshalWithOptions(data, bytesSchema, &result, scrt.WithZeroCopyBytes())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_Unmarshal_Map_1000(b *testing.B) {
	messages := generateMessageMaps(1000)
	data, err := json.Marshal(messages)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result []map[string]interface{}
		err := json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// CSV Benchmarks

func generateCSV(messages []BenchMessage) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	// Write header
	w.Write([]string{"MsgID", "User", "Text", "Lang", "Seen"})
	// Write records
	for _, msg := range messages {
		w.Write([]string{
			strconv.FormatUint(msg.MsgID, 10),
			strconv.FormatUint(msg.User, 10),
			msg.Text,
			msg.Lang,
			strconv.FormatBool(msg.Seen),
		})
	}
	w.Flush()
	return buf.Bytes()
}

func parseCSV(data []byte) ([]BenchMessage, error) {
	r := csv.NewReader(bytes.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("no records")
	}
	// Skip header
	messages := make([]BenchMessage, 0, len(records)-1)
	for i := 1; i < len(records); i++ {
		if len(records[i]) != 5 {
			continue
		}
		msgID, _ := strconv.ParseUint(records[i][0], 10, 64)
		user, _ := strconv.ParseUint(records[i][1], 10, 64)
		seen, _ := strconv.ParseBool(records[i][4])
		messages = append(messages, BenchMessage{
			MsgID: msgID,
			User:  user,
			Text:  records[i][2],
			Lang:  records[i][3],
			Seen:  seen,
		})
	}
	return messages, nil
}

func BenchmarkDataSize_CSV_100(b *testing.B) {
	messages := generateMessages(100)
	data := generateCSV(messages)
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_CSV_1000(b *testing.B) {
	messages := generateMessages(1000)
	data := generateCSV(messages)
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkDataSize_CSV_10000(b *testing.B) {
	messages := generateMessages(10000)
	data := generateCSV(messages)
	b.ReportMetric(float64(len(data)), "bytes")
}

func BenchmarkRoundTrip_CSV_1000(b *testing.B) {
	messages := generateMessages(1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data := generateCSV(messages)
		_, err := parseCSV(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

const (
	protoWireVarint       = 0
	protoWireBytes        = 2
	protoEnvelopeFieldNum = 1
)

func encodeProtoMessages(messages []BenchMessage) ([]byte, error) {
	out := make([]byte, 0, len(messages)*64)
	scratch := make([]byte, 0, 64)
	for _, msg := range messages {
		scratch = scratch[:0]
		scratch = appendProtoMessage(scratch, msg)
		out = appendProtoTag(out, protoEnvelopeFieldNum, protoWireBytes)
		out = appendUvarint(out, uint64(len(scratch)))
		out = append(out, scratch...)
	}
	return out, nil
}

func decodeProtoMessages(data []byte) ([]BenchMessage, error) {
	result := make([]BenchMessage, 0, 32)
	for len(data) > 0 {
		field, wireType, consumed, err := readProtoTag(data)
		if err != nil {
			return nil, err
		}
		if field != protoEnvelopeFieldNum || wireType != protoWireBytes {
			return nil, fmt.Errorf("bench proto: unexpected envelope field %d wire %d", field, wireType)
		}
		data = data[consumed:]
		length, n, err := consumeUvarint(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]
		if int(length) > len(data) {
			return nil, fmt.Errorf("bench proto: truncated message payload")
		}
		payload := data[:length]
		msg, err := parseProtoMessage(payload)
		if err != nil {
			return nil, err
		}
		result = append(result, msg)
		data = data[length:]
	}
	return result, nil
}

func appendProtoMessage(dst []byte, msg BenchMessage) []byte {
	dst = appendProtoVarint(dst, 1, msg.MsgID)
	dst = appendProtoVarint(dst, 2, msg.User)
	dst = appendProtoBytes(dst, 3, []byte(msg.Text))
	dst = appendProtoBytes(dst, 4, []byte(msg.Lang))
	if msg.Seen {
		dst = appendProtoVarint(dst, 5, 1)
	} else {
		dst = appendProtoVarint(dst, 5, 0)
	}
	return dst
}

func parseProtoMessage(data []byte) (BenchMessage, error) {
	var msg BenchMessage
	for len(data) > 0 {
		field, wireType, consumed, err := readProtoTag(data)
		if err != nil {
			return msg, err
		}
		data = data[consumed:]
		switch field {
		case 1:
			if wireType != protoWireVarint {
				return msg, fmt.Errorf("bench proto: unexpected wire type %d for msg_id", wireType)
			}
			value, n, err := consumeUvarint(data)
			if err != nil {
				return msg, err
			}
			msg.MsgID = value
			data = data[n:]
		case 2:
			if wireType != protoWireVarint {
				return msg, fmt.Errorf("bench proto: unexpected wire type %d for user", wireType)
			}
			value, n, err := consumeUvarint(data)
			if err != nil {
				return msg, err
			}
			msg.User = value
			data = data[n:]
		case 3:
			if wireType != protoWireBytes {
				return msg, fmt.Errorf("bench proto: unexpected wire type %d for text", wireType)
			}
			str, n, err := consumeBytes(data)
			if err != nil {
				return msg, err
			}
			msg.Text = str
			data = data[n:]
		case 4:
			if wireType != protoWireBytes {
				return msg, fmt.Errorf("bench proto: unexpected wire type %d for lang", wireType)
			}
			str, n, err := consumeBytes(data)
			if err != nil {
				return msg, err
			}
			msg.Lang = str
			data = data[n:]
		case 5:
			if wireType != protoWireVarint {
				return msg, fmt.Errorf("bench proto: unexpected wire type %d for seen", wireType)
			}
			value, n, err := consumeUvarint(data)
			if err != nil {
				return msg, err
			}
			msg.Seen = value != 0
			data = data[n:]
		default:
			skipped, err := skipProtoField(wireType, data)
			if err != nil {
				return msg, err
			}
			data = data[skipped:]
		}
	}
	return msg, nil
}

func appendProtoVarint(dst []byte, field int, value uint64) []byte {
	dst = appendProtoTag(dst, field, protoWireVarint)
	return appendUvarint(dst, value)
}

func appendProtoBytes(dst []byte, field int, value []byte) []byte {
	dst = appendProtoTag(dst, field, protoWireBytes)
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendProtoTag(dst []byte, field int, wireType int) []byte {
	tag := uint64(field<<3 | wireType)
	return appendUvarint(dst, tag)
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func readProtoTag(data []byte) (field int, wireType int, consumed int, err error) {
	tag, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, 0, fmt.Errorf("bench proto: malformed tag")
	}
	return int(tag >> 3), int(tag & 0x7), n, nil
}

func consumeUvarint(data []byte) (uint64, int, error) {
	value, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, fmt.Errorf("bench proto: malformed varint")
	}
	return value, n, nil
}

func consumeBytes(data []byte) (string, int, error) {
	length, n, err := consumeUvarint(data)
	if err != nil {
		return "", 0, err
	}
	data = data[n:]
	if int(length) > len(data) {
		return "", 0, fmt.Errorf("bench proto: truncated bytes field")
	}
	return string(data[:length]), n + int(length), nil
}

func skipProtoField(wireType int, data []byte) (int, error) {
	switch wireType {
	case protoWireVarint:
		_, n, err := consumeUvarint(data)
		return n, err
	case protoWireBytes:
		length, n, err := consumeUvarint(data)
		if err != nil {
			return 0, err
		}
		if int(length) > len(data)-n {
			return 0, fmt.Errorf("bench proto: truncated skip payload")
		}
		return n + int(length), nil
	default:
		return 0, fmt.Errorf("bench proto: unsupported wire type %d", wireType)
	}
}
