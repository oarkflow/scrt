package scrt_test

import (
	"bytes"
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
