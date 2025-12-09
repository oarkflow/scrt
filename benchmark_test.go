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

func BenchmarkCSV_Marshal_100(b *testing.B) {
	messages := generateMessages(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generateCSV(messages)
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

func BenchmarkCSV_Marshal_10000(b *testing.B) {
	messages := generateMessages(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = generateCSV(messages)
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
