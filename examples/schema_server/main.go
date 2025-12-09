package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
)

func main() {
	var schemaPath string
	var addr string
	flag.StringVar(&schemaPath, "schema", "data.scrt", "path to SCRT schema file")
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.Parse()

	cache := schema.NewCache()
	doc, err := cache.LoadFile(schemaPath)
	if err != nil {
		log.Fatalf("load schema: %v", err)
	}
	messageSchema, ok := doc.Schema("Message")
	if !ok {
		log.Fatalf("schema Message not found in %s", schemaPath)
	}

	svc := schema.NewHTTPService(cache, schemaPath)
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)
	mux.HandleFunc("/api/messages", func(w http.ResponseWriter, r *http.Request) {
		if allowCORS(w, r) {
			return
		}
		if r.URL.Query().Get("format") == "text" {
			text, err := encodeMessagesAsSCRT(messageSchema, demoMessages)
			if err != nil {
				http.Error(w, fmt.Sprintf("encode text: %v", err), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(text)); err != nil {
				log.Printf("write messages text: %v", err)
			}
			return
		}
		payload, err := scrt.Marshal(messageSchema, demoMessages)
		if err != nil {
			http.Error(w, fmt.Sprintf("encode messages: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-scrt")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-SCRT-Schema", messageSchema.Name)
		w.Header().Set("X-SCRT-Schema-Fingerprint", fmt.Sprintf("%016x", messageSchema.Fingerprint()))
		if _, err := w.Write(payload); err != nil {
			log.Printf("write messages: %v", err)
		}
	})

	log.Printf("serving SCRT schema from %s at %s", schemaPath, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

type Message struct {
	MsgID uint64
	User  uint64
	Text  string
	Lang  string
}

var demoMessages = []Message{
	{MsgID: 1, User: 1001, Text: "Hello World!", Lang: "en"},
	{MsgID: 2, User: 1002, Text: "Hola Mundo!", Lang: "es"},
	{MsgID: 3, User: 1003, Text: "Bonjour Monde!", Lang: "fr"},
}

func allowCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func encodeMessagesAsSCRT(sch *schema.Schema, rows []Message) (string, error) {
	var b strings.Builder
	writeSchemaDefinition(&b, sch)
	b.WriteString("\n@")
	b.WriteString(sch.Name)
	b.WriteString("\n")
	for _, row := range rows {
		line, err := formatRow(sch, map[string]any{
			"MsgID": row.MsgID,
			"User":  row.User,
			"Text":  row.Text,
			"Lang":  row.Lang,
		})
		if err != nil {
			return "", err
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String(), nil
}

func writeSchemaDefinition(b *strings.Builder, sch *schema.Schema) {
	b.WriteString("@schema\n")
	b.WriteString(sch.Name)
	b.WriteString("\n")
	for _, field := range sch.Fields {
		b.WriteString("@field ")
		b.WriteString(field.Name)
		b.WriteString(" ")
		b.WriteString(field.RawType)
		if attrs := fieldAttributes(field); attrs != "" {
			b.WriteString(" ")
			b.WriteString(attrs)
		}
		b.WriteString("\n")
	}
}

func fieldAttributes(field schema.Field) string {
	var attrs []string
	if field.AutoIncrement {
		attrs = append(attrs, "auto_increment")
	}
	for _, attr := range field.Attributes {
		lower := strings.ToLower(attr)
		if lower == "auto_increment" || lower == "autoincrement" || lower == "serial" {
			continue
		}
		if strings.HasPrefix(lower, "default=") || strings.HasPrefix(lower, "default:") {
			continue
		}
		attrs = append(attrs, attr)
	}
	if field.Default != nil {
		attrs = append(attrs, "default="+defaultLiteral(field.Default))
	}
	return strings.Join(attrs, "|")
}

func defaultLiteral(def *schema.DefaultValue) string {
	switch def.Kind {
	case schema.KindBool:
		if def.Bool {
			return "true"
		}
		return "false"
	case schema.KindInt64:
		return strconv.FormatInt(def.Int, 10)
	case schema.KindUint64, schema.KindRef:
		return strconv.FormatUint(def.Uint, 10)
	case schema.KindFloat64:
		return strconv.FormatFloat(def.Float, 'f', -1, 64)
	case schema.KindString:
		return strconv.Quote(def.String)
	case schema.KindBytes:
		return "0x" + hex.EncodeToString(def.Bytes)
	default:
		return ""
	}
}

func formatRow(sch *schema.Schema, row map[string]any) (string, error) {
	cells := make([]string, len(sch.Fields))
	for idx, field := range sch.Fields {
		val, ok := row[field.Name]
		if !ok || val == nil {
			cells[idx] = ""
			continue
		}
		formatted, err := formatValue(field, val)
		if err != nil {
			return "", err
		}
		cells[idx] = formatted
	}
	return strings.Join(cells, ", "), nil
}

func formatValue(field schema.Field, value any) (string, error) {
	switch field.Kind {
	case schema.KindUint64, schema.KindRef:
		switch v := value.(type) {
		case uint64:
			return strconv.FormatUint(v, 10), nil
		case uint:
			return strconv.FormatUint(uint64(v), 10), nil
		case int:
			if v < 0 {
				return "", fmt.Errorf("field %s expects unsigned value", field.Name)
			}
			return strconv.FormatUint(uint64(v), 10), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case schema.KindInt64:
		switch v := value.(type) {
		case int64:
			return strconv.FormatInt(v, 10), nil
		case int:
			return strconv.Itoa(v), nil
		case uint64:
			return strconv.FormatInt(int64(v), 10), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case schema.KindFloat64:
		return strconv.FormatFloat(value.(float64), 'f', -1, 64), nil
	case schema.KindBool:
		if value.(bool) {
			return "true", nil
		}
		return "false", nil
	case schema.KindString:
		return strconv.Quote(fmt.Sprint(value)), nil
	case schema.KindBytes:
		if b, ok := value.([]byte); ok {
			return "0x" + hex.EncodeToString(b), nil
		}
		return "", fmt.Errorf("field %s expects []byte", field.Name)
	default:
		return fmt.Sprintf("%v", value), nil
	}
}
