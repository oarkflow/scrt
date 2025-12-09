package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/schema"
)

type schemaList []string

func (s *schemaList) String() string {
	return strings.Join(*s, ",")
}

func (s *schemaList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var specs schemaList
	var addr string
	var basePath string
	flag.Var(&specs, "schema", "schema specification in the form name=path or just /path/to/doc.scrt (repeatable)")
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.StringVar(&basePath, "base-path", "/schemas", "base HTTP path for schema APIs")
	flag.Parse()
	if len(specs) == 0 {
		_ = specs.Set("message=data.scrt")
	}
	registry := schema.NewDocumentRegistry()
	for _, spec := range specs {
		name, path := parseSchemaSpec(spec)
		doc, err := registry.LoadFile(name, path)
		if err != nil {
			log.Fatalf("load schema %s: %v", spec, err)
		}
		seeded := seedEmbeddedRows(registry, name, doc)
		if !seeded["Message"] {
			if sch, ok := doc.Schema("Message"); ok {
				payload, err := scrt.Marshal(sch, demoMessages)
				if err != nil {
					log.Fatalf("marshal demo messages: %v", err)
				}
				if err := registry.SetPayload(name, "Message", payload); err != nil {
					log.Fatalf("seed payload: %v", err)
				}
			}
		}
	}

	svc := schema.NewRegistryHTTPService(registry)
	svc.BasePath = basePath
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)

	log.Printf("serving %d SCRT documents at %s%s", len(specs), addr, basePath)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func parseSchemaSpec(input string) (string, string) {
	if strings.TrimSpace(input) == "" {
		log.Fatal("empty schema specification")
	}
	if strings.Contains(input, "=") {
		parts := strings.SplitN(input, "=", 2)
		name := strings.TrimSpace(parts[0])
		path := strings.TrimSpace(parts[1])
		if name == "" || path == "" {
			log.Fatalf("invalid schema spec %q", input)
		}
		return name, path
	}
	base := filepath.Base(input)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	if name == "" {
		name = fmt.Sprintf("doc-%d", time.Now().UnixNano())
	}
	return name, input
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

func seedEmbeddedRows(reg *schema.DocumentRegistry, docName string, doc *schema.Document) map[string]bool {
	seeded := make(map[string]bool)
	if doc == nil {
		return seeded
	}
	for schemaName, rows := range doc.Data {
		if len(rows) == 0 {
			continue
		}
		sch, ok := doc.Schema(schemaName)
		if !ok {
			continue
		}
		payload, err := scrt.Marshal(sch, rows)
		if err != nil {
			log.Printf("marshal embedded rows for %s/%s: %v", docName, schemaName, err)
			continue
		}
		if err := reg.SetPayload(docName, schemaName, payload); err != nil {
			log.Printf("store embedded rows for %s/%s: %v", docName, schemaName, err)
			continue
		}
		seeded[schemaName] = true
	}
	return seeded
}

// retain for compatibility if other handlers are added later
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
