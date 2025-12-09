package main

import (
	"flag"
	"log"
	"net/http"

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
	svc.BundleProvider = func(_ *schema.Document, requested string) (map[string][]byte, error) {
		if requested != "" && requested != "Message" {
			return nil, nil
		}
		payload, err := scrt.Marshal(messageSchema, demoMessages)
		if err != nil {
			return nil, err
		}
		return map[string][]byte{"Message": payload}, nil
	}
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)

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
