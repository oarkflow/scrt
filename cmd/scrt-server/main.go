package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
)

const (
	bundleMagic   = "SCB1"
	bundleVersion = 1
)

type server struct {
	registry *schema.DocumentRegistry
}

func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		// Allow common headers used by browsers and our client
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization")
		// Expose specific headers to client-side JS if needed
		w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	registry := schema.NewDocumentRegistry()

	srv := &server{registry: registry}
	mux := http.NewServeMux()

	mux.HandleFunc("/schemas", srv.handleSchemas)
	mux.HandleFunc("/schemas/", srv.handleSchema)
	mux.HandleFunc("/records/", srv.handleRecords)
	mux.HandleFunc("/bundle", srv.handleBundle)

	listener := allowCORS(noCache(mux))
	httpServer := &http.Server{
		Addr:    *addr,
		Handler: listener,
	}

	// Channel to listen for interrupt signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("SCRT server listening on %s (CORS enabled)", *addr)

	// Start server in a goroutine
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	<-stop

	log.Println("Shutting down server...")

	// Create context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	log.Println("Server stopped")
}

func (s *server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	summaries := s.registry.List()
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, summary := range summaries {
		fmt.Fprintln(w, summary.Name)
	}
}

func (s *server) handleSchema(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/schemas/")
	if name == "" {
		http.Error(w, "document name required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		buf := &bytes.Buffer{}
		if err := s.registry.CopyDSL(name, buf); err != nil {
			statusFromError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(buf.Bytes())
	case http.MethodPost:
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(bytes.TrimSpace(raw)) == 0 {
			http.Error(w, "empty schema body", http.StatusBadRequest)
			return
		}
		if _, err := s.registry.Upsert(name, raw, "api", time.Now().UTC()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		s.registry.DeleteSchema(name)
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w)
	}
}

func (s *server) handleRecords(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/records/")
	if path == "" {
		http.Error(w, "records path must be /records/{schema}", http.StatusBadRequest)
		return
	}
	parts := strings.Split(path, "/")
	schemaName := parts[0]
	if schemaName == "" {
		http.Error(w, "schema name required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		payload, ok := s.registry.Payload(schemaName)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-scrt")
		_, _ = w.Write(payload)
	case http.MethodPost, http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			http.Error(w, "empty payload", http.StatusBadRequest)
			return
		}
		doc, _, _, err := s.registry.Snapshot(schemaName)
		if err != nil {
			statusFromError(w, err)
			return
		}
		sch, ok := doc.Schema(schemaName)
		if !ok {
			http.Error(w, "unknown schema", http.StatusNotFound)
			return
		}
		if err := validatePayload(body, sch); err != nil {
			http.Error(w, fmt.Sprintf("invalid SCRT payload: %v", err), http.StatusBadRequest)
			return
		}
		replace := r.Method == http.MethodPut
		if mode := strings.ToLower(r.URL.Query().Get("mode")); mode == "replace" {
			replace = true
		} else if mode == "append" {
			replace = false
		}
		payload := append([]byte(nil), body...)
		if !replace {
			existing, _ := s.registry.Payload(schemaName)
			merged, mergeErr := appendPayload(existing, body, sch)
			if mergeErr != nil {
				http.Error(w, fmt.Sprintf("append failed: %v", mergeErr), http.StatusBadRequest)
				return
			}
			payload = merged
		}
		if err := s.registry.SetPayload(schemaName, payload); err != nil {
			statusFromError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		s.registry.ClearPayload(schemaName)
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w)
	}
}

func (s *server) handleBundle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	schemaName := r.URL.Query().Get("schema")
	if schemaName == "" {
		http.Error(w, "schema query param required", http.StatusBadRequest)
		return
	}
	doc, raw, updated, err := s.registry.Snapshot(schemaName)
	if err != nil {
		statusFromError(w, err)
		return
	}
	sch, ok := doc.Schema(schemaName)
	if !ok {
		http.Error(w, "unknown schema", http.StatusNotFound)
		return
	}
	payload, _ := s.registry.Payload(schemaName)
	if err := writeBundle(w, doc, sch, raw, payload, schemaName, schemaName, updated); err != nil {
		log.Printf("write bundle: %v", err)
	}
}

func validatePayload(data []byte, sch *schema.Schema) error {
	reader := codec.NewReader(bytes.NewReader(data), sch)
	row := codec.NewRow(sch)
	for {
		ok, err := reader.ReadRow(row)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if !ok {
			return nil
		}
	}
}

func appendPayload(existing, incoming []byte, sch *schema.Schema) ([]byte, error) {
	if len(existing) == 0 {
		return append([]byte(nil), incoming...), nil
	}
	buf := &bytes.Buffer{}
	writer := codec.NewWriter(buf, sch, 1024)
	row := codec.NewRow(sch)
	var firstErr error
	if err := copyPayload(writer, row, existing); err != nil {
		firstErr = err
	}
	if firstErr == nil {
		if err := copyPayload(writer, row, incoming); err != nil {
			firstErr = err
		}
	}
	closeErr := writer.Close()
	if firstErr == nil {
		firstErr = closeErr
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return buf.Bytes(), nil
}

func copyPayload(writer *codec.Writer, row codec.Row, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	reader := codec.NewReader(bytes.NewReader(payload), row.Schema())
	for {
		ok, err := reader.ReadRow(row)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if !ok {
			return nil
		}
		if err := writer.WriteRow(row); err != nil {
			return err
		}
	}
}

func writeBundle(w http.ResponseWriter, doc *schema.Document, sch *schema.Schema, raw []byte, payload []byte, docName, schemaName string, updated time.Time) error {
	if payload == nil {
		payload = make([]byte, 0)
	}
	w.Header().Set("Content-Type", "application/x-scrt-bundle")
	buf := &bytes.Buffer{}
	buf.WriteString(bundleMagic)
	buf.WriteByte(bundleVersion)
	if err := binary.Write(buf, binary.LittleEndian, fingerprintDocument(doc)); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, sch.Fingerprint()); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, updated.UTC().UnixNano()); err != nil {
		return err
	}
	if err := writeCompactString(buf, docName); err != nil {
		return err
	}
	if err := writeCompactString(buf, schemaName); err != nil {
		return err
	}
	if err := writeBlob(buf, raw); err != nil {
		return err
	}
	if err := writeBlob(buf, payload); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

func writeCompactString(w io.Writer, value string) error {
	if len(value) > math.MaxUint16 {
		return fmt.Errorf("value too large")
	}
	var prefix [2]byte
	binary.LittleEndian.PutUint16(prefix[:], uint16(len(value)))
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	if len(value) == 0 {
		return nil
	}
	_, err := io.WriteString(w, value)
	return err
}

func writeBlob(w io.Writer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("blob too large")
	}
	var prefix [4]byte
	binary.LittleEndian.PutUint32(prefix[:], uint32(len(data)))
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func fingerprintDocument(doc *schema.Document) uint64 {
	if doc == nil {
		return 0
	}
	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	var hash uint64
	const fnvOffset = 1469598103934665603
	const fnvPrime = 1099511628211
	hash = fnvOffset
	for _, name := range names {
		for i := 0; i < len(name); i++ {
			hash ^= uint64(name[i])
			hash *= fnvPrime
		}
		sch := doc.Schemas[name]
		if sch == nil {
			continue
		}
		hash ^= sch.Fingerprint()
		hash *= fnvPrime
	}
	return hash
}

func statusFromError(w http.ResponseWriter, err error) {
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func methodNotAllowed(w http.ResponseWriter) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
