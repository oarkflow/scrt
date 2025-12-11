package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	scrt "github.com/oarkflow/scrt"
	"github.com/oarkflow/scrt/codec"
	"github.com/oarkflow/scrt/schema"
	"github.com/oarkflow/scrt/storage"
	"github.com/oarkflow/scrt/temporal"
)

const (
	bundleMagic   = "SCB1"
	bundleVersion = 1
)

type server struct {
	registry  *schema.DocumentRegistry
	store     storage.Backend
	schemaDir string
}

func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
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
	storageDir := flag.String("storage", "./data", "directory for SCRT snapshots")
	schemaDir := flag.String("schemas", "./schemas", "directory for SCRT schema DSL files")
	flag.Parse()

	if err := os.MkdirAll(*schemaDir, 0o755); err != nil {
		log.Fatalf("schema dir: %v", err)
	}
	registry := schema.NewDocumentRegistry()
	backend, err := storage.NewSnapshotBackend(*storageDir)
	if err != nil {
		log.Fatalf("storage backend: %v", err)
	}

	srv := &server{registry: registry, store: backend, schemaDir: *schemaDir}
	if err := srv.bootstrapSchemas(); err != nil {
		log.Fatalf("bootstrap schemas: %v", err)
	}
	mux := http.NewServeMux()

	mux.HandleFunc("/schemas", srv.handleSchemas)
	mux.HandleFunc("/schemas/", srv.handleSchema)
	mux.HandleFunc("/records/", srv.handleRecords)
	mux.HandleFunc("/snapshots", srv.handleSnapshots)
	mux.HandleFunc("/ids/", srv.handleIDs)
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
	switch r.Method {
	case http.MethodGet:
		summaries := s.registry.List()
		sort.Slice(summaries, func(i, j int) bool {
			return summaries[i].Name < summaries[j].Name
		})
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		for _, summary := range summaries {
			fmt.Fprintln(w, summary.Name)
		}
	case http.MethodPost:
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		schemaName, err := s.upsertSchemaBody("", raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", fmt.Sprintf("/schemas/%s", url.PathEscape(schemaName)))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schemaName})
	default:
		methodNotAllowed(w)
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
		schemaName, err := s.upsertSchemaBody(name, raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Location", fmt.Sprintf("/schemas/%s", url.PathEscape(schemaName)))
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		s.registry.DeleteSchema(name)
		if err := s.removeSchemaFile(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("remove schema file %s: %v", name, err)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w)
	}
}

func (s *server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	metas, err := s.store.ListMeta()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, metas)
}

func (s *server) handleIDs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/ids/")
	if path == "" {
		http.Error(w, "ids path must be /ids/{schema}/{field} or /ids/uuid", http.StatusBadRequest)
		return
	}
	if strings.EqualFold(path, "uuid") || strings.EqualFold(path, "uuidv7") {
		id, err := storage.GenerateUUIDv7()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"uuid": id})
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "ids path must be /ids/{schema}/{field}", http.StatusBadRequest)
		return
	}
	schemaName, fieldName := parts[0], parts[1]
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
	next, err := s.store.NextAutoValue(schemaName, sch, fieldName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"schema": schemaName,
		"field":  fieldName,
		"next":   next,
	})
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
	if len(parts) >= 3 && strings.EqualFold(parts[1], "row") {
		fieldName := parts[2]
		var key string
		if len(parts) > 3 {
			raw := strings.Join(parts[3:], "/")
			decoded, err := url.PathUnescape(raw)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid record key: %v", err), http.StatusBadRequest)
				return
			}
			key = decoded
		} else {
			key = r.URL.Query().Get("key")
		}
		if key == "" {
			http.Error(w, "record key required via path segment or ?key=", http.StatusBadRequest)
			return
		}
		s.handleRecordRow(w, r, schemaName, fieldName, key)
		return
	}
	switch r.Method {
	case http.MethodGet:
		payload, err := s.store.LoadPayload(schemaName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		payloadWithIDs, err := s.populateAutoValues(schemaName, sch, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("auto-populate failed: %v", err), http.StatusInternalServerError)
			return
		}
		payload := append([]byte(nil), payloadWithIDs...)
		if !replace {
			existing, err := s.store.LoadPayload(schemaName)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			merged, mergeErr := appendPayload(existing, body, sch)
			if mergeErr != nil {
				http.Error(w, fmt.Sprintf("append failed: %v", mergeErr), http.StatusBadRequest)
				return
			}
			payload = merged
		}
		if _, err := s.store.Persist(schemaName, sch, payload, storage.PersistOptions{Indexes: storage.AutoIndexSpecs(sch)}); err != nil {
			http.Error(w, fmt.Sprintf("persist failed: %v", err), http.StatusInternalServerError)
			return
		}
		if err := s.registry.SetPayload(schemaName, payload); err != nil {
			statusFromError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		s.registry.ClearPayload(schemaName)
		if err := s.store.Delete(schemaName); err != nil && !errors.Is(err, os.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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
	payload, err := s.store.LoadPayload(schemaName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := writeBundle(w, doc, sch, raw, payload, schemaName, schemaName, updated); err != nil {
		log.Printf("write bundle: %v", err)
	}
}

func (s *server) handleRecordRow(w http.ResponseWriter, r *http.Request, schemaName, fieldName, rawKey string) {
	if fieldName == "" {
		http.Error(w, "field name required", http.StatusBadRequest)
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
	fieldIdx, ok := sch.FieldIndex(fieldName)
	if !ok {
		http.Error(w, fmt.Sprintf("schema %s lacks field %s", schemaName, fieldName), http.StatusBadRequest)
		return
	}
	key, err := parseRecordKey(&sch.Fields[fieldIdx], rawKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	payload, err := s.store.LoadPayload(schemaName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(payload) == 0 {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		record, found, err := findRecordRow(payload, sch, fieldIdx, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{
			"schema": schemaName,
			"field":  fieldName,
			"key":    rawKey,
			"row":    record,
		})
	case http.MethodDelete:
		updated, found, err := rewriteRecord(payload, sch, fieldIdx, key, nil, rowEditDelete)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		if _, err := s.store.Persist(schemaName, sch, updated, storage.PersistOptions{Indexes: storage.AutoIndexSpecs(sch)}); err != nil {
			http.Error(w, fmt.Sprintf("persist failed: %v", err), http.StatusInternalServerError)
			return
		}
		if err := s.registry.SetPayload(schemaName, updated); err != nil {
			statusFromError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch, http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("read row payload: %v", err), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			http.Error(w, "row payload required", http.StatusBadRequest)
			return
		}
		rowMap, err := parseSingleRowPayload(body, sch)
		if err != nil {
			http.Error(w, fmt.Sprintf("decode row: %v", err), http.StatusBadRequest)
			return
		}
		enforceKeyValue(rowMap, sch.Fields[fieldIdx], key)
		replacement, err := scrt.Marshal(sch, []map[string]any{rowMap})
		if err != nil {
			http.Error(w, fmt.Sprintf("marshal row failed: %v", err), http.StatusBadRequest)
			return
		}
		updated, found, err := rewriteRecord(payload, sch, fieldIdx, key, replacement, rowEditReplace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		if _, err := s.store.Persist(schemaName, sch, updated, storage.PersistOptions{Indexes: storage.AutoIndexSpecs(sch)}); err != nil {
			http.Error(w, fmt.Sprintf("persist failed: %v", err), http.StatusInternalServerError)
			return
		}
		if err := s.registry.SetPayload(schemaName, updated); err != nil {
			statusFromError(w, err)
			return
		}
		writeJSON(w, map[string]any{
			"schema": schemaName,
			"field":  fieldName,
			"key":    rawKey,
			"row":    rowMap,
		})
	default:
		methodNotAllowed(w)
	}
}

type rowEditOp int

const (
	rowEditReplace rowEditOp = iota
	rowEditDelete
)

type recordKey struct {
	kind      schema.FieldKind
	fieldName string
	raw       string
	uintVal   uint64
	intVal    int64
	strVal    string
	boolVal   bool
	floatVal  float64
}

func parseRecordKey(field *schema.Field, raw string) (recordKey, error) {
	key := recordKey{kind: field.ValueKind(), fieldName: field.Name, raw: raw}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return key, fmt.Errorf("record key for %s cannot be empty", field.Name)
	}
	switch key.kind {
	case schema.KindUint64, schema.KindRef:
		val, err := strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return key, fmt.Errorf("invalid numeric key for %s: %w", field.Name, err)
		}
		key.uintVal = val
	case schema.KindInt64:
		val, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return key, fmt.Errorf("invalid integer key for %s: %w", field.Name, err)
		}
		key.intVal = val
	case schema.KindFloat64:
		val, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return key, fmt.Errorf("invalid float key for %s: %w", field.Name, err)
		}
		key.floatVal = val
	case schema.KindBool:
		val, err := strconv.ParseBool(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid bool key for %s: %w", field.Name, err)
		}
		key.boolVal = val
	case schema.KindString:
		key.strVal = trimmed
	case schema.KindTimestampTZ:
		canonical, err := temporal.CanonicalTimestampTZ(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid timestamptz key for %s: %w", field.Name, err)
		}
		key.strVal = canonical
	case schema.KindDate:
		t, err := temporal.ParseDate(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid date key for %s: %w", field.Name, err)
		}
		key.intVal = temporal.EncodeDate(t)
	case schema.KindDateTime:
		t, err := temporal.ParseDateTime(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid datetime key for %s: %w", field.Name, err)
		}
		key.intVal = temporal.EncodeInstant(t)
	case schema.KindTimestamp:
		t, err := temporal.ParseTimestamp(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid timestamp key for %s: %w", field.Name, err)
		}
		key.intVal = temporal.EncodeInstant(t)
	case schema.KindDuration:
		dur, err := temporal.ParseDuration(trimmed)
		if err != nil {
			return key, fmt.Errorf("invalid duration key for %s: %w", field.Name, err)
		}
		key.intVal = int64(dur)
	default:
		return key, fmt.Errorf("field %s (kind %d) is not supported for record lookups", field.Name, field.ValueKind())
	}
	return key, nil
}

func (k recordKey) matches(val codec.Value) bool {
	if !val.Set {
		return false
	}
	switch k.kind {
	case schema.KindUint64, schema.KindRef:
		return val.Uint == k.uintVal
	case schema.KindInt64, schema.KindDate, schema.KindDateTime, schema.KindTimestamp, schema.KindDuration:
		return val.Int == k.intVal
	case schema.KindFloat64:
		return val.Float == k.floatVal
	case schema.KindBool:
		return val.Bool == k.boolVal
	case schema.KindString, schema.KindTimestampTZ:
		return val.Str == k.strVal
	default:
		return false
	}
}

func parseSingleRowPayload(data []byte, sch *schema.Schema) (map[string]any, error) {
	reader := codec.NewReader(bytes.NewReader(data), sch)
	row := codec.NewRow(sch)
	ok, err := reader.ReadRow(row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("row payload contained no data")
	}
	result := rowToMap(row, sch)
	second, err := reader.ReadRow(row)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if second {
		return nil, fmt.Errorf("only single-row payloads are supported")
	}
	return result, nil
}

func enforceKeyValue(row map[string]any, field schema.Field, key recordKey) {
	if row == nil {
		return
	}
	switch field.ValueKind() {
	case schema.KindUint64, schema.KindRef:
		row[field.Name] = key.uintVal
	case schema.KindInt64:
		row[field.Name] = key.intVal
	case schema.KindFloat64:
		row[field.Name] = key.floatVal
	case schema.KindBool:
		row[field.Name] = key.boolVal
	case schema.KindString, schema.KindTimestampTZ:
		row[field.Name] = key.strVal
	case schema.KindDate:
		row[field.Name] = temporal.FormatDate(temporal.DecodeDate(key.intVal))
	case schema.KindDateTime, schema.KindTimestamp:
		row[field.Name] = temporal.FormatInstant(temporal.DecodeInstant(key.intVal))
	case schema.KindDuration:
		row[field.Name] = time.Duration(key.intVal).String()
	}
}

func findRecordRow(payload []byte, sch *schema.Schema, fieldIdx int, key recordKey) (map[string]any, bool, error) {
	reader := codec.NewReader(bytes.NewReader(payload), sch)
	row := codec.NewRow(sch)
	matchCount := 0
	for {
		ok, err := reader.ReadRow(row)
		if errors.Is(err, io.EOF) || !ok {
			break
		}
		if err != nil {
			return nil, false, err
		}
		values := row.Values()
		if key.matches(values[fieldIdx]) {
			matchCount++
			if matchCount > 1 {
				return nil, false, fmt.Errorf("multiple rows match %s=%q", key.fieldName, key.raw)
			}
			return rowToMap(row, sch), true, nil
		}
	}
	return nil, false, nil
}

func rewriteRecord(payload []byte, sch *schema.Schema, fieldIdx int, key recordKey, replacement []byte, op rowEditOp) ([]byte, bool, error) {
	if op == rowEditReplace && len(replacement) == 0 {
		return nil, false, fmt.Errorf("replacement payload required for record update")
	}
	reader := codec.NewReader(bytes.NewReader(payload), sch)
	buf := &bytes.Buffer{}
	writer := codec.NewWriter(buf, sch, 1024)
	row := codec.NewRow(sch)
	matchCount := 0
	for {
		ok, err := reader.ReadRow(row)
		if errors.Is(err, io.EOF) || !ok {
			break
		}
		if err != nil {
			return nil, false, err
		}
		values := row.Values()
		if key.matches(values[fieldIdx]) {
			matchCount++
			if matchCount > 1 {
				return nil, false, fmt.Errorf("multiple rows match %s=%q", key.fieldName, key.raw)
			}
			if op == rowEditDelete {
				continue
			}
			if err := copyPayload(writer, row, replacement); err != nil {
				return nil, false, err
			}
			continue
		}
		if err := writer.WriteRow(row); err != nil {
			return nil, false, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, false, err
	}
	if matchCount == 0 {
		return nil, false, nil
	}
	return buf.Bytes(), true, nil
}

func rowToMap(row codec.Row, sch *schema.Schema) map[string]any {
	values := row.Values()
	out := make(map[string]any)
	for idx, field := range sch.Fields {
		val := values[idx]
		if !val.Set {
			continue
		}
		switch field.ValueKind() {
		case schema.KindUint64, schema.KindRef:
			out[field.Name] = val.Uint
		case schema.KindInt64:
			out[field.Name] = val.Int
		case schema.KindFloat64:
			out[field.Name] = val.Float
		case schema.KindBool:
			out[field.Name] = val.Bool
		case schema.KindString, schema.KindTimestampTZ:
			out[field.Name] = val.Str
		case schema.KindBytes:
			buf := append([]byte(nil), val.Bytes...)
			out[field.Name] = buf
		case schema.KindDate:
			out[field.Name] = temporal.FormatDate(temporal.DecodeDate(val.Int))
		case schema.KindDateTime, schema.KindTimestamp:
			out[field.Name] = temporal.FormatInstant(temporal.DecodeInstant(val.Int))
		case schema.KindDuration:
			out[field.Name] = time.Duration(val.Int).String()
		default:
			out[field.Name] = val.Str
		}
	}
	return out
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

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func canonicalSchemaName(doc *schema.Document) string {
	if doc == nil {
		return ""
	}
	for name := range doc.Schemas {
		return name
	}
	return ""
}

func (s *server) upsertSchemaBody(name string, raw []byte) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", fmt.Errorf("empty schema body")
	}
	if s.registry == nil {
		return "", fmt.Errorf("schema registry unavailable")
	}
	doc, err := s.registry.Upsert(name, raw, "api", time.Now().UTC())
	if err != nil {
		return "", err
	}
	schemaName := canonicalSchemaName(doc)
	if schemaName == "" {
		schemaName = name
	}
	if err := s.saveSchemaFile(schemaName, raw); err != nil {
		return "", fmt.Errorf("persist schema: %w", err)
	}
	return schemaName, nil
}

func (s *server) saveSchemaFile(name string, raw []byte) error {
	if s.schemaDir == "" {
		return nil
	}
	if err := os.MkdirAll(s.schemaDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.schemaDir, fmt.Sprintf("%s.scrt", name))
	return os.WriteFile(path, raw, 0o644)
}

func (s *server) removeSchemaFile(name string) error {
	if s.schemaDir == "" {
		return nil
	}
	path := filepath.Join(s.schemaDir, fmt.Sprintf("%s.scrt", name))
	return os.Remove(path)
}

func (s *server) bootstrapSchemas() error {
	if s == nil || s.registry == nil {
		return fmt.Errorf("schema registry unavailable")
	}
	if s.schemaDir == "" {
		return nil
	}
	entries, err := os.ReadDir(s.schemaDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), ".scrt") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		path := filepath.Join(s.schemaDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("schema bootstrap: read %s: %v", path, err)
			continue
		}
		info, err := entry.Info()
		if err != nil {
			log.Printf("schema bootstrap: stat %s: %v", path, err)
			continue
		}
		if _, err := s.registry.Upsert(name, data, path, info.ModTime()); err != nil {
			log.Printf("schema bootstrap: load %s: %v", path, err)
			continue
		}
		log.Printf("Loaded schema %s from %s", name, path)
	}
	return nil
}

func (s *server) populateAutoValues(schemaName string, sch *schema.Schema, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}
	autoFields := make([]int, 0)
	uuidFields := make([]int, 0)
	for idx, field := range sch.Fields {
		if field.AutoIncrement {
			autoFields = append(autoFields, idx)
		}
		if requiresUUID(field) {
			uuidFields = append(uuidFields, idx)
		}
	}
	if len(autoFields) == 0 && len(uuidFields) == 0 {
		return payload, nil
	}
	reader := codec.NewReader(bytes.NewReader(payload), sch)
	var buf bytes.Buffer
	writer := codec.NewWriter(&buf, sch, 1024)
	row := codec.NewRow(sch)
	for {
		ok, err := reader.ReadRow(row)
		if errors.Is(err, io.EOF) || !ok {
			break
		}
		if err != nil {
			return nil, err
		}
		values := row.Values()
		for _, idx := range autoFields {
			current := values[idx]
			if current.Set && current.Uint != 0 {
				continue
			}
			next, err := s.store.NextAutoValue(schemaName, sch, sch.Fields[idx].Name)
			if err != nil {
				return nil, err
			}
			current.Uint = next
			current.Set = true
			current.Str = ""
			row.SetByIndex(idx, current)
		}
		for _, idx := range uuidFields {
			current := values[idx]
			if current.Set && current.Str != "" {
				continue
			}
			uuid, err := storage.GenerateUUIDv7()
			if err != nil {
				return nil, err
			}
			current.Str = uuid
			current.Set = true
			row.SetByIndex(idx, current)
		}
		if err := writer.WriteRow(row); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func requiresUUID(field schema.Field) bool {
	if field.Kind != schema.KindString {
		return false
	}
	if field.HasAttribute("uuid") || field.HasAttribute("uuidv7") || field.HasAttribute("unique") {
		return true
	}
	return false
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
