package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"
)

// HTTPService exposes schema documents and metadata over HTTP so non-Go
// clients (TypeScript, etc.) can reuse the same compiled cache.
type HTTPService struct {
	Cache          *Cache
	SchemaPath     string
	BasePath       string
	BundleProvider BundleProvider
}

const (
	bundleMagic   = "SCB1"
	bundleVersion = 1
)

// BundleProvider returns SCRT payloads keyed by schema name for inclusion in the bundle response.
type BundleProvider func(doc *Document, schemaName string) (map[string][]byte, error)

// NewHTTPService wires an existing cache + schema file to HTTP routes.
func NewHTTPService(cache *Cache, schemaPath string) *HTTPService {
	base := "/schemas"
	return &HTTPService{Cache: cache, SchemaPath: schemaPath, BasePath: base}
}

// RegisterRoutes mounts /schemas/doc and /schemas/index onto mux.
func (svc *HTTPService) RegisterRoutes(mux *http.ServeMux) {
	base := strings.TrimSuffix(svc.BasePath, "/")
	mux.Handle(path.Join(base, "doc"), http.HandlerFunc(svc.serveDocument))
	mux.Handle(path.Join(base, "index"), http.HandlerFunc(svc.serveIndex))
	mux.Handle(path.Join(base, "bundle"), http.HandlerFunc(svc.serveBundle))
}

func (svc *HTTPService) serveDocument(w http.ResponseWriter, r *http.Request) {
	if handleCORS(w, r) {
		return
	}
	doc, raw, modTime, err := svc.Cache.Snapshot(svc.SchemaPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("schema snapshot error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
	w.Header().Set("X-SCRT-Fingerprint", fmt.Sprintf("%016x", documentFingerprint(doc)))
	if _, err := w.Write(raw); err != nil {
		http.Error(w, fmt.Sprintf("write error: %v", err), http.StatusInternalServerError)
	}
}

func (svc *HTTPService) serveIndex(w http.ResponseWriter, r *http.Request) {
	if handleCORS(w, r) {
		return
	}
	doc, _, modTime, err := svc.Cache.Snapshot(svc.SchemaPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("schema snapshot error: %v", err), http.StatusInternalServerError)
		return
	}
	var resp struct {
		Source      string       `json:"source"`
		Fingerprint string       `json:"fingerprint"`
		UpdatedAt   time.Time    `json:"updatedAt"`
		Schemas     []indexEntry `json:"schemas"`
	}
	resp.Source = doc.Source
	resp.Fingerprint = fmt.Sprintf("%016x", documentFingerprint(doc))
	resp.UpdatedAt = modTime.UTC()
	resp.Schemas = svc.indexEntries(doc)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
	}
}

func (svc *HTTPService) serveBundle(w http.ResponseWriter, r *http.Request) {
	if handleCORS(w, r) {
		return
	}
	doc, raw, modTime, err := svc.Cache.Snapshot(svc.SchemaPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("schema snapshot error: %v", err), http.StatusInternalServerError)
		return
	}
	fingerprint := documentFingerprint(doc)
	entries := svc.indexEntries(doc)
	updatedAt := modTime.UTC()
	var payload *bundlePayload
	if svc.BundleProvider != nil {
		reqSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
		payloads, err := svc.BundleProvider(doc, reqSchema)
		if err != nil {
			http.Error(w, fmt.Sprintf("bundle payload error: %v", err), http.StatusInternalServerError)
			return
		}
		payload = svc.selectPayload(doc, payloads, reqSchema)
	}
	body, err := buildBinaryBundle(doc, raw, entries, payload, fingerprint, updatedAt)
	if err != nil {
		http.Error(w, fmt.Sprintf("bundle encode error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-scrt-bundle")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write(body); err != nil {
		http.Error(w, fmt.Sprintf("bundle write error: %v", err), http.StatusInternalServerError)
	}
}

func (svc *HTTPService) selectPayload(doc *Document, payloads map[string][]byte, requested string) *bundlePayload {
	if len(payloads) == 0 {
		return nil
	}
	if requested != "" {
		if data, ok := payloads[requested]; ok {
			return svc.encodePayload(doc, requested, data)
		}
	}
	names := make([]string, 0, len(payloads))
	for name := range payloads {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		payload := svc.encodePayload(doc, name, payloads[name])
		if payload != nil {
			return payload
		}
	}
	return nil
}

func (svc *HTTPService) encodePayload(doc *Document, schemaName string, data []byte) *bundlePayload {
	sch, ok := doc.Schema(schemaName)
	if !ok {
		return nil
	}
	return &bundlePayload{
		Schema:      schemaName,
		Fingerprint: sch.Fingerprint(),
		Data:        data,
	}
}

type bundlePayload struct {
	Schema      string
	Fingerprint uint64
	Data        []byte
}

type indexEntry struct {
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
	Fields      int    `json:"fields"`
}

func (svc *HTTPService) indexEntries(doc *Document) []indexEntry {
	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := make([]indexEntry, 0, len(names))
	for _, name := range names {
		sch := doc.Schemas[name]
		entries = append(entries, indexEntry{
			Name:        name,
			Fingerprint: fmt.Sprintf("%016x", sch.Fingerprint()),
			Fields:      len(sch.Fields),
		})
	}
	return entries
}

func handleCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func documentFingerprint(doc *Document) uint64 {
	if doc == nil {
		return 0
	}
	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	var hash uint64 = 1469598103934665603
	const prime uint64 = 1099511628211
	for _, name := range names {
		hash ^= doc.Schemas[name].Fingerprint()
		hash *= prime
	}
	return hash
}

func buildBinaryBundle(doc *Document, schemaData []byte, entries []indexEntry, payload *bundlePayload, fingerprint uint64, updatedAt time.Time) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(schemaData) + 256)
	buf.WriteString(bundleMagic)
	buf.WriteByte(bundleVersion)
	if err := binary.Write(&buf, binary.BigEndian, fingerprint); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, updatedAt.UnixNano()); err != nil {
		return nil, err
	}
	if err := writeBytesWithUint32(&buf, schemaData); err != nil {
		return nil, err
	}
	if len(entries) > math.MaxUint16 {
		return nil, fmt.Errorf("too many schemas in bundle: %d", len(entries))
	}
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(entries))); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := writeStringWithUint16(&buf, entry.Name); err != nil {
			return nil, err
		}
		sch := doc.Schemas[entry.Name]
		if sch == nil {
			return nil, fmt.Errorf("schema %s missing from document", entry.Name)
		}
		if err := binary.Write(&buf, binary.BigEndian, sch.Fingerprint()); err != nil {
			return nil, err
		}
		if entry.Fields > math.MaxUint16 {
			return nil, fmt.Errorf("schema %s has too many fields: %d", entry.Name, entry.Fields)
		}
		if err := binary.Write(&buf, binary.BigEndian, uint16(entry.Fields)); err != nil {
			return nil, err
		}
	}
	if payload == nil {
		buf.WriteByte(0)
		return buf.Bytes(), nil
	}
	buf.WriteByte(1)
	if err := writeStringWithUint16(&buf, payload.Schema); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, payload.Fingerprint); err != nil {
		return nil, err
	}
	if err := writeBytesWithUint32(&buf, payload.Data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeStringWithUint16(buf *bytes.Buffer, value string) error {
	if len(value) > math.MaxUint16 {
		return fmt.Errorf("value too large for uint16 length: %q", value)
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(value))); err != nil {
		return err
	}
	_, err := buf.WriteString(value)
	return err
}

func writeBytesWithUint32(buf *bytes.Buffer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("segment too large for uint32 length: %d bytes", len(data))
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := buf.Write(data)
	return err
}
