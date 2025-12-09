package schema

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"
)

// HTTPService exposes schema documents and metadata over HTTP so non-Go
// clients (TypeScript, etc.) can reuse the same compiled cache.
type HTTPService struct {
	Cache      *Cache
	SchemaPath string
	BasePath   string
}

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
	type schemaEntry struct {
		Name        string `json:"name"`
		Fingerprint string `json:"fingerprint"`
		Fields      int    `json:"fields"`
	}
	var resp struct {
		Source      string        `json:"source"`
		Fingerprint string        `json:"fingerprint"`
		UpdatedAt   time.Time     `json:"updatedAt"`
		Schemas     []schemaEntry `json:"schemas"`
	}
	resp.Source = doc.Source
	resp.Fingerprint = fmt.Sprintf("%016x", documentFingerprint(doc))
	resp.UpdatedAt = modTime.UTC()

	names := make([]string, 0, len(doc.Schemas))
	for name := range doc.Schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	resp.Schemas = make([]schemaEntry, 0, len(names))
	for _, name := range names {
		sch := doc.Schemas[name]
		resp.Schemas = append(resp.Schemas, schemaEntry{
			Name:        name,
			Fingerprint: fmt.Sprintf("%016x", sch.Fingerprint()),
			Fields:      len(sch.Fields),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
	}
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
