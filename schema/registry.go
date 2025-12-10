package schema

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// DocumentRegistry keeps multiple SCRT documents in memory along with their raw DSL and payloads.
type DocumentRegistry struct {
	mu   sync.RWMutex
	docs map[string]*registryDocument
}

type registryDocument struct {
	name    string
	doc     *Document
	raw     []byte
	source  string
	updated time.Time
	payload []byte
}

// DocumentSummary describes a stored SCRT document.
type DocumentSummary struct {
	Name        string    `json:"name"`
	Fingerprint string    `json:"fingerprint"`
	SchemaCount int       `json:"schemaCount"`
	UpdatedAt   time.Time `json:"updatedAt"`
	Source      string    `json:"source"`
}

// NewDocumentRegistry creates an empty registry.
func NewDocumentRegistry() *DocumentRegistry {
	return &DocumentRegistry{docs: make(map[string]*registryDocument)}
}

// LoadFile reads a .scrt file from disk and stores it under the provided name.
func (r *DocumentRegistry) LoadFile(name, path string) (*Document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if name == "" {
		base := filepath.Base(path)
		name = strings.TrimSuffix(base, filepath.Ext(base))
	}
	return r.Upsert(name, data, path, info.ModTime())
}

// Upsert parses raw SCRT DSL bytes and stores/overwrites the document.
func (r *DocumentRegistry) Upsert(name string, raw []byte, source string, updatedAt time.Time) (*Document, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("schema body cannot be empty")
	}
	doc, err := Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	schemaName, err := extractSingleSchemaName(doc)
	if err != nil {
		return nil, err
	}
	if name != "" && !strings.EqualFold(name, schemaName) {
		return nil, fmt.Errorf("schema name mismatch: DSL defines %q but request targeted %q", schemaName, name)
	}
	if name == "" {
		name = schemaName
	}
	// Re-map document schemas so only the canonical name exists in the map.
	normalized := ensureSingleEntryDocument(doc, schemaName)
	normalized.Source = source
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	entry := &registryDocument{
		name:    schemaName,
		doc:     normalized,
		raw:     append([]byte(nil), raw...),
		source:  source,
		updated: updatedAt,
	}
	r.mu.Lock()
	r.docs[schemaName] = entry
	r.mu.Unlock()
	return normalized, nil
}

// Snapshot returns the parsed document, raw DSL, and timestamp for a document name.
func (r *DocumentRegistry) Snapshot(name string) (*Document, []byte, time.Time, error) {
	r.mu.RLock()
	entry, ok := r.docs[name]
	r.mu.RUnlock()
	if !ok {
		return nil, nil, time.Time{}, os.ErrNotExist
	}
	rawCopy := append([]byte(nil), entry.raw...)
	return entry.doc, rawCopy, entry.updated, nil
}

// List returns all document summaries.
func (r *DocumentRegistry) List() []DocumentSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]DocumentSummary, 0, len(r.docs))
	for name, entry := range r.docs {
		out = append(out, DocumentSummary{
			Name:        name,
			Fingerprint: fmt.Sprintf("%016x", documentFingerprint(entry.doc)),
			SchemaCount: len(entry.doc.Schemas),
			UpdatedAt:   entry.updated,
			Source:      entry.source,
		})
	}
	return out
}

// Payload returns a copy of the stored payload bytes for a schema.
func (r *DocumentRegistry) Payload(schemaName string) ([]byte, bool) {
	r.mu.RLock()
	entry, ok := r.docs[schemaName]
	r.mu.RUnlock()
	if !ok || entry.payload == nil {
		return nil, false
	}
	return append([]byte(nil), entry.payload...), true
}

// SetPayload saves/overwrites the payload bytes for a schema.
func (r *DocumentRegistry) SetPayload(schemaName string, data []byte) error {
	if schemaName == "" {
		return fmt.Errorf("schema name cannot be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.docs[schemaName]
	if !ok {
		return os.ErrNotExist
	}
	entry.payload = append([]byte(nil), data...)
	entry.updated = time.Now().UTC()
	return nil
}

// ClearPayload removes any stored rows for a schema but leaves the schema itself intact.
func (r *DocumentRegistry) ClearPayload(schemaName string) {
	if schemaName == "" {
		return
	}
	r.mu.Lock()
	if entry, ok := r.docs[schemaName]; ok {
		entry.payload = nil
		entry.updated = time.Now().UTC()
	}
	r.mu.Unlock()
}

// DeleteSchema removes a schema from the registry.
func (r *DocumentRegistry) DeleteSchema(name string) {
	r.mu.Lock()
	delete(r.docs, name)
	r.mu.Unlock()
}

// HasSchema returns true if a named schema exists.
func (r *DocumentRegistry) HasSchema(name string) bool {
	r.mu.RLock()
	_, ok := r.docs[name]
	r.mu.RUnlock()
	return ok
}

// CopyDSL streams the stored DSL into w.
func (r *DocumentRegistry) CopyDSL(name string, w io.Writer) error {
	r.mu.RLock()
	entry, ok := r.docs[name]
	r.mu.RUnlock()
	if !ok {
		return os.ErrNotExist
	}
	_, err := w.Write(entry.raw)
	return err
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
	hasher := fnv.New64a()
	for _, name := range names {
		schema := doc.Schemas[name]
		if schema == nil {
			continue
		}
		_, _ = hasher.Write([]byte(name))
		_, _ = hasher.Write([]byte{':'})
		fp := schema.Fingerprint()
		buf := [8]byte{}
		for i := 0; i < 8; i++ {
			buf[i] = byte(fp >> (8 * i))
		}
		_, _ = hasher.Write(buf[:])
		_, _ = hasher.Write([]byte{'|'})
	}
	return hasher.Sum64()
}

func extractSingleSchemaName(doc *Document) (string, error) {
	if doc == nil || len(doc.Schemas) == 0 {
		return "", fmt.Errorf("schema documents must declare at least one @schema block")
	}
	if len(doc.Schemas) > 1 {
		return "", fmt.Errorf("schema documents must declare exactly one @schema block (found %d)", len(doc.Schemas))
	}
	for name := range doc.Schemas {
		return name, nil
	}
	return "", fmt.Errorf("schema name missing")
}

func ensureSingleEntryDocument(doc *Document, schemaName string) *Document {
	if doc == nil {
		return nil
	}
	schema := doc.Schemas[schemaName]
	data := make(map[string][]map[string]interface{})
	if rows, ok := doc.Data[schemaName]; ok {
		data[schemaName] = rows
	}
	return &Document{
		Schemas: map[string]*Schema{schemaName: schema},
		Data:    data,
		Source:  doc.Source,
	}
}
